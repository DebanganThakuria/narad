#!/usr/bin/env bash
#
# Runs a three-node Narad cluster under load, kills and partitions nodes
# underneath it, records every client operation with the interval it was
# in flight for, and checks the resulting history against a sequential
# specification of the delivery contract.
#
# The question it answers is not "did anything break", which the load
# driver already answers. It is "is every anomaly accounted for". Narad
# promises at-least-once, so a message that comes back after its ack is
# permitted, and the useful distinction is whether it came back while a
# broker was being killed or partitioned, or for no reason anybody has
# written down. The first is the contract. The second is a bug nobody has
# found yet, and this exits non-zero for it.
#
# Two faults are injected, alternating:
#
#   kill       the node is stopped and restarted. Its leases expire
#              afterwards, which is what makes messages come back.
#   partition  the node keeps serving clients but its peers can no
#              longer reach it on the cluster plane, so produces route
#              around it, Raft loses a member, and its partitions answer
#              from a node that cannot hear the rest of the cluster.
#              Needs iptables and passwordless sudo; skipped with a
#              warning when either is missing, which is the normal case
#              on a developer laptop.
#
# Usage: scripts/linearizability-nightly.sh [options]
#   --duration SECONDS      how long load runs          (default 180)
#   --drain SECONDS         how long consumers get after (default 150)
#   --rate N                produces per second          (default 300)
#   --topics N              topics                       (default 3)
#   --partitions N          partitions per topic         (default 6)
#   --visibility SECONDS    visibility timeout           (default 5)
#   --no-faults             run clean, for a baseline
#   --no-partition-faults   kills only
#   --strict                fail on any post-ack redelivery at all
#   --out DIR               where to leave history, faults and verdict
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
TMP_DIR="${NARAD_LINEARIZABILITY_DIR:-$(mktemp -d "${TMP_BASE}/narad-linearizability.XXXXXX")}"
BIN="$TMP_DIR/narad"
DRIVER_BIN="$TMP_DIR/driver"
CHECKER_BIN="$TMP_DIR/linearizability"
LOG_DIR="$TMP_DIR/logs"
PID_DIR="$TMP_DIR/pids"
GO_BIN="${GO:-go}"
DEFAULT_GO_CACHE="${TMP_BASE}/narad-go-cache"

# Deliberately not the ports local-cluster-chaos.sh uses, so a developer
# can run both at once without them fighting over a listener.
HTTP_PORTS=(18381 18382 18383)
CLUSTER_PORTS=(19381 19382 19383)
NODE_IDS=(narad-1 narad-2 narad-3)

DURATION_SECONDS=180
DRAIN_SECONDS=150
RATE=300
TOPICS=3
PARTITIONS=6
VISIBILITY_SECONDS=5
FAULTS_ENABLED=1
PARTITION_FAULTS=1
STRICT=0
OUT_DIR=""

CLUSTER_SECRET="${NARAD_CLUSTER_SECRET:-linearizability-cluster-secret}"
ADMIN_PASSWORD="${NARAD_ADMIN_PASSWORD:-linearizability-admin-password}"
SECURITY_ENABLED="${NARAD_SECURITY_ENABLED:-true}"
FAULT_PID=""

while [[ $# -gt 0 ]]; do
	case "$1" in
	--duration) DURATION_SECONDS="$2"; shift 2 ;;
	--drain) DRAIN_SECONDS="$2"; shift 2 ;;
	--rate) RATE="$2"; shift 2 ;;
	--topics) TOPICS="$2"; shift 2 ;;
	--partitions) PARTITIONS="$2"; shift 2 ;;
	--visibility) VISIBILITY_SECONDS="$2"; shift 2 ;;
	--no-faults) FAULTS_ENABLED=0; shift ;;
	--no-partition-faults) PARTITION_FAULTS=0; shift ;;
	--strict) STRICT=1; shift ;;
	--out) OUT_DIR="$2"; shift 2 ;;
	-h | --help) sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
	*) echo "unknown option: $1" >&2; exit 2 ;;
	esac
done

if [[ -z "$OUT_DIR" ]]; then
	OUT_DIR="$TMP_DIR/out"
fi
mkdir -p "$OUT_DIR" "$LOG_DIR" "$PID_DIR"
HISTORY="$OUT_DIR/history.jsonl"
FAULTS="$OUT_DIR/faults.jsonl"
VERDICT="$OUT_DIR/verdict.txt"
VERDICT_JSON="$OUT_DIR/verdict.json"
VERDICT_MD="$OUT_DIR/verdict.md"
: >"$FAULTS"

# ---- portable nanosecond clock ------------------------------------------
#
# The history's timestamps come from two processes: the Go driver and
# this script. They have to be on the same scale, and GNU date's %N is
# not available on macOS, where a developer reproducing a nightly failure
# will be running this.
NOW_NS_MODE="seconds"
detect_clock() {
	local probe
	probe="$(date +%s%N 2>/dev/null || true)"
	if [[ "$probe" =~ ^[0-9]+$ ]]; then
		NOW_NS_MODE="date"
	elif command -v python3 >/dev/null 2>&1; then
		NOW_NS_MODE="python"
	else
		echo "warning: no nanosecond clock; fault windows will be second-resolution" >&2
	fi
}

now_ns() {
	case "$NOW_NS_MODE" in
	date) date +%s%N ;;
	python) python3 -c 'import time; print(time.time_ns())' ;;
	*) echo "$(($(date +%s) * 1000000000))" ;;
	esac
}

record_fault() {
	local kind="$1" target="$2" start="$3" end="$4"
	printf '{"op":"fault","kind":"%s","target":"%s","call":%s,"ret":%s}\n' \
		"$kind" "$target" "$start" "$end" >>"$FAULTS"
}

# ---- cluster ------------------------------------------------------------

node_url() { echo "http://127.0.0.1:${HTTP_PORTS[$1]}"; }
nodes_csv() { echo "$(node_url 0),$(node_url 1),$(node_url 2)"; }
pid_file() { echo "$PID_DIR/${NODE_IDS[$1]}.pid"; }

wait_ready() {
	local i="$1" url
	url="$(node_url "$i")/readyz"
	for _ in {1..120}; do
		if curl -fsS "$url" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.25
	done
	echo "${NODE_IDS[$i]} did not become ready at $url" >&2
	return 1
}

wait_admin_auth() {
	local url
	url="$(node_url 0)/v1/users"
	for _ in {1..120}; do
		if curl -fsS -u "admin:${ADMIN_PASSWORD}" "$url" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.25
	done
	echo "root admin was not seeded / could not authenticate" >&2
	return 1
}

start_node() {
	local i="$1" node="${NODE_IDS[$1]}"
	NARAD_HTTP_ADDR="127.0.0.1:${HTTP_PORTS[$i]}" \
		NARAD_CLUSTER_ADDR="127.0.0.1:${CLUSTER_PORTS[$i]}" \
		NARAD_NODE_ID="$node" \
		NARAD_CLUSTER_PEERS="$PEERS" \
		NARAD_DATA_DIR="$TMP_DIR/$node" \
		NARAD_SECURITY_ENABLED="$SECURITY_ENABLED" \
		NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT=true \
		NARAD_SECURITY_ALLOW_INSECURE_CLUSTER=true \
		NARAD_CLUSTER_SECRET="$CLUSTER_SECRET" \
		NARAD_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
		NARAD_LOG_FORMAT="${NARAD_LOG_FORMAT:-text}" \
		NARAD_LOG_LEVEL="${NARAD_LOG_LEVEL:-info}" \
		"$BIN" serve >>"$LOG_DIR/$node.log" 2>&1 &
	echo "$!" >"$(pid_file "$i")"
}

stop_node() {
	local i="$1" file pid
	file="$(pid_file "$i")"
	[[ -f "$file" ]] || return 0
	pid="$(cat "$file")"
	if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
		kill "$pid" 2>/dev/null || true
		for _ in {1..40}; do
			kill -0 "$pid" 2>/dev/null || break
			sleep 0.25
		done
		kill -9 "$pid" 2>/dev/null || true
	fi
	rm -f "$file"
}

# ---- partition faults ---------------------------------------------------
#
# The rules drop traffic INBOUND to the node's cluster plane only: Raft on
# its TCP cluster port, and the QUIC plane, which rides the API port
# number over UDP. The API's TCP port is untouched, so clients keep
# talking to the node throughout. That is the interesting shape: a broker
# that is up, serving, and cannot hear its peers.
#
# It is also asymmetric. The node's own outbound connections use ephemeral
# source ports and are unaffected, so it can still reach peers while they
# cannot reach it. Asymmetric partitions are both realistic and nastier
# than a clean cut, which is the point of injecting them.
partitions_available() {
	[[ "$PARTITION_FAULTS" -eq 1 ]] || return 1
	command -v iptables >/dev/null 2>&1 || return 1
	sudo -n iptables -L INPUT -n >/dev/null 2>&1 || return 1
	return 0
}

partition_start() {
	local i="$1"
	sudo -n iptables -I INPUT -p tcp --dport "${CLUSTER_PORTS[$i]}" -j DROP
	sudo -n iptables -I INPUT -p udp --dport "${HTTP_PORTS[$i]}" -j DROP
}

# partition_stop removes the rules for one node, and shouts if it cannot.
#
# "iptables says there is no such rule" and "sudo would not let me ask"
# look the same from the exit status, and treating the second as the
# first is how a node stays cut off for the rest of a run while the
# script believes it healed it. A sudo timestamp expiring mid-run is the
# realistic way that happens.
partition_stop() {
	local i="$1" _ out
	for _ in 1 2 3 4 5; do
		out="$(sudo -n iptables -D INPUT -p tcp --dport "${CLUSTER_PORTS[$i]}" -j DROP 2>&1)" || {
			case "$out" in
			*"No chain/target/match"* | *"does a matching rule exist"* | *"Bad rule"*) ;;
			*) echo "WARNING: could not remove the TCP partition rule for ${NODE_IDS[$i]}: $out" >&2 ;;
			esac
			break
		}
	done
	for _ in 1 2 3 4 5; do
		out="$(sudo -n iptables -D INPUT -p udp --dport "${HTTP_PORTS[$i]}" -j DROP 2>&1)" || {
			case "$out" in
			*"No chain/target/match"* | *"does a matching rule exist"* | *"Bad rule"*) ;;
			*) echo "WARNING: could not remove the UDP partition rule for ${NODE_IDS[$i]}: $out" >&2 ;;
			esac
			break
		}
	done
}

# heal_all_partitions removes every rule this script could have added,
# for every node, without consulting any state.
#
# It has to work that way: the fault loop runs as a background subshell,
# so a list of "currently partitioned nodes" it maintained would be
# invisible to the parent shell running this on the way out. Firewall
# rules outlive the process that installed them, and a run that dies
# holding one leaves a broken machine behind, so this is written to be
# correct with no memory of what happened.
heal_all_partitions() {
	# Deliberately NOT gated on partitions_available. That check ends in
	# `sudo -n`, which can succeed at injection time on a cached sudo
	# timestamp and fail fifteen minutes later at cleanup. Gating on it
	# meant a long run could end leaving a machine with DROP rules on its
	# broker ports and say nothing about it.
	command -v iptables >/dev/null 2>&1 || return 0
	local i
	for i in 0 1 2; do
		partition_stop "$i"
	done
}

cleanup() {
	local status=$?
	if [[ -n "${FAULT_PID:-}" ]] && kill -0 "$FAULT_PID" 2>/dev/null; then
		kill "$FAULT_PID" 2>/dev/null || true
		wait "$FAULT_PID" 2>/dev/null || true
	fi
	# Firewall rules outlive this process, so healing them is not
	# optional and must happen before anything that can fail.
	heal_all_partitions
	local i
	for i in 0 1 2; do
		stop_node "$i" >/dev/null 2>&1 || true
	done
	# start_node backgrounds the broker and then writes its pid file, so a
	# loop killed between the two leaves a broker with no pid file and
	# stop_node cannot see it. Sweep by binary path, which is unique to
	# this run's temp directory and so cannot match anyone else's broker.
	pkill -f "^$BIN serve" 2>/dev/null || true
	if [[ "$status" -ne 0 || "${NARAD_KEEP_CLUSTER_ARTIFACTS:-0}" == "1" ]]; then
		echo "artifacts kept at: $TMP_DIR" >&2
		echo "node logs: $LOG_DIR" >&2
	elif [[ "$OUT_DIR" != "$TMP_DIR"/* ]]; then
		# The verdict and history live outside the temp tree, so the
		# binaries and three node data directories can go. Repeated local
		# runs were leaving write-ahead logs behind in /tmp.
		rm -rf "$TMP_DIR"
	fi
}
trap cleanup EXIT

# ---- fault loop ---------------------------------------------------------

fault_loop() {
	local deadline=$(($(date +%s) + $1))
	local turn=0
	# Let the cluster settle and the first messages flow before breaking
	# anything, so the history has a clean prologue to compare against.
	sleep 15
	while [[ "$(date +%s)" -lt "$deadline" ]]; do
		local idx=$((RANDOM % 3))
		local node="${NODE_IDS[$idx]}"
		local start end
		# The window is recorded twice: a zero-width one the moment the
		# fault begins, and the real one when it ends. If this loop is
		# killed mid-fault (the parent does exactly that when the driver
		# exits early) the provisional record still marks the moment, so
		# the redeliveries that fault caused are attributed to it instead
		# of being reported as unexplained anomalies. The checker merges
		# the pair, keeping the wider window.
		if [[ $((turn % 2)) -eq 0 ]] || ! partitions_available; then
			start="$(now_ns)"
			record_fault "kill" "$node" "$start" "$start"
			echo "fault: killing $node"
			stop_node "$idx"
			sleep $((2 + RANDOM % 3))
			start_node "$idx"
			wait_ready "$idx" || true
			end="$(now_ns)"
			record_fault "kill" "$node" "$start" "$end"
		else
			start="$(now_ns)"
			record_fault "partition" "$node" "$start" "$start"
			echo "fault: partitioning $node from its peers"
			partition_start "$idx"
			sleep $((5 + RANDOM % 4))
			partition_stop "$idx"
			end="$(now_ns)"
			record_fault "partition" "$node" "$start" "$end"
		fi
		turn=$((turn + 1))
		# Longer than the grace period, so consecutive windows do not
		# merge into one continuous excuse. Fault coverage well below
		# 100% is what leaves the check something to discriminate with,
		# and the verdict reports the figure.
		sleep $((25 + RANDOM % 11))
	done
	echo "fault: window closed, leaving the drain quiet"
}

# ---- build --------------------------------------------------------------

export GOCACHE="${GOCACHE:-$DEFAULT_GO_CACHE}"
echo "building narad, driver and checker"
(
	cd "$ROOT_DIR"
	"$GO_BIN" build -o "$BIN" ./cmd/narad
	"$GO_BIN" build -o "$DRIVER_BIN" ./tests/integration
	"$GO_BIN" build -o "$CHECKER_BIN" ./tests/linearizability
)

detect_clock

PEERS="narad-1@127.0.0.1:${CLUSTER_PORTS[0]},narad-2@127.0.0.1:${CLUSTER_PORTS[1]},narad-3@127.0.0.1:${CLUSTER_PORTS[2]}"

for i in 0 1 2; do start_node "$i"; done
for i in 0 1 2; do wait_ready "$i"; done
echo "all nodes ready"

DRIVER_AUTH=()
if [[ "$SECURITY_ENABLED" == "true" ]]; then
	wait_admin_auth
	DRIVER_AUTH=(--username admin --password "$ADMIN_PASSWORD")
fi

if [[ "$FAULTS_ENABLED" -eq 1 ]]; then
	if partitions_available; then
		echo "faults: kills and partitions"
	else
		echo "faults: kills only (iptables with passwordless sudo not available)"
	fi
	# Leave the last stretch of the load window, and the whole drain,
	# fault-free. A drain interrupted by a fault reports a backlog that
	# says nothing about correctness.
	FAULT_WINDOW=$((DURATION_SECONDS - 30))
	[[ "$FAULT_WINDOW" -lt 30 ]] && FAULT_WINDOW=$((DURATION_SECONDS / 2))
	fault_loop "$FAULT_WINDOW" &
	FAULT_PID="$!"
else
	echo "faults: none (baseline run)"
fi

echo "load: ${DURATION_SECONDS}s at ${RATE}/s across ${TOPICS} topics, draining up to ${DRAIN_SECONDS}s"
DRIVER_STATUS=0
"$DRIVER_BIN" \
	--mode steady \
	--nodes "$(nodes_csv)" \
	"${DRIVER_AUTH[@]}" \
	--run-id "lin-$(date +%s)" \
	--topics "$TOPICS" \
	--partitions "$PARTITIONS" \
	--produce-concurrency 8 \
	--consume-concurrency 8 \
	--produce-rate "$RATE" \
	--duration "${DURATION_SECONDS}s" \
	--drain-timeout "${DRAIN_SECONDS}s" \
	--visibility-timeout "${VISIBILITY_SECONDS}s" \
	--report-every 15s \
	--history "$HISTORY" \
	--fatal-dup-after-ack=false \
	--cleanup=false || DRIVER_STATUS=$?

if [[ -n "${FAULT_PID:-}" ]] && kill -0 "$FAULT_PID" 2>/dev/null; then
	kill "$FAULT_PID" 2>/dev/null || true
	wait "$FAULT_PID" 2>/dev/null || true
	FAULT_PID=""
fi
heal_all_partitions

echo
echo "checking $(wc -l <"$HISTORY" | tr -d ' ') recorded operations against the model"
# A run this size records tens of thousands of operations. Anything near
# zero means the driver died early or never recorded, and a clean verdict
# over it would be meaningless; the checker reports UNKNOWN below this.
MIN_OPERATIONS=$((DURATION_SECONDS * 3))
CHECK_ARGS=(--history "$HISTORY" --faults "$FAULTS"
	--json "$VERDICT_JSON" --markdown "$VERDICT_MD"
	--min-operations "$MIN_OPERATIONS"
	--visualize "$OUT_DIR/violation.html")
[[ "$STRICT" -eq 1 ]] && CHECK_ARGS+=(--strict)

CHECK_STATUS=0
"$CHECKER_BIN" "${CHECK_ARGS[@]}" | tee "$VERDICT" || CHECK_STATUS=$?

# The history is the evidence; it compresses by roughly an order of
# magnitude and a nightly artifact is kept for weeks.
if [[ -s "$HISTORY" ]] && command -v gzip >/dev/null 2>&1; then
	# Never fatal. Under `set -e` a failure here would kill the script
	# before it copies the broker logs and prints why the run failed,
	# which is exactly the run where those matter.
	gzip -f "$HISTORY" || echo "warning: could not compress the history" >&2
fi

# The broker logs are the first thing anyone reads after a bad verdict,
# so they travel with the evidence rather than staying in a temp
# directory the CI runner throws away.
mkdir -p "$OUT_DIR/logs"
cp "$LOG_DIR"/*.log "$OUT_DIR/logs/" 2>/dev/null || true

echo
echo "artifacts: $OUT_DIR"
if [[ "$DRIVER_STATUS" -ne 0 ]]; then
	echo "FAIL the load driver exited $DRIVER_STATUS (see its output above: loss, or an id it never produced)"
fi
if [[ "$CHECK_STATUS" -ne 0 ]]; then
	echo "FAIL the linearizability check rejected this run"
fi
if [[ "$DRIVER_STATUS" -ne 0 || "$CHECK_STATUS" -ne 0 ]]; then
	exit 1
fi
echo "PASS linearizability nightly"
