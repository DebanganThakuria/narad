{{/*
Whether the scale-in guard hook runs (scaleInGuard.enabled, on by
default). Read with dig: values stored by an older chart lack the key.
*/}}
{{- define "narad.scaleInGuardEnabled" -}}
{{- dig "enabled" true (.Values.scaleInGuard | default dict) -}}
{{- end -}}

{{/*
Labels of the scale-in guard's hook pod. Deliberately NOT the release's
selector labels: the Services, the PodDisruptionBudget and the
NetworkPolicy select on those, and must not pick up this pod.
*/}}
{{- define "narad.scaleInGuardSelectorLabels" -}}
app.kubernetes.io/name: {{ include "narad.name" . }}-scale-in-guard
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: scale-in-guard
{{- end -}}

{{/*
The scale-in guard, a POSIX sh script run by the narad image's /bin/sh
(busybox on Alpine). It decides whether the change it guards (an upgrade,
or a rollback to the revision that rendered it) deletes pods that are
still in the cluster:

1. The pods at or above TARGET_REPLICAS that exist now are the ones the
   StatefulSet will delete. A pod exists when its headless DNS name
   resolves (the headless Service publishes not-ready pods too); a pod
   still Pending without an address counts as absent. StatefulSet
   ordinals are contiguous, so the scan stops after 16 missing ordinals
   in a row. None: the change deletes nothing, and it is allowed without
   calling the API, so an emergency rollback never waits on the API.
2. Otherwise read the member list (narad cluster members, a command
   every release has) from the internal Service, which routes to ready
   pods only. If that fails, ask each pod that stays and count a member
   that any of them lists: a lagging replica can only list more members,
   never fewer. The pods being deleted are not asked.
3. A listed member whose pod is being deleted blocks the change: a
   member is a Raft voter until decommission removes it, unless the view
   says "voter": false and it owns no partitions. A listed member with no
   pod is only noted: this change does not delete it.
4. If no member list can be read, refuse: the check could not be made.

Exit 0 allows the change; exit 1 fails the hook and with it the upgrade
or rollback. The reason is in the Job's log. Kubernetes rewrites $(NAME)
and $$ in container args, so the script uses neither.
*/}}
{{- define "narad.scaleInGuardScript" -}}
set -u
say() { printf 'scale-in guard: %s\n' "$*"; }
host_of() { printf '%s-%s.%s.%s.svc.%s' "$STS_NAME" "$1" "$HEADLESS_SERVICE" "$POD_NAMESPACE" "$CLUSTER_DOMAIN"; }
pod_exists() { getent hosts "$(host_of "$1")." >/dev/null 2>&1; }

target=$TARGET_REPLICAS

removing=""
i=$target
misses=0
while [ "$misses" -lt 16 ]; do
  if pod_exists "$i"; then
    removing="$removing $STS_NAME-$i"
    misses=0
  else
    misses=$((misses + 1))
  fi
  i=$((i + 1))
done
if [ -z "$removing" ]; then
  say "no pod at or above ordinal $target exists, so this change deletes none; allowed."
  exit 0
fi
say "this change sets replicaCount=$target and deletes:$removing"

errfile="${TMPDIR:-/tmp}/scale-in-guard.err"
started=$(date +%s)
members=""
lasterr=""
ask() {
  if out=$(NARAD_ADDR="$1" timeout 10 narad cluster members 2>"$errfile"); then
    members="$members
$out"
    return 0
  fi
  lasterr="$1: $(tail -n 3 "$errfile" 2>/dev/null | tr '\n' ' ')"
  return 1
}

if [ -n "$SERVICE_HOST" ]; then
  ask "http://$SERVICE_HOST:$API_PORT" || say "the Service did not answer ($lasterr); asking each pod that stays instead."
fi
if [ -z "$members" ]; then
  i=0
  while [ "$i" -lt "$target" ]; do
    if [ $(($(date +%s) - started)) -ge 120 ]; then
      say "stopped asking pods after 120s."
      break
    fi
    if pod_exists "$i"; then
      ask "http://$(host_of "$i"):$API_PORT" || true
    fi
    i=$((i + 1))
  done
fi

if [ -z "$members" ]; then
  say "REFUSED: could not read the cluster's member list, so it cannot check that the pods above were decommissioned first. Last error: $lasterr"
  if [ "$SECURITY_ENABLED" = "true" ]; then
    if [ -z "${NARAD_PASS:-}" ]; then
      say "the security Secret has no '$ADMIN_PASSWORD_KEY' key, so the guard has no credentials: add the root admin's current password under that key."
    else
      say "the guard signs in as admin with the '$ADMIN_PASSWORD_KEY' key of the security Secret, which must hold the root admin's current password."
    fi
  fi
  say "to go ahead once you have checked by hand that narad cluster members lists none of those pods, re-run the same command with --no-hooks."
  exit 1
fi

parsed=$(printf '%s\n' "$members" | awk '
  function flush() { if (id != "") print id, owned, voter }
  BEGIN { id = ""; owned = 0; voter = "unknown" }
  /"id"[ \t]*:/ {
    flush()
    v = $0; sub(/^[^:]*:[ \t]*"/, "", v); sub(/".*$/, "", v)
    id = v; owned = 0; voter = "unknown"; next
  }
  /"owned_partitions"[ \t]*:/ {
    v = $0; sub(/^[^:]*:[ \t]*/, "", v); sub(/[^0-9].*$/, "", v)
    owned = v + 0; next
  }
  /"voter"[ \t]*:/ { voter = ($0 ~ /true/) ? "true" : "false"; next }
  END { flush() }')

blocked=""
stale=""
seen=" "
while read -r id owned voter; do
  [ -n "$id" ] || continue
  case "$id" in "$STS_NAME"-*) ;; *) continue ;; esac
  ord=${id#"$STS_NAME"-}
  case "$ord" in '' | *[!0-9]*) continue ;; esac
  [ "$ord" -ge "$target" ] || continue
  if [ "$voter" = "false" ] && [ "$owned" -eq 0 ]; then continue; fi
  case "$seen" in *" $id "*) continue ;; esac
  seen="$seen$id "
  if pod_exists "$ord"; then
    blocked="$blocked $id (owns $owned partitions)"
  else
    stale="$stale $id"
  fi
done <<EOF
$parsed
EOF

if [ -n "$stale" ]; then
  say "note: listed as members, but no pod exists:$stale. This change does not delete them; decommission them to take them out of the cluster."
fi
if [ -n "$blocked" ]; then
  say "REFUSED: these pods are still cluster members (Raft voters until decommission removes them):$blocked"
  say "deleting a member without decommission strands the partitions it owns and can cost the cluster its Raft quorum. Run narad cluster decommission <pod> for each, wait until narad cluster members no longer lists it, then retry."
  say "to go ahead anyway once you have checked by hand, re-run the same command with --no-hooks."
  exit 1
fi
say "no pod being deleted is a cluster member; allowed."
exit 0
{{- end -}}
