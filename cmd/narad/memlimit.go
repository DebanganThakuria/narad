package main

// Go runtime memory limit from the container's cgroup limit.
//
// The Go runtime only knows a memory limit when GOMEMLIMIT says so; left
// alone it lets the heap grow to twice the live heap between GCs and
// never looks at the container it runs in, so a burst (a schema hydrate
// herd, a fan-out catch-up) can take a pod past its cgroup limit and get
// it OOM-killed while the collector still thinks there is room. Only the
// ops doc's example values set GOMEMLIMIT (through the chart's extraEnv);
// the chart itself, a brew install and a docker run do not, so a pod
// given just resources.limits.memory would otherwise run with no soft
// limit. So at startup, when GOMEMLIMIT is unset and the process runs
// under a cgroup memory limit, serve sets the soft limit to
// containerMemoryLimitPercent of it. GOGC is left alone: raising it
// showed no throughput, latency or CPU gain.

import (
	"bufio"
	"bytes"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	// containerMemoryLimitPercent is the share of the cgroup limit the Go
	// soft limit gets. The rest is headroom for what the runtime does not
	// count against it but the cgroup charges: page cache of the logs the
	// broker writes, and memory mapped outside the Go heap.
	containerMemoryLimitPercent = 90

	// cgroupUnlimitedBytes: a limit at or above this is the kernel's
	// "unlimited" (cgroup v1 reports it as about 2^63 rounded down to a
	// page), not a real one; no machine this runs on has a petabyte.
	cgroupUnlimitedBytes = 1 << 50
)

// applyContainerMemoryLimit sets the Go runtime's soft memory limit from
// the process's cgroup memory limit, and logs it, unless GOMEMLIMIT is set
// (an explicit setting always wins) or no finite limit applies. Linux
// only; a no-op elsewhere.
func applyContainerMemoryLimit(log *slog.Logger) {
	if runtime.GOOS != "linux" {
		return
	}
	soft, limit, source := containerMemoryLimit(os.LookupEnv, "/")
	if soft <= 0 {
		return
	}
	debug.SetMemoryLimit(soft)
	log.Info("go memory limit set from the cgroup memory limit (set GOMEMLIMIT to override)",
		"gomemlimit_bytes", soft, "cgroup_limit_bytes", limit, "cgroup_file", source)
}

// containerMemoryLimit returns the Go soft memory limit to set, the cgroup
// limit it derives from, and the file that limit came from, reading the
// cgroup files under root. soft is 0 when GOMEMLIMIT is set in the
// environment or no finite cgroup limit applies.
func containerMemoryLimit(lookupEnv func(string) (string, bool), root string) (soft, limit int64, source string) {
	if v, ok := lookupEnv("GOMEMLIMIT"); ok && strings.TrimSpace(v) != "" {
		return 0, 0, ""
	}
	limit, source = cgroupMemoryLimit(root)
	if limit <= 0 {
		return 0, 0, ""
	}
	return limit / 100 * containerMemoryLimitPercent, limit, source
}

// cgroupMemoryLimit returns the tightest memory limit set on the cgroup
// this process belongs to or any of its ancestors, and the file it was
// read from, or 0 when none is finite. It understands cgroup v2 (unified,
// memory.max) and v1 (memory controller, memory.limit_in_bytes, preferred
// when a hybrid host mounts both). Inside a container the cgroup
// directories either start at the container's own cgroup (a cgroup
// namespace) or have it bind-mounted at the controller root, so the walk
// covers the process's path relative to the mount and ends at the mount
// root.
func cgroupMemoryLimit(root string) (int64, string) {
	v1Path, v2Path, haveV1, haveV2 := "/", "/", false, false
	if data, err := os.ReadFile(filepath.Join(root, "proc", "self", "cgroup")); err == nil {
		v1Path, haveV1, v2Path, haveV2 = parseProcCgroup(data)
	}
	cgroupFS := filepath.Join(root, "sys", "fs", "cgroup")
	if haveV1 || !haveV2 {
		if limit, source := tightestCgroupLimit(filepath.Join(cgroupFS, "memory"), v1Path, "memory.limit_in_bytes"); limit > 0 {
			return limit, source
		}
		if haveV1 {
			return 0, ""
		}
	}
	return tightestCgroupLimit(cgroupFS, v2Path, "memory.max")
}

// parseProcCgroup reads /proc/self/cgroup: the path of the v1 cgroup that
// carries the memory controller ("N:...memory...:/path") and of the v2
// unified cgroup ("0::/path"), and whether each was present.
func parseProcCgroup(data []byte) (v1Path string, haveV1 bool, v2Path string, haveV2 bool) {
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		fields := strings.SplitN(sc.Text(), ":", 3)
		if len(fields) != 3 {
			continue
		}
		if fields[0] == "0" && fields[1] == "" {
			v2Path, haveV2 = fields[2], true
			continue
		}
		for _, controller := range strings.Split(fields[1], ",") {
			if controller == "memory" {
				v1Path, haveV1 = fields[2], true
			}
		}
	}
	return v1Path, haveV1, v2Path, haveV2
}

// tightestCgroupLimit reads file in the cgroup directory for rel under
// mount and in each of its ancestors up to the mount root, and returns
// the smallest finite limit found and its file, or 0.
func tightestCgroupLimit(mount, rel, file string) (int64, string) {
	var best int64
	var source string
	dir := path.Clean("/" + rel)
	for {
		p := filepath.Join(mount, filepath.FromSlash(dir), file)
		if limit, ok := readCgroupLimit(p); ok && (best == 0 || limit < best) {
			best, source = limit, p
		}
		if dir == "/" {
			return best, source
		}
		dir = path.Dir(dir)
	}
}

// readCgroupLimit parses a memory.max or memory.limit_in_bytes file. ok is
// false when the file is missing or unreadable, says "max", or holds a
// value that is not a real limit.
func readCgroupLimit(file string) (int64, bool) {
	data, err := os.ReadFile(file)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return 0, false
	}
	limit, err := strconv.ParseInt(s, 10, 64)
	if err != nil || limit <= 0 || limit >= cgroupUnlimitedBytes {
		return 0, false
	}
	return limit, true
}
