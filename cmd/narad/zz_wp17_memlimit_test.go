package main

import (
	"os"
	"path/filepath"
	"testing"
)

// zzWP17CgroupRoot builds a fake filesystem root holding the given files
// (paths relative to the root).
func zzWP17CgroupRoot(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func zzWP17NoEnv(string) (string, bool) { return "", false }

func TestZZWP17CgroupMemoryLimit(t *testing.T) {
	const gib = int64(1) << 30
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  int64
		file  string // expected source, relative to the root
	}{
		{
			name: "v2 with a cgroup namespace",
			files: map[string]string{
				"proc/self/cgroup":         "0::/\n",
				"sys/fs/cgroup/memory.max": "2147483648\n",
			},
			want: 2 * gib, file: "sys/fs/cgroup/memory.max",
		},
		{
			name: "v2 nested cgroup, limit on the leaf",
			files: map[string]string{
				"proc/self/cgroup":                           "0::/kubepods/pod1/ctr\n",
				"sys/fs/cgroup/kubepods/pod1/ctr/memory.max": "1073741824\n",
				"sys/fs/cgroup/kubepods/pod1/memory.max":     "max\n",
			},
			want: gib, file: "sys/fs/cgroup/kubepods/pod1/ctr/memory.max",
		},
		{
			name: "v2 leaf unlimited, an ancestor is the tightest",
			files: map[string]string{
				"proc/self/cgroup": "0::/system.slice/narad.service\n",
				"sys/fs/cgroup/system.slice/narad.service/memory.max": "max\n",
				"sys/fs/cgroup/system.slice/memory.max":               "3221225472\n",
			},
			want: 3 * gib, file: "sys/fs/cgroup/system.slice/memory.max",
		},
		{
			name: "v2 with no limit anywhere",
			files: map[string]string{
				"proc/self/cgroup":                    "0::/user.slice\n",
				"sys/fs/cgroup/user.slice/memory.max": "max\n",
			},
		},
		{
			name: "v1 bind-mounted container cgroup",
			files: map[string]string{
				"proc/self/cgroup":                           "12:memory:/docker/abc\n4:cpu,cpuacct:/docker/abc\n0::/\n",
				"sys/fs/cgroup/memory/memory.limit_in_bytes": "536870912\n",
				// Hybrid host: the unified hierarchy carries no memory
				// controller and must not be consulted.
				"sys/fs/cgroup/memory.max": "1024\n",
			},
			want: gib / 2, file: "sys/fs/cgroup/memory/memory.limit_in_bytes",
		},
		{
			name: "v1 unlimited reads as about 2^63",
			files: map[string]string{
				"proc/self/cgroup":                           "9:memory:/\n",
				"sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n",
			},
		},
		{
			name: "unreadable /proc/self/cgroup falls back to the mount roots",
			files: map[string]string{
				"sys/fs/cgroup/memory.max": "4294967296",
			},
			want: 4 * gib, file: "sys/fs/cgroup/memory.max",
		},
		{
			name: "garbage and zero are not limits",
			files: map[string]string{
				"proc/self/cgroup":           "0::/a\n",
				"sys/fs/cgroup/a/memory.max": "lots\n",
				"sys/fs/cgroup/memory.max":   "0\n",
			},
		},
		{
			name:  "no cgroup files at all",
			files: map[string]string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := zzWP17CgroupRoot(t, tc.files)
			got, source := cgroupMemoryLimit(root)
			if got != tc.want {
				t.Fatalf("cgroupMemoryLimit = %d, want %d (source %q)", got, tc.want, source)
			}
			wantSource := ""
			if tc.file != "" {
				wantSource = filepath.Join(root, filepath.FromSlash(tc.file))
			}
			if source != wantSource {
				t.Fatalf("source = %q, want %q", source, wantSource)
			}
		})
	}
}

func TestZZWP17ContainerMemoryLimit(t *testing.T) {
	root := zzWP17CgroupRoot(t, map[string]string{
		"proc/self/cgroup":         "0::/\n",
		"sys/fs/cgroup/memory.max": "1000000000\n",
	})

	soft, limit, _ := containerMemoryLimit(zzWP17NoEnv, root)
	if limit != 1_000_000_000 || soft != 900_000_000 {
		t.Fatalf("containerMemoryLimit = soft %d, limit %d; want 900000000 of 1000000000", soft, limit)
	}

	// An explicit GOMEMLIMIT always wins, whatever it says.
	for _, v := range []string{"1GiB", "off"} {
		env := func(k string) (string, bool) {
			if k == "GOMEMLIMIT" {
				return v, true
			}
			return "", false
		}
		if soft, _, _ := containerMemoryLimit(env, root); soft != 0 {
			t.Fatalf("GOMEMLIMIT=%s: containerMemoryLimit set %d, want no override", v, soft)
		}
	}
	// An empty GOMEMLIMIT is the same as none.
	empty := func(k string) (string, bool) { return "", k == "GOMEMLIMIT" }
	if soft, _, _ := containerMemoryLimit(empty, root); soft != 900_000_000 {
		t.Fatalf("GOMEMLIMIT empty: soft = %d, want 900000000", soft)
	}

	// No finite limit: nothing to set.
	unlimited := zzWP17CgroupRoot(t, map[string]string{
		"proc/self/cgroup":         "0::/\n",
		"sys/fs/cgroup/memory.max": "max\n",
	})
	if soft, _, _ := containerMemoryLimit(zzWP17NoEnv, unlimited); soft != 0 {
		t.Fatalf("unlimited cgroup: soft = %d, want 0", soft)
	}
}
