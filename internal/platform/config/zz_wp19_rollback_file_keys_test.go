package config

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// zzWP19RollbackTargetFileKeys are the config file keys the loader of the
// newest release (v3.0.1) accepts, as dotted paths ("[]" marks the
// elements of a list). That loader decodes the file with
// DisallowUnknownFields, so a node rolled back with any other key in its
// config file (the chart's narad.config) fails to start, as it does with a
// new storage key. Storage is one entry: StorageConfig decodes itself, and
// TestRollbackNotesTellOperatorsToRemoveNewStorageKeys checks its keys.
// When a release is cut, replace this set with that release's
// zzWP19FileKeys.
var zzWP19RollbackTargetFileKeys = []string{
	"http.addr",
	"http.pprof_addr",
	"http.read_timeout",
	"http.write_timeout",
	"http.idle_timeout",
	"http.shutdown_grace",
	"http.max_consume_wait",
	"http.max_header_bytes",
	"http.max_connections",
	"http.max_consume_in_flight_per_identity",
	"http.metrics_addr",
	"http.metrics_unauthenticated",
	"cluster.addr",
	"cluster.advertise_addr",
	"cluster.node_id",
	"cluster.peers[].id",
	"cluster.peers[].addr",
	"cluster.initial_members",
	"cluster.raft_snapshot_threshold",
	"cluster.raft_snapshot_interval",
	"cluster.raft_trailing_logs",
	"storage",
	"topic.default_partitions",
	"topic.max_partitions",
	"topic.default_retention_age_ms",
	"topic.default_visibility_timeout_ms",
	"topic.default_max_in_flight_per_partition",
	"topic.default_max_acked_ahead_per_partition",
	"fanout.max_batch_records",
	"fanout.max_batch_bytes",
	"fanout.linger_ms",
	"log.level",
	"log.format",
	"security.enabled",
	"security.allow_legacy_cluster_auth",
	"security.cluster_tls_cert_file",
	"security.cluster_tls_key_file",
	"security.cluster_tls_ca_file",
	"security.allow_insecure_cluster",
	"security.allow_plaintext_raft",
}

// zzWP19AwaitingRollbackNote are new file keys whose rollback note the
// documentation has not caught up with yet. Delete an entry in the same
// change that names the key, with an instruction to remove it, in every
// rollbackDocs section: the test fails while an entry outlives its need.
var zzWP19AwaitingRollbackNote = map[string]bool{
	"http.max_produce_in_flight_per_identity": true,
}

// Every file key outside storage that this loader accepts and the rollback
// target's loader rejects is named, together with an instruction to remove
// it, in the rollback notes, for the same reason as the storage keys: the
// documented rollback keeps the key in the file and the first rolled-back
// pod fails to start. Environment variables need no note; an older binary
// ignores the ones it does not know.
func TestZZWP19RollbackNotesTellOperatorsToRemoveNewFileKeys(t *testing.T) {
	var keys []string
	zzWP19FileKeys("", reflect.TypeFor[Config](), &keys)
	if !slices.Contains(keys, "http.max_consume_in_flight_per_identity") {
		t.Fatalf("file key walk missed http.max_consume_in_flight_per_identity: %q", keys)
	}
	for _, key := range keys {
		if slices.Contains(zzWP19RollbackTargetFileKeys, key) {
			continue
		}
		var missing []string
		for _, doc := range rollbackDocs {
			if !zzWP19BlockSaysRemove(t, doc.path, doc.heading, key) {
				missing = append(missing, doc.path+", section "+doc.heading)
			}
		}
		switch {
		case !zzWP19AwaitingRollbackNote[key]:
			for _, where := range missing {
				t.Errorf("%s: no paragraph or list item names file key %q and says to remove it before a rollback; the rollback target's loader rejects it",
					where, key)
			}
		case len(missing) == 0:
			t.Errorf("every rollback doc now tells operators to remove %q; delete its zzWP19AwaitingRollbackNote entry so this test keeps them doing so", key)
		}
	}
}

// zzWP19FileKeys appends the dotted path of every key the config file
// decoder accepts under typ. A type that decodes itself (Duration,
// StorageConfig) is one key.
func zzWP19FileKeys(prefix string, typ reflect.Type, out *[]string) {
	unmarshaler := reflect.TypeFor[json.Unmarshaler]()
	for f := range typ.Fields() {
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "-" || !f.IsExported() {
			continue
		}
		if name == "" {
			name = f.Name
		}
		ft, path := f.Type, prefix+name
		for ft.Kind() == reflect.Pointer || ft.Kind() == reflect.Slice || ft.Kind() == reflect.Array {
			if ft.Kind() != reflect.Pointer {
				path += "[]"
			}
			ft = ft.Elem()
		}
		if ft.Kind() != reflect.Struct || reflect.PointerTo(ft).Implements(unmarshaler) {
			*out = append(*out, strings.TrimSuffix(path, "[]"))
			continue
		}
		zzWP19FileKeys(path+".", ft, out)
	}
}

// zzWP19BlockSaysRemove reports whether one paragraph or list item of the
// section under heading names key (its dotted path, or its last segment in
// backticks) and contains the word "remove".
func zzWP19BlockSaysRemove(t *testing.T, path, heading, key string) bool {
	t.Helper()
	leaf := key[strings.LastIndexByte(key, '.')+1:]
	for _, block := range markdownBlocks(sectionOf(t, path, heading)) {
		if (strings.Contains(block, key) || strings.Contains(block, "`"+leaf+"`")) && wordRemove.MatchString(block) {
			return true
		}
	}
	return false
}
