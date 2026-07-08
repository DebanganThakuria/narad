package config

import (
	"reflect"
	"testing"
)

func TestInitialMembersEnvParsing(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_INITIAL_MEMBERS", " narad-0, narad-1 ,narad-2,, ")
	cfg := Default()
	if err := applyEnv(cfg); err != nil {
		t.Fatalf("applyEnv() error = %v", err)
	}
	want := []string{"narad-0", "narad-1", "narad-2"}
	if !reflect.DeepEqual(cfg.Cluster.InitialMembers, want) {
		t.Fatalf("InitialMembers = %v, want %v", cfg.Cluster.InitialMembers, want)
	}
}
