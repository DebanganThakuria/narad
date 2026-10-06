package metastore_test

import (
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func TestRemoteReadsOnAnEmptyRegistry(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.GetRemote("b"); !errors.Is(err, errs.ErrRemoteNotFound) || !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("GetRemote on empty registry = %v, want ErrRemoteNotFound and ErrNotFound", err)
	}
	if _, err := s.GetRemote("_keys"); !errors.Is(err, errs.ErrRemoteNotFound) {
		t.Fatalf("GetRemote(_keys) = %v, want not found", err)
	}
	list, err := s.ListRemotes()
	if err != nil || len(list) != 0 {
		t.Fatalf("ListRemotes = %v, %v", list, err)
	}
	keys, err := s.RemoteKeys()
	if err != nil || keys.Salt != nil || len(keys.Versions) != 0 {
		t.Fatalf("RemoteKeys = %+v, %v", keys, err)
	}
	links, err := s.RemoteChildrenOf("b")
	if err != nil || len(links) != 0 {
		t.Fatalf("RemoteChildrenOf = %v, %v", links, err)
	}
}
