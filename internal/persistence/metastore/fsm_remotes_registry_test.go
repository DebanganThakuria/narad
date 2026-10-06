package metastore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	bolt "go.etcd.io/bbolt"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
)

const (
	testKV    = "0123456789abcdef"
	testKVNew = "fedcba9876543210"
)

var testSalt = bytes.Repeat([]byte{7}, 32)

func env(kv string, b byte) domremote.Envelope {
	return domremote.Envelope{V: 1, KV: kv, CT: bytes.Repeat([]byte{b}, 40)}
}

func record(name string) domremote.Record {
	return domremote.Record{
		Name: name, ID: "id-" + name, URL: "https://" + name + ".example", Username: "repl",
		Credential: env(testKV, 1), Fingerprint: "aaaaaaaaaaaa", Limits: domremote.DefaultLimits(),
	}
}

func applyJSON(t *testing.T, apply func([]byte) error, op any) error {
	t.Helper()
	raw, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	return apply(raw)
}

func put(t *testing.T, f *fsmState, rec domremote.Record, salt []byte, at int64) error {
	t.Helper()
	return applyJSON(t, f.applyPutRemote, PutRemoteOp{Record: rec, Salt: salt, SealedAtMs: at, Actor: "alice", RequestID: "r1"})
}

func readRecord(t *testing.T, f *fsmState, name string) domremote.Record {
	t.Helper()
	var r domremote.Record
	err := f.view(func(tx *bolt.Tx) error {
		raw, err := getRemoteRaw(tx, name)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, &r)
	})
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return r
}

func readKeys(t *testing.T, f *fsmState) domremote.Keys {
	t.Helper()
	var k domremote.Keys
	if err := f.view(func(tx *bolt.Tx) (err error) { k, err = readRemoteKeys(tx); return err }); err != nil {
		t.Fatal(err)
	}
	return k
}

func tupleOf(t *testing.T, r domremote.Record) *domremote.SealedTuple {
	t.Helper()
	tu, err := domremote.TupleOf(r)
	if err != nil {
		t.Fatal(err)
	}
	return &tu
}

func TestPutRemoteStoresTheRecordAndTheSalt(t *testing.T) {
	f := openTestFSM(t)
	v0 := f.versions.remotesVersion()
	if err := put(t, f, record("b"), testSalt, 1000); err != nil {
		t.Fatal(err)
	}
	r := readRecord(t, f, "b")
	if r.CredentialVersion != 1 || r.Revision != 1 || r.PasswordSetAtMs != 1000 || r.PasswordSetBy != "alice" || r.CreatedBy != "alice" || r.CreatedAtMs != 1000 {
		t.Fatalf("stored record = %+v", r)
	}
	k := readKeys(t, f)
	if !bytes.Equal(k.Salt, testSalt) || k.Versions[testKV].Seals != 1 || k.Versions[testKV].FirstSealedAtMs != 1000 {
		t.Fatalf("keys = %+v", k)
	}
	if f.versions.remotesVersion() <= v0 {
		t.Fatal("remotes version did not move")
	}
}

func TestPutRemoteRefusals(t *testing.T) {
	f := openTestFSM(t)
	if err := put(t, f, record("b"), nil, 1); !errors.Is(err, errs.ErrRemoteSaltRace) {
		t.Fatalf("first create without a salt: %v", err)
	}
	if err := put(t, f, record("b"), testSalt, 1); err != nil {
		t.Fatal(err)
	}
	v := f.versions.remotesVersion()
	if err := put(t, f, record("b"), testSalt, 2); !errors.Is(err, errs.ErrRemoteExists) {
		t.Fatalf("duplicate name: %v", err)
	}
	if err := put(t, f, record("c"), bytes.Repeat([]byte{9}, 32), 3); !errors.Is(err, errs.ErrRemoteSaltRace) {
		t.Fatalf("a different salt: %v", err)
	}
	if err := put(t, f, record("c"), nil, 4); err != nil {
		t.Fatalf("a later create without a salt: %v", err)
	}
	if f.versions.remotesVersion() <= v {
		t.Fatal("the successful create did not move the version")
	}
	v = f.versions.remotesVersion()
	bad := record("d")
	bad.Credential.V = 2
	if err := put(t, f, bad, nil, 5); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("unknown envelope format: %v", err)
	}
	if f.versions.remotesVersion() != v {
		t.Fatal("a refused create moved the version")
	}
	// Refused entries are counted: four ciphertexts entered the log
	// under testKV after the first refusal (which carried one too).
	if got := readKeys(t, f).Versions[testKV].Seals; got != 5 {
		t.Fatalf("seals = %d, want 5 (refused entries included)", got)
	}
	if got := readKeys(t, f).Versions[testKV].FirstSealedAtMs; got != 1 {
		t.Fatalf("first_sealed_at_ms = %d, want the first proposal's 1", got)
	}
}

func TestPutRemoteCap(t *testing.T) {
	f := openTestFSM(t)
	for i := range domremote.MaxRemotes {
		if err := put(t, f, record(fmt.Sprintf("r%d", i)), testSalt, 1); err != nil {
			t.Fatalf("remote %d: %v", i, err)
		}
	}
	if err := put(t, f, record("one-more"), testSalt, 1); !errors.Is(err, errs.ErrRemoteLimit) {
		t.Fatalf("65th remote: %v", err)
	}
}

func TestSealCapRefusesFurtherSeals(t *testing.T) {
	old := remotecred.SealCap
	remotecred.SealCap = 2
	t.Cleanup(func() { remotecred.SealCap = old })
	f := openTestFSM(t)
	if err := put(t, f, record("a"), testSalt, 1); err != nil {
		t.Fatal(err)
	}
	if err := put(t, f, record("b"), nil, 1); err != nil {
		t.Fatal(err)
	}
	if err := put(t, f, record("c"), nil, 1); !errors.Is(err, errs.ErrRemoteKeyExhausted) {
		t.Fatalf("third seal under a cap of 2: %v", err)
	}
	// Under a new key the budget starts over.
	c := record("c")
	c.Credential = env(testKVNew, 3)
	if err := put(t, f, c, nil, 9); err != nil {
		t.Fatalf("a seal under a new key: %v", err)
	}
	k := readKeys(t, f)
	if k.Versions[testKV].Seals != 3 || k.Versions[testKVNew].Seals != 1 || k.Versions[testKVNew].FirstSealedAtMs != 9 {
		t.Fatalf("keys = %+v", k)
	}
}

func update(t *testing.T, f *fsmState, op UpdateRemoteOp) error {
	t.Helper()
	return applyJSON(t, f.applyUpdateRemote, op)
}

func TestUpdateRemoteFieldsAndCredential(t *testing.T) {
	f := openTestFSM(t)
	if err := put(t, f, record("b"), testSalt, 1); err != nil {
		t.Fatal(err)
	}
	base := readRecord(t, f, "b")

	// Limits alone: no credential needed, credential version kept.
	n := 64
	if err := update(t, f, UpdateRemoteOp{Name: "b", Fields: RemoteFields{Limits: &domremote.LimitsPatch{MaxInFlight: &n}}, Actor: "bob"}); err != nil {
		t.Fatal(err)
	}
	r := readRecord(t, f, "b")
	if r.Limits.MaxInFlight != 64 || r.Limits.RequestTimeoutMs != domremote.DefaultRequestTimeoutMs || r.CredentialVersion != 1 || r.Revision != 2 {
		t.Fatalf("after limits = %+v", r)
	}

	// A URL change without a credential is refused.
	url := "https://moved.example"
	for _, fields := range []RemoteFields{{URL: &url}, {Username: ptr("admin")}, {CAPEM: ptr(testCA)}} {
		err := update(t, f, UpdateRemoteOp{Name: "b", Fields: fields})
		var changed *RemoteChangedError
		if !errors.As(err, &changed) || !errors.Is(err, errs.ErrRemoteChanged) || changed.Revision != 2 {
			t.Fatalf("bound field without a credential: %v", err)
		}
	}

	// A URL change with a credential sealed against the new record.
	next := base
	next.URL = url
	if err := update(t, f, UpdateRemoteOp{
		Name: "b", Fields: RemoteFields{URL: &url}, Credential: ptr(env(testKV, 2)), Fingerprint: "bbbbbbbbbbbb",
		SealedAgainst: tupleOf(t, next), ReadRevision: 2, SealedAtMs: 50, Actor: "carol",
	}); err != nil {
		t.Fatal(err)
	}
	r = readRecord(t, f, "b")
	if r.URL != url || r.CredentialVersion != 2 || r.Revision != 3 || r.Fingerprint != "bbbbbbbbbbbb" || r.PasswordSetBy != "carol" || r.PasswordSetAtMs != 50 || r.Limits.MaxInFlight != 64 {
		t.Fatalf("after URL change = %+v", r)
	}
	if err := update(t, f, UpdateRemoteOp{Name: "b"}); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("an empty update: %v", err)
	}
	if err := update(t, f, UpdateRemoteOp{Name: "ghost", Fields: RemoteFields{Limits: &domremote.LimitsPatch{MaxInFlight: &n}}}); !errors.Is(err, errs.ErrRemoteNotFound) {
		t.Fatalf("missing remote: %v", err)
	}
}

// A password sealed on a lagging replica, against a record whose URL,
// username or CA has since moved, never lands.
func TestUpdateRemoteRefusesACredentialSealedAgainstAnOlderRecord(t *testing.T) {
	for _, field := range []string{"url", "username", "ca_pem"} {
		t.Run(field, func(t *testing.T) {
			f := openTestFSM(t)
			if err := put(t, f, record("b"), testSalt, 1); err != nil {
				t.Fatal(err)
			}
			stale := readRecord(t, f, "b") // what the lagging ingress sealed against
			moved := stale
			fields := RemoteFields{}
			switch field {
			case "url":
				moved.URL = "https://elsewhere.example"
				fields.URL = &moved.URL
			case "username":
				moved.Username = "someone-else"
				fields.Username = &moved.Username
			case "ca_pem":
				moved.CAPEM = testCA
				fields.CAPEM = &moved.CAPEM
			}
			if err := update(t, f, UpdateRemoteOp{Name: "b", Fields: fields, Credential: ptr(env(testKV, 2)), SealedAgainst: tupleOf(t, moved)}); err != nil {
				t.Fatal(err)
			}
			seals := readKeys(t, f).Versions[testKV].Seals
			// The password change sealed against the old record.
			err := update(t, f, UpdateRemoteOp{Name: "b", Credential: ptr(env(testKV, 3)), SealedAgainst: tupleOf(t, stale), ReadRevision: 1})
			var changed *RemoteChangedError
			if !errors.As(err, &changed) || changed.Revision != 2 {
				t.Fatalf("stale seal: %v", err)
			}
			if r := readRecord(t, f, "b"); r.CredentialVersion != 2 || !bytes.Equal(r.Credential.CT, env(testKV, 2).CT) {
				t.Fatalf("a stale ciphertext landed: %+v", r)
			}
			if got := readKeys(t, f).Versions[testKV].Seals; got != seals+1 {
				t.Fatalf("refused seal not counted: %d, want %d", got, seals+1)
			}
		})
	}
}

// The re-encrypt compare-and-swap loses to a password change that
// landed first: the newer ciphertext is already under the current key.
func TestReencryptCompareAndSwap(t *testing.T) {
	f := openTestFSM(t)
	if err := put(t, f, record("b"), testSalt, 1); err != nil {
		t.Fatal(err)
	}
	r := readRecord(t, f, "b")
	cv := r.CredentialVersion
	// A concurrent password change lands first.
	if err := update(t, f, UpdateRemoteOp{Name: "b", Credential: ptr(env(testKVNew, 5)), SealedAgainst: tupleOf(t, r), Actor: "dave", SealedAtMs: 70}); err != nil {
		t.Fatal(err)
	}
	err := update(t, f, UpdateRemoteOp{Name: "b", Credential: ptr(env(testKVNew, 6)), SealedAgainst: tupleOf(t, r), ExpectCredentialVersion: &cv, Reseal: true})
	if !errors.Is(err, errs.ErrRemoteChanged) {
		t.Fatalf("stale re-encrypt: %v", err)
	}
	after := readRecord(t, f, "b")
	if !bytes.Equal(after.Credential.CT, env(testKVNew, 5).CT) || after.PasswordSetBy != "dave" {
		t.Fatalf("the re-encrypt overwrote a newer password: %+v", after)
	}
	// A current re-encrypt applies and keeps who set the password.
	cv = after.CredentialVersion
	if err := update(t, f, UpdateRemoteOp{Name: "b", Credential: ptr(env(testKVNew, 7)), Fingerprint: "cccccccccccc", SealedAgainst: tupleOf(t, after), ExpectCredentialVersion: &cv, Reseal: true, Actor: "root", SealedAtMs: 99}); err != nil {
		t.Fatal(err)
	}
	final := readRecord(t, f, "b")
	if final.CredentialVersion != cv+1 || final.PasswordSetBy != "dave" || final.PasswordSetAtMs != 70 || final.Fingerprint != "cccccccccccc" {
		t.Fatalf("after re-encrypt = %+v", final)
	}
}

func TestDeleteRemoteRefusedWhileAStubNamesIt(t *testing.T) {
	f := openTestFSM(t)
	if err := put(t, f, record("b"), testSalt, 1); err != nil {
		t.Fatal(err)
	}
	// A hand-written stub record (package B creates real ones).
	putRaw(t, f, bucketTopics, "orders-to-b", topic.Topic{Name: "orders-to-b", Parent: "orders", Role: topic.RoleChild, Remote: &topic.RemoteLink{Name: "b", Topic: "orders"}})
	v := f.versions.remotesVersion()
	err := applyJSON(t, f.applyDeleteRemote, DeleteRemoteOp{Name: "b"})
	var inUse *RemoteInUseError
	if !errors.As(err, &inUse) || !errors.Is(err, errs.ErrRemoteInUse) || len(inUse.Links) != 1 || inUse.Links[0] != "orders/orders-to-b" {
		t.Fatalf("delete while linked: %v", err)
	}
	if f.versions.remotesVersion() != v {
		t.Fatal("a refused delete moved the version")
	}
	if err := applyJSON(t, f.applyDeleteRemote, DeleteRemoteOp{Name: "b", Force: true}); err != nil {
		t.Fatalf("forced delete: %v", err)
	}
	if f.versions.remotesVersion() <= v {
		t.Fatal("the delete did not move the version")
	}
	if err := applyJSON(t, f.applyDeleteRemote, DeleteRemoteOp{Name: "b"}); !errors.Is(err, errs.ErrRemoteNotFound) {
		t.Fatalf("delete of a gone remote: %v", err)
	}
	if !bytes.Equal(readKeys(t, f).Salt, testSalt) {
		t.Fatal("the salt did not survive the delete")
	}
}

// A snapshot carries the remotes bucket, keys entry included, byte for
// byte, and a restore moves the remotes version.
func TestRemotesSurviveASnapshotByteForByte(t *testing.T) {
	src := openTestFSM(t)
	if err := put(t, src, record("b"), testSalt, 1); err != nil {
		t.Fatal(err)
	}
	image := snapshotImage(t, src)
	dst := openTestFSM(t)
	v := dst.versions.remotesVersion()
	if err := dst.Restore(io.NopCloser(bytes.NewReader(image))); err != nil {
		t.Fatal(err)
	}
	if dst.versions.remotesVersion() <= v {
		t.Fatal("restore did not move the remotes version")
	}
	dump := func(f *fsmState) map[string][]byte {
		out := map[string][]byte{}
		_ = f.view(func(tx *bolt.Tx) error {
			return tx.Bucket(bucketRemotes).ForEach(func(k, v []byte) error { out[string(k)] = bytes.Clone(v); return nil })
		})
		return out
	}
	a, b := dump(src), dump(dst)
	if len(a) != 2 || len(a) != len(b) {
		t.Fatalf("bucket sizes %d %d", len(a), len(b))
	}
	for k, v := range a {
		if !bytes.Equal(v, b[k]) {
			t.Fatalf("key %s differs after restore", k)
		}
	}
}

func ptr[T any](v T) *T { return &v }

// testCA is a PEM block of type CERTIFICATE; the FSM only digests the
// block's bytes, so they need not parse as a certificate.
const testCA = `-----BEGIN CERTIFICATE-----
MIIBHTCB0KADAgECAgEBMAUGAytlcDAOMQwwCgYDVQQDEwN0Y2EwHhcNMjYwMTAx
MDAwMDAwWhcNMzYwMTAxMDAwMDAwWjAOMQwwCgYDVQQDEwN0Y2EwKjAFBgMrZXAD
IQDSc3mPcCI0K2aYx6cM3wpxhSvhL1lRk7G7xWsrV2dpnKNFMEMwDgYDVR0PAQH/
BAQDAgIEMBIGA1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0OBBYEFM0AGIWd4Hd9ocVq
7ioOE6XrxMk6MAUGAytlcANBAJKZQd3i3/EpT/0q3bJfT3rUgmm/3a2BUZXw76Ws
k3n9kPj0B0BvdNvnWmh9m3b2y1lqV0cUgVmP9bVTNpT6pAM=
-----END CERTIFICATE-----
`
