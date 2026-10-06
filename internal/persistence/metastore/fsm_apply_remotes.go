package metastore

// Remotes registry apply handlers. Same determinism contract as
// fsm_apply.go: they run on Raft's FSM goroutine on every node and must
// produce identical bbolt state and identical business errors for
// identical inputs. The remotes domain version bumps only after a
// successful write, so the credential cache never rebuilds for nothing.

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"strings"

	bolt "go.etcd.io/bbolt"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
)

// PutRemoteOp is the body of opPutRemote: a whole new remote with its
// ciphertext and fingerprint, the salt when the ingress minted one (the
// cluster's first remote), and the proposal's seal time.
type PutRemoteOp struct {
	Record     domremote.Record `json:"record"`
	Salt       []byte           `json:"salt,omitempty"`
	SealedAtMs int64            `json:"sealed_at_ms"`
	Actor      string           `json:"actor"`
	RequestID  string           `json:"request_id"`
}

// RemoteFields are the named fields of a remote change; nil keeps a
// field. CAPEM set to "" removes the CA.
type RemoteFields struct {
	URL      *string                `json:"url,omitempty"`
	Username *string                `json:"username,omitempty"`
	CAPEM    *string                `json:"ca_pem,omitempty"`
	Limits   *domremote.LimitsPatch `json:"limits,omitempty"`
}

// UpdateRemoteOp is the body of opUpdateRemote: field-scoped, applied
// against the record as the FSM holds it. A new credential carries the
// tuple it was sealed against and the revision the ingress read;
// ExpectCredentialVersion makes a re-encrypt a compare-and-swap. Reseal
// marks a re-encrypt, which keeps the password's set-at and set-by.
type UpdateRemoteOp struct {
	Name                    string                 `json:"name"`
	Fields                  RemoteFields           `json:"fields"`
	Credential              *domremote.Envelope    `json:"credential,omitempty"`
	Fingerprint             string                 `json:"fingerprint,omitempty"`
	SealedAgainst           *domremote.SealedTuple `json:"sealed_against,omitempty"`
	ReadRevision            uint64                 `json:"read_revision"`
	ExpectCredentialVersion *uint64                `json:"expect_credential_version,omitempty"`
	Reseal                  bool                   `json:"reseal,omitempty"`
	SealedAtMs              int64                  `json:"sealed_at_ms"`
	Actor                   string                 `json:"actor"`
	RequestID               string                 `json:"request_id"`
}

// DeleteRemoteOp is the body of opDeleteRemote.
type DeleteRemoteOp struct {
	Name      string `json:"name"`
	Force     bool   `json:"force"`
	Actor     string `json:"actor"`
	RequestID string `json:"request_id"`
}

// RemoteChangedError is a sealed credential whose record moved under it,
// a URL, username or CA change without a credential, or a lost
// compare-and-swap. Revision is the record's revision as the FSM holds
// it, so the caller can say which change it lost to.
type RemoteChangedError struct {
	Revision uint64
}

func (e *RemoteChangedError) Error() string {
	return fmt.Sprintf("remote changed at revision %d, retry", e.Revision)
}

// Is makes the error match errs.ErrRemoteChanged.
func (e *RemoteChangedError) Is(target error) bool { return target == errs.ErrRemoteChanged }

// RemoteInUseError is a delete refused while remote children name the
// remote; Links are their "parent/child" pairs.
type RemoteInUseError struct {
	Links []string
}

func (e *RemoteInUseError) Error() string {
	return fmt.Sprintf("remote is used by %d remote children", len(e.Links))
}

// Is makes the error match errs.ErrRemoteInUse.
func (e *RemoteInUseError) Is(target error) bool { return target == errs.ErrRemoteInUse }

// errNothingToChange refuses an update that names no field and carries
// no credential.
var errNothingToChange = fmt.Errorf("%w: remote update changes nothing", errs.ErrInvalidArgument)

// applyPutRemote stores a new remote. The name must be free, fewer than
// MaxRemotes may exist, the op's salt must be the one stored (or, for
// the cluster's first remote, none may be stored and the op must carry
// one), and the key version must be under its seal cap.
func (f *fsmState) applyPutRemote(data []byte) error {
	var op PutRemoteOp
	if err := json.Unmarshal(data, &op); err != nil {
		return err
	}
	rec := op.Record
	if err := domremote.ValidateName(rec.Name); err != nil || rec.ID == "" || !sealedEnvelope(rec.Credential) {
		return fmt.Errorf("%w: malformed remote create", errs.ErrInvalidArgument)
	}
	var refused error
	err := f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketRemotes)
		keys, err := readRemoteKeys(tx)
		if err != nil {
			return err
		}
		prior := countSeal(&keys, rec.Credential.KV, op.SealedAtMs)
		refused = func() error {
			switch {
			case keys.Salt == nil && len(op.Salt) == 0,
				keys.Salt != nil && len(op.Salt) != 0 && !bytes.Equal(keys.Salt, op.Salt):
				return errs.ErrRemoteSaltRace
			case prior >= remotecred.SealCap:
				return errs.ErrRemoteKeyExhausted
			case b.Get([]byte(rec.Name)) != nil:
				return errs.ErrRemoteExists
			}
			n, err := countRemotes(b)
			if err != nil {
				return err
			}
			if n >= domremote.MaxRemotes {
				return errs.ErrRemoteLimit
			}
			return nil
		}()
		if refused == nil {
			if keys.Salt == nil {
				keys.Salt = bytes.Clone(op.Salt)
			}
			rec.CredentialVersion = 1
			rec.Revision = 1
			rec.PasswordSetAtMs = op.SealedAtMs
			rec.PasswordSetBy = op.Actor
			rec.CreatedAtMs = op.SealedAtMs
			rec.CreatedBy = op.Actor
			if err := putJSON(b, rec.Name, rec); err != nil {
				return err
			}
		}
		// The seal is counted whether the entry applied or not: a
		// refused entry's ciphertext still sits in the Raft log.
		return putJSON(b, domremote.KeysKey, keys)
	})
	if err != nil {
		return err
	}
	if refused == nil {
		f.versions.bumpRemotes()
	}
	return refused
}

// applyUpdateRemote applies a field-scoped change against the record as
// the FSM holds it. A URL, username or CA change needs a credential; a
// credential must have been sealed against the record the op's fields
// produce; an expected credential version makes it a compare-and-swap.
func (f *fsmState) applyUpdateRemote(data []byte) error {
	var op UpdateRemoteOp
	if err := json.Unmarshal(data, &op); err != nil {
		return err
	}
	if op.Credential != nil && (!sealedEnvelope(*op.Credential) || op.SealedAgainst == nil) {
		return fmt.Errorf("%w: malformed remote update", errs.ErrInvalidArgument)
	}
	var refused error
	err := f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketRemotes)
		var keys domremote.Keys
		var prior uint64
		if op.Credential != nil {
			var err error
			if keys, err = readRemoteKeys(tx); err != nil {
				return err
			}
			prior = countSeal(&keys, op.Credential.KV, op.SealedAtMs)
			if err := putJSON(b, domremote.KeysKey, keys); err != nil {
				return err
			}
		}
		raw, err := getRemoteRaw(tx, op.Name)
		if err != nil {
			refused = err
			return nil
		}
		var rec domremote.Record
		if err := json.Unmarshal(raw, &rec); err != nil {
			return err
		}
		next, err := applyRemoteFields(rec, op.Fields)
		if err != nil {
			refused = err
			return nil
		}
		if refused = checkRemoteUpdate(rec, next, op, prior); refused != nil {
			return nil
		}
		next.Revision = rec.Revision + 1
		if op.Credential != nil {
			next.Credential = *op.Credential
			next.CredentialVersion = rec.CredentialVersion + 1
			next.Fingerprint = op.Fingerprint
			if !op.Reseal {
				next.PasswordSetAtMs = op.SealedAtMs
				next.PasswordSetBy = op.Actor
			}
		}
		return putJSON(b, rec.Name, next)
	})
	if err != nil {
		return err
	}
	if refused == nil {
		f.versions.bumpRemotes()
	}
	return refused
}

// checkRemoteUpdate is applyUpdateRemote's refusal rule, given the
// record before (rec) and after (next) the op's fields.
func checkRemoteUpdate(rec, next domremote.Record, op UpdateRemoteOp, priorSeals uint64) error {
	bound := next.URL != rec.URL || next.Username != rec.Username || next.CAPEM != rec.CAPEM
	switch {
	case op.Credential == nil && !bound && op.Fields.Limits == nil:
		return errNothingToChange
	case op.Credential == nil && bound:
		// A stored password is only ever sent to the URL and user it
		// was entered with, verified against its trust anchor.
		return &RemoteChangedError{Revision: rec.Revision}
	case op.ExpectCredentialVersion != nil && *op.ExpectCredentialVersion != rec.CredentialVersion:
		return &RemoteChangedError{Revision: rec.Revision}
	}
	if op.Credential == nil {
		return nil
	}
	tuple, err := domremote.TupleOf(next)
	if err != nil || !tupleEqual(tuple, *op.SealedAgainst) {
		// Sealed on a replica whose record has since moved: the
		// ciphertext would not open for the record it would land on.
		return &RemoteChangedError{Revision: rec.Revision}
	}
	if priorSeals >= remotecred.SealCap {
		return errs.ErrRemoteKeyExhausted
	}
	return nil
}

// applyRemoteFields returns rec with the named fields set.
func applyRemoteFields(rec domremote.Record, f RemoteFields) (domremote.Record, error) {
	if f.URL != nil {
		rec.URL = *f.URL
	}
	if f.Username != nil {
		rec.Username = *f.Username
	}
	if f.CAPEM != nil {
		rec.CAPEM = *f.CAPEM
	}
	if f.Limits != nil {
		if err := f.Limits.Validate(); err != nil {
			return rec, fmt.Errorf("%w: %v", errs.ErrInvalidArgument, err)
		}
		rec.Limits = rec.Limits.WithDefaults().Apply(*f.Limits)
	}
	return rec, nil
}

// applyDeleteRemote deletes a remote, refusing while remote children
// name it unless forced. The keys entry stays: the salt never changes.
func (f *fsmState) applyDeleteRemote(data []byte) error {
	var op DeleteRemoteOp
	if err := json.Unmarshal(data, &op); err != nil {
		return err
	}
	var refused error
	err := f.update(func(tx *bolt.Tx) error {
		if _, err := getRemoteRaw(tx, op.Name); err != nil {
			refused = err
			return nil
		}
		links, err := remoteChildrenNaming(tx, op.Name)
		if err != nil {
			return err
		}
		if len(links) > 0 && !op.Force {
			refused = &RemoteInUseError{Links: links}
			return nil
		}
		return tx.Bucket(bucketRemotes).Delete([]byte(op.Name))
	})
	if err != nil {
		return err
	}
	if refused == nil {
		f.versions.bumpRemotes()
	}
	return refused
}

// reservedRemoteKey reports a key of the remotes bucket that is not a
// remote: remote names start with a letter.
func reservedRemoteKey(k string) bool { return strings.HasPrefix(k, "_") }

// countSeal counts one seal under kv and records the key version's
// first seal time from the proposal (never the FSM's clock, so every
// replica stores the same value). It returns the count before this one.
func countSeal(keys *domremote.Keys, kv string, sealedAtMs int64) uint64 {
	if keys.Versions == nil {
		keys.Versions = map[string]domremote.KeyVersion{}
	}
	v, seen := keys.Versions[kv]
	prior := v.Seals
	v.Seals++
	if !seen {
		v.FirstSealedAtMs = sealedAtMs
	}
	keys.Versions[kv] = v
	return prior
}

// sealedEnvelope reports an envelope in the one known format.
func sealedEnvelope(e domremote.Envelope) bool {
	return e.V == domremote.EnvelopeVersion && len(e.KV) == 16 && len(e.CT) > 0
}

// tupleEqual compares two sealed tuples; the trust anchors, digests,
// in constant time.
func tupleEqual(a, b domremote.SealedTuple) bool {
	return a.Name == b.Name && a.ID == b.ID && a.URL == b.URL && a.Username == b.Username &&
		subtle.ConstantTimeCompare([]byte(a.TrustAnchor), []byte(b.TrustAnchor)) == 1
}

func countRemotes(b *bolt.Bucket) (int, error) {
	n := 0
	err := b.ForEach(func(k, _ []byte) error {
		if !reservedRemoteKey(string(k)) {
			n++
		}
		return nil
	})
	return n, err
}

func putJSON(b *bolt.Bucket, key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.Put([]byte(key), raw)
}

// remoteExists reports whether the registry holds the named remote, in
// the caller's transaction, so an attach and a delete of the remote it
// names cannot interleave.
func remoteExists(tx *bolt.Tx, name string) (bool, error) {
	if name == "" || reservedRemoteKey(name) {
		return false, nil
	}
	b := tx.Bucket(bucketRemotes)
	if b == nil {
		return false, nil
	}
	return b.Get([]byte(name)) != nil, nil
}
