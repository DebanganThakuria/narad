package main

import "testing"

// The transition table is the whole specification, so it is asserted
// exhaustively rather than through the histories that happen to exercise
// it. A wrong cell here turns into either a false alarm on a healthy
// broker or silence on a broken one, and this project has already paid
// for a checker whose bugs imitated broker failures convincingly.
func TestStepTransitions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		state   leaseState
		kind    opKind
		strict  bool
		wantOK  bool
		wantNew leaseState
	}{
		// Produce creates the message, once.
		{"produce into absent", stateAbsent, kindProduce, false, true, statePending},
		{"produce into pending is a second produce of one id", statePending, kindProduce, false, false, statePending},
		{"produce into leased would be a delivery before its produce", stateLeased, kindProduce, false, false, stateLeased},
		{"produce into acked", stateAcked, kindProduce, false, false, stateAcked},

		// An ambiguous produce asserts only that the message may now exist.
		{"ambiguous produce into absent", stateAbsent, kindProduceAmbiguous, false, true, statePending},
		{"ambiguous produce into pending is idempotent", statePending, kindProduceAmbiguous, false, true, statePending},
		{"ambiguous produce into leased", stateLeased, kindProduceAmbiguous, false, false, stateLeased},

		// Delivery.
		{"deliver a pending message", statePending, kindDeliver, false, true, stateLeased},
		{"redeliver after the lease lapsed", stateLeased, kindDeliver, false, true, stateLeased},
		{"deliver a message never produced", stateAbsent, kindDeliver, false, false, stateAbsent},
		{"deliver after ack is at-least-once under the contract", stateAcked, kindDeliver, false, true, stateLeased},
		{"deliver after ack is a violation under strict", stateAcked, kindDeliver, true, false, stateAcked},

		// Only a 204 reaches the model, and only from a live lease.
		{"ack a leased message", stateLeased, kindAck, false, true, stateAcked},
		{"ack a message never delivered", statePending, kindAck, false, false, statePending},
		{"ack a message never produced", stateAbsent, kindAck, false, false, stateAbsent},
		{"ack an already acked message", stateAcked, kindAck, false, false, stateAcked},

		// Strict changes exactly one cell; everything else must match.
		{"strict still allows a pending delivery", statePending, kindDeliver, true, true, stateLeased},
		{"strict still allows redelivery of a live lease", stateLeased, kindDeliver, true, true, stateLeased},
		{"strict still allows an ack", stateLeased, kindAck, true, true, stateAcked},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ok, next := step(tc.state, tc.kind, tc.strict)
			if ok != tc.wantOK {
				t.Fatalf("step(%s, %s, strict=%v) ok = %v, want %v", tc.state, tc.kind, tc.strict, ok, tc.wantOK)
			}
			if got := next.(leaseState); got != tc.wantNew {
				t.Fatalf("step(%s, %s, strict=%v) state = %s, want %s", tc.state, tc.kind, tc.strict, got, tc.wantNew)
			}
		})
	}
}

// A rejected step must leave the state untouched: porcupine explores
// alternative orderings from it, and a step that mutated on failure
// would corrupt every branch explored afterwards.
func TestStepLeavesStateUnchangedOnRejection(t *testing.T) {
	t.Parallel()

	states := []leaseState{stateAbsent, statePending, stateLeased, stateAcked}
	kinds := []opKind{kindProduce, kindProduceAmbiguous, kindDeliver, kindAck}
	for _, strict := range []bool{false, true} {
		for _, state := range states {
			for _, kind := range kinds {
				ok, next := step(state, kind, strict)
				if !ok && next.(leaseState) != state {
					t.Fatalf("rejected step(%s, %s, strict=%v) changed state to %s", state, kind, strict, next.(leaseState))
				}
			}
		}
	}
}

func TestLeaseStateString(t *testing.T) {
	t.Parallel()

	for state, want := range map[leaseState]string{
		stateAbsent:   "absent",
		statePending:  "pending",
		stateLeased:   "leased",
		stateAcked:    "acked",
		leaseState(9): "state(9)",
	} {
		if got := state.String(); got != want {
			t.Errorf("leaseState(%d).String() = %q, want %q", int(state), got, want)
		}
	}
}

// opKind.String() feeds directly into describeIllegal's output, which a
// person reads while debugging a failing run, so its every case is worth
// pinning the same way leaseState's is above.
func TestOpKindString(t *testing.T) {
	t.Parallel()

	for kind, want := range map[opKind]string{
		kindProduce:          "produce",
		kindProduceAmbiguous: "produce?",
		kindDeliver:          "deliver",
		kindAck:              "ack",
		opKind(9):            "kind(9)",
	} {
		if got := kind.String(); got != want {
			t.Errorf("opKind(%d).String() = %q, want %q", int(kind), got, want)
		}
	}
}
