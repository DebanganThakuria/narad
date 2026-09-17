package main

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// The sequential specification a Narad message is checked against.
//
// The unit is not a message but a (message, path) pair: a message
// produced to a topic with fan-out children is a separate delivery
// obligation on the parent and on every child, and they succeed or fail
// independently. Partitioning on that pair is also what makes the check
// cheap, because each partition holds a handful of operations rather
// than the whole run.
//
// What the model deliberately does not know is time. Porcupine models
// are untimed, so "the lease expired" is not something the model can
// observe; a redelivery is therefore always legal from the leased state.
// The consequence is stated plainly because it bounds the whole check:
// two deliveries of one message that overlap in real time are a genuine
// double-lease, and this model accepts them. Catching those needs the
// lease clock, which the driver counts separately as dupBeforeAck.

// leaseState is the per-partition state. Porcupine compares states with
// == by default, which an int satisfies.
type leaseState int

const (
	// stateAbsent means the broker is not known to hold the message.
	stateAbsent leaseState = iota
	// statePending means the broker holds it and no lease is out.
	statePending
	// stateLeased means it has been handed to a consumer.
	stateLeased
	// stateAcked means a consumer acked it and got a 204.
	stateAcked
)

func (s leaseState) String() string {
	switch s {
	case stateAbsent:
		return "absent"
	case statePending:
		return "pending"
	case stateLeased:
		return "leased"
	case stateAcked:
		return "acked"
	default:
		return fmt.Sprintf("state(%d)", int(s))
	}
}

// opKind is the operation as the model sees it. It is narrower than the
// wire history: operations whose effect the client cannot determine are
// dropped before they reach the model rather than guessed at.
type opKind int

const (
	// kindProduce is a produce the broker accepted with a 202.
	kindProduce opKind = iota
	// kindProduceAmbiguous is a produce whose request errored. The broker
	// may or may not hold the message, so the operation moves the
	// partition to pending without asserting that it had to.
	kindProduceAmbiguous
	// kindDeliver is a consume that returned this message.
	kindDeliver
	// kindAck is an ack the broker answered with 204.
	kindAck
)

func (k opKind) String() string {
	switch k {
	case kindProduce:
		return "produce"
	case kindProduceAmbiguous:
		return "produce?"
	case kindDeliver:
		return "deliver"
	case kindAck:
		return "ack"
	default:
		return fmt.Sprintf("kind(%d)", int(k))
	}
}

// partitionKey identifies the (message, path) pair a partition covers.
type partitionKey struct {
	Msg  string
	Path string
}

func (k partitionKey) String() string { return k.Msg + " on " + k.Path }

// modelInput is the porcupine input for one operation.
type modelInput struct {
	Key  partitionKey
	Kind opKind
}

// newModel builds the porcupine model. In strict mode a delivery after a
// successful ack is illegal, which is exactly-once-after-ack. In
// contract mode it is legal, because Narad promises at-least-once and
// documents that a broker restart can redeliver acks that were still in
// the batch being persisted. Contract mode is the default; the anomaly
// analysis reports those redeliveries and insists each one sits inside a
// fault window, which is the useful question once the contract itself
// allows them.
func newModel(strict bool) porcupine.Model {
	return porcupine.Model{
		Partition: partitionOperations,
		Init:      func() any { return stateAbsent },
		Step: func(state, input, _ any) (bool, any) {
			return step(state.(leaseState), input.(modelInput).Kind, strict)
		},
		DescribeOperation: func(input, _ any) string {
			in := input.(modelInput)
			return fmt.Sprintf("%s(%s)", in.Kind, in.Key.Msg)
		},
		DescribeState: func(state any) string {
			return state.(leaseState).String()
		},
	}
}

// step is the transition function, kept separate from the porcupine
// plumbing so it can be tested directly.
func step(state leaseState, kind opKind, strict bool) (bool, any) {
	switch kind {
	case kindProduce:
		// A message is produced once. Accepting a produce onto anything
		// but an absent partition would let a history where a message was
		// delivered before it was produced linearize.
		if state == stateAbsent {
			return true, statePending
		}
		return false, state

	case kindProduceAmbiguous:
		// "It may now exist." Legal from absent, and idempotent from
		// pending so a retry of an ambiguous produce does not fail the
		// history on its own.
		if state == stateAbsent || state == statePending {
			return true, statePending
		}
		return false, state

	case kindDeliver:
		switch state {
		case statePending:
			return true, stateLeased
		case stateLeased:
			// The lease lapsed and it came back. The model cannot see the
			// clock, so this is always allowed.
			return true, stateLeased
		case stateAcked:
			if strict {
				return false, state
			}
			// At-least-once: the message is leased again and will be acked
			// again. Counted by the anomaly analysis, not failed here.
			return true, stateLeased
		default:
			// Delivered a message the broker was never told to hold, or
			// told and refused.
			return false, state
		}

	case kindAck:
		// Only a 204 reaches the model, and a 204 proves the lease was
		// live. The client never acks a handle it was not given, and it
		// sends the ack strictly after the delivery returned, so a
		// linearization that puts the ack first cannot arise from a real
		// run.
		if state == stateLeased {
			return true, stateAcked
		}
		return false, state

	default:
		return false, state
	}
}

// partitionOperations groups a history by (message, path). Porcupine
// checks each group independently, which is sound here because no
// operation in one group constrains another.
func partitionOperations(history []porcupine.Operation) [][]porcupine.Operation {
	groups := make(map[partitionKey][]porcupine.Operation)
	// Preserve a stable group order so a failure reports the same
	// partition on every run over the same history.
	var order []partitionKey
	for _, op := range history {
		key := op.Input.(modelInput).Key
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}
		groups[key] = append(groups[key], op)
	}
	partitions := make([][]porcupine.Operation, 0, len(order))
	for _, key := range order {
		partitions = append(partitions, groups[key])
	}
	return partitions
}
