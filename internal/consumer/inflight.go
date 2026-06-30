// Package consumer tracks in-flight offset reservations and the receipt
// handles that back Narad's at-least-once delivery.
//
// A Consume reserves the next deliverable offset on a partition, marking
// it invisible for a visibility timeout and handing the caller a receipt
// handle. Ack commits the offset by echoing that handle; the nonce in the
// handle is verified against the live reservation so a stale or forged
// ack is rejected. Reservations that are never acked expire and become
// deliverable again. The committed-offset frontier is persisted via a
// CommitFunc so delivery state survives a restart.
package consumer
