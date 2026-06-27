package main

import (
	"context"
	"fmt"
	"time"
)

func verifyAcceptedProduceVisibility(ctx context.Context, lb *roundRobinClient, cfg config, topicName string) error {
	job := messageJob{
		Topic: topicName,
		Key:   "recovery-probe",
		Body: messageRecord{
			ID:       cfg.runID + "/" + topicName + "/recovery-probe",
			Topic:    topicName,
			Sequence: 0,
			Key:      "recovery-probe",
			RunID:    cfg.runID,
		},
	}

	if err := produceOne(ctx, lb, job); err != nil {
		return fmt.Errorf("produce recovery probe: %w", err)
	}
	return retry(ctx, 20, 100*time.Millisecond, func() error {
		msg, found, err := consumeOne(ctx, lb, topicName)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("accepted produce is not visible yet")
		}
		if msg.Payload != job.Body {
			return fmt.Errorf("visible probe payload = %+v, want %+v", msg.Payload, job.Body)
		}
		if msg.ReceiptHandle == "" {
			return fmt.Errorf("visible probe missing receipt handle")
		}
		return ackOne(ctx, lb, topicName, msg.ReceiptHandle)
	})
}
