package store

import (
	"context"
	"testing"
)

func TestFinalizeRejectsUnknownFinalState(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.RegisterResource(ctx, "R1", VoteYes, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginTxn(ctx, "T1", []string{"R1"}, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeParticipant(ctx, "T1", "R1", "mystery", 3); err == nil {
		t.Fatal("unknown final state was accepted")
	}
	txn, parts, err := s.GetTxn(ctx, "T1")
	if err != nil || txn.State != StatePreparing || len(parts) != 1 || parts[0].Final != "" {
		t.Fatalf("invalid final state mutated txn: txn=%+v parts=%+v err=%v", txn, parts, err)
	}
	r, _, _ := s.GetResource(ctx, "R1")
	if r.AbortedCount != 0 || r.CommittedCount != 0 {
		t.Fatalf("invalid final state changed counters: %+v", r)
	}
}
