package store

import (
	"context"
	"testing"
)

func TestSetTxnFinalStateRejectsNonTerminalTransition(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.RegisterResource(ctx, "R1", VoteYes, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginTxn(ctx, "T1", []string{"R1"}, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordPrepare(ctx, "T1", map[string]string{"R1": VoteYes}, DecisionCommit, 3); err != nil {
		t.Fatal(err)
	}
	if err := s.SetTxnFinalState(ctx, "T1", StatePreparing, 4); err == nil {
		t.Fatal("non-terminal final state was accepted")
	}
	txn, _, err := s.GetTxn(ctx, "T1")
	if err != nil || txn.State != StateCommitting {
		t.Fatalf("invalid transition changed state: txn=%+v err=%v", txn, err)
	}
}
