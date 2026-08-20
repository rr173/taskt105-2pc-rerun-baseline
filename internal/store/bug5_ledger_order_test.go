package store

import (
	"context"
	"testing"
)

func TestLedgerForResourceUsesEffectTimeOrder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.RegisterResource(ctx, "R1", VoteYes, 1); err != nil {
		t.Fatal(err)
	}
	for _, txn := range []string{"z-late-name", "a-early-name"} {
		if err := s.BeginTxn(ctx, txn, []string{"R1"}, 2); err != nil {
			t.Fatal(err)
		}
		if err := s.RecordPrepare(ctx, txn, map[string]string{"R1": VoteYes}, DecisionCommit, 3); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.FinalizeParticipant(ctx, "z-late-name", "R1", FinalCommitted, 10); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeParticipant(ctx, "a-early-name", "R1", FinalCommitted, 20); err != nil {
		t.Fatal(err)
	}
	rows, ok, err := s.ListLedgerForResource(ctx, "R1")
	if err != nil || !ok || len(rows) != 2 || rows[0].TxnID != "z-late-name" || rows[1].TxnID != "a-early-name" {
		t.Fatalf("ledger order=%+v ok=%v err=%v", rows, ok, err)
	}
}
