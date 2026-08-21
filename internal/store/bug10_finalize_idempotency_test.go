package store

import (
	"context"
	"testing"
)

func TestFinalizeParticipantIsIdempotentAcrossDirectRetries(t *testing.T) {
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
	if err := s.FinalizeParticipant(ctx, "T1", "R1", FinalCommitted, 4); err != nil {
		t.Fatal(err)
	}
	if err := s.SetTxnFinalState(ctx, "T1", StateCommitted, 5); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeParticipant(ctx, "T1", "R1", FinalCommitted, 6); err != nil {
		t.Fatal(err)
	}
	r, _, _ := s.GetResource(ctx, "R1")
	n, err := s.LedgerCount(ctx, "T1")
	if err != nil || n != 1 || r.CommittedCount != 1 {
		t.Fatalf("retry duplicated effect: ledger=%d resource=%+v err=%v", n, r, err)
	}
}

// TestFinalizeParticipantTerminalAbortRetryIdempotent guards the same
// invariant as the commit case above, but for an abort decision: once the
// transaction is ABORTED, re-running a participant's finalize must not
// double-increment aborted_count.
func TestFinalizeParticipantTerminalAbortRetryIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.RegisterResource(ctx, "R1", VoteNo, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginTxn(ctx, "T1", []string{"R1"}, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordPrepare(ctx, "T1", map[string]string{"R1": VoteNo}, DecisionAbort, 3); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeParticipant(ctx, "T1", "R1", FinalAborted, 4); err != nil {
		t.Fatal(err)
	}
	if err := s.SetTxnFinalState(ctx, "T1", StateAborted, 5); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeParticipant(ctx, "T1", "R1", FinalAborted, 6); err != nil {
		t.Fatal(err)
	}
	r, _, _ := s.GetResource(ctx, "R1")
	if n, _ := s.LedgerCount(ctx, "T1"); n != 0 {
		t.Fatalf("abort wrote ledger rows=%d want 0", n)
	}
	if r.AbortedCount != 1 {
		t.Fatalf("retry duplicated effect: aborted_count=%d want 1, resource=%+v", r.AbortedCount, r)
	}
}
