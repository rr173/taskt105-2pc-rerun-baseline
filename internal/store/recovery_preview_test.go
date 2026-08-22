package store

import (
	"context"
	"testing"
)

func TestRecoverPreviewEmptyStore(t *testing.T) {
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rows, err := s.ListRecoverPreview(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows=%v", rows)
	}
}

// TestRecoverPreviewMapsRecoveryStrategy asserts the preview mirrors the
// actual Recover strategy and never rewrites a PREPARING txn (no decision
// recorded yet) as a commit: it must target ABORTED, while COMMITTING targets
// COMMITTED and ABORTING targets ABORTED. The preview must also be read-only.
func TestRecoverPreviewMapsRecoveryStrategy(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.RegisterResource(ctx, "R1", VoteYes, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterResource(ctx, "R2", VoteYes, 2); err != nil {
		t.Fatal(err)
	}

	// Tprep: created but never prepared -> stays PREPARING (no decision).
	if err := s.BeginTxn(ctx, "Tprep", []string{"R1", "R2"}, 10); err != nil {
		t.Fatalf("begin Tprep: %v", err)
	}
	// Tcom: prepared with all-yes -> COMMITTING.
	if err := s.BeginTxn(ctx, "Tcom", []string{"R1", "R2"}, 11); err != nil {
		t.Fatalf("begin Tcom: %v", err)
	}
	if err := s.RecordPrepare(ctx, "Tcom",
		map[string]string{"R1": VoteYes, "R2": VoteYes}, DecisionCommit, 12); err != nil {
		t.Fatalf("prepare Tcom: %v", err)
	}
	// Tabt: prepared with a no vote -> ABORTING.
	if err := s.RegisterResource(ctx, "R3", VoteNo, 3); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginTxn(ctx, "Tabt", []string{"R1", "R3"}, 13); err != nil {
		t.Fatalf("begin Tabt: %v", err)
	}
	if err := s.RecordPrepare(ctx, "Tabt",
		map[string]string{"R1": VoteYes, "R3": VoteNo}, DecisionAbort, 14); err != nil {
		t.Fatalf("prepare Tabt: %v", err)
	}

	rows, err := s.ListRecoverPreview(ctx)
	if err != nil {
		t.Fatalf("ListRecoverPreview: %v", err)
	}
	want := map[string]string{
		"Tprep": StateAborted,
		"Tcom":  StateCommitted,
		"Tabt":  StateAborted,
	}
	if len(rows) != len(want) {
		t.Fatalf("row count=%d want %d (rows=%v)", len(rows), len(want), rows)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r.TxnID] = r.TargetState
		if r.TargetState == "" {
			t.Fatalf("empty target for %s", r.TxnID)
		}
	}
	for id, target := range want {
		if got[id] != target {
			t.Errorf("preview %s: current=%s target=%q want %q (preview must reflect recovery, not change it)",
				id, rState(rows, id), got[id], target)
		}
	}

	// Preview is read-only: the txns' real states must be untouched.
	for _, id := range []string{"Tprep", "Tcom", "Tabt"} {
		tRow, _, err := s.GetTxn(ctx, id)
		if err != nil {
			t.Fatalf("GetTxn %s: %v", id, err)
		}
		switch id {
		case "Tprep":
			if tRow.State != StatePreparing {
				t.Errorf("preview mutated %s state to %s; must stay %s", id, tRow.State, StatePreparing)
			}
		case "Tcom":
			if tRow.State != StateCommitting {
				t.Errorf("preview mutated %s state to %s; must stay %s", id, tRow.State, StateCommitting)
			}
		case "Tabt":
			if tRow.State != StateAborting {
				t.Errorf("preview mutated %s state to %s; must stay %s", id, tRow.State, StateAborting)
			}
		}
	}
}

// rState returns the CurrentState recorded for txnID in rows, for diagnostics.
func rState(rows []RecoverPreviewRow, txnID string) string {
	for _, r := range rows {
		if r.TxnID == txnID {
			return r.CurrentState
		}
	}
	return "?"
}
