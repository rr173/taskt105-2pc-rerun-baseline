package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

// newTestStore opens a store backed by a fresh temp file.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestOpenMigrate(t *testing.T) {
	s := newTestStore(t)
	for _, tbl := range []string{"resources", "transactions", "participants", "decisions", "commit_ledger"} {
		var name string
		if err := s.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, tbl).Scan(&name); err != nil {
			t.Fatalf("table %s missing: %v", tbl, err)
		}
	}
}

func TestRegisterResourceDuplicate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.RegisterResource(ctx, "R1", VoteYes, 1); err != nil {
		t.Fatalf("register R1: %v", err)
	}
	err := s.RegisterResource(ctx, "R1", VoteNo, 2)
	if !errors.Is(err, ErrResourceExists) {
		t.Fatalf("expected ErrResourceExists, got %v", err)
	}
	// The duplicate must not have overwritten the original vote.
	r, ok, err := s.GetResource(ctx, "R1")
	if err != nil || !ok {
		t.Fatalf("GetResource: ok=%v err=%v", ok, err)
	}
	if r.Vote != VoteYes {
		t.Fatalf("vote overwritten by failed duplicate: %s", r.Vote)
	}
}

func TestBeginTxnUnknownResource(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.RegisterResource(ctx, "R1", VoteYes, 1); err != nil {
		t.Fatal(err)
	}
	err := s.BeginTxn(ctx, "T1", []string{"R1", "NOPE"}, 2)
	if !errors.Is(err, ErrResourceMissing) {
		t.Fatalf("expected ErrResourceMissing, got %v", err)
	}
	// The txn must not have been created.
	if _, _, err := s.GetTxn(ctx, "T1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for rolled-back txn, got %v", err)
	}
}

func TestBeginTxnDuplicate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	s.RegisterResource(ctx, "R1", VoteYes, 1)
	if err := s.BeginTxn(ctx, "T1", []string{"R1"}, 2); err != nil {
		t.Fatal(err)
	}
	err := s.BeginTxn(ctx, "T1", []string{"R1"}, 3)
	if !errors.Is(err, ErrTxnExists) {
		t.Fatalf("expected ErrTxnExists, got %v", err)
	}
}

func TestRecordPrepareStateGuard(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	s.RegisterResource(ctx, "R1", VoteYes, 1)
	s.BeginTxn(ctx, "T1", []string{"R1"}, 2)
	if err := s.RecordPrepare(ctx, "T1", map[string]string{"R1": VoteYes}, DecisionCommit, 3); err != nil {
		t.Fatalf("RecordPrepare: %v", err)
	}
	// Second prepare must refuse: not PREPARING.
	err := s.RecordPrepare(ctx, "T1", map[string]string{"R1": VoteYes}, DecisionCommit, 4)
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("expected ErrInvalidState, got %v", err)
	}
}

func TestRecordPrepareAtomicDecisionAndState(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	s.RegisterResource(ctx, "R1", VoteYes, 1)
	s.RegisterResource(ctx, "R2", VoteNo, 1)
	s.BeginTxn(ctx, "T1", []string{"R1", "R2"}, 2)
	if err := s.RecordPrepare(ctx, "T1",
		map[string]string{"R1": VoteYes, "R2": VoteNo}, DecisionAbort, 3); err != nil {
		t.Fatalf("RecordPrepare: %v", err)
	}
	txn, parts, err := s.GetTxn(ctx, "T1")
	if err != nil {
		t.Fatal(err)
	}
	if txn.State != StateAborting || txn.Decision != DecisionAbort {
		t.Fatalf("state/decision wrong: %s/%s", txn.State, txn.Decision)
	}
	// votes recorded, finals still empty
	byRes := map[string]ParticipantRow{}
	for _, p := range parts {
		byRes[p.Resource] = p
	}
	if byRes["R1"].Vote != VoteYes || byRes["R2"].Vote != VoteNo {
		t.Fatalf("votes wrong: %v", byRes)
	}
	if byRes["R1"].Final != "" || byRes["R2"].Final != "" {
		t.Fatalf("finals should be empty before finish: %v", byRes)
	}
	// decision row present
	if n := countRows(t, s, "decisions"); n != 1 {
		t.Fatalf("decisions rows=%d want 1", n)
	}
}

func TestFinalizeParticipantIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	s.RegisterResource(ctx, "R1", VoteYes, 1)
	s.BeginTxn(ctx, "T1", []string{"R1"}, 2)
	s.RecordPrepare(ctx, "T1", map[string]string{"R1": VoteYes}, DecisionCommit, 3)
	if err := s.FinalizeParticipant(ctx, "T1", "R1", FinalCommitted, 4); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	// Re-running must not duplicate the ledger row nor double-increment the
	// committed counter.
	if err := s.FinalizeParticipant(ctx, "T1", "R1", FinalCommitted, 5); err != nil {
		t.Fatalf("re-finalize: %v", err)
	}
	if n, _ := s.LedgerCount(ctx, "T1"); n != 1 {
		t.Fatalf("ledger count after re-finalize=%d want 1", n)
	}
	r, _, _ := s.GetResource(ctx, "R1")
	if r.CommittedCount != 1 {
		t.Fatalf("committed_count after re-finalize=%d want 1", r.CommittedCount)
	}
	has, _ := s.LedgerHas(ctx, "R1", "T1")
	if !has {
		t.Fatal("LedgerHas should be true")
	}
}

func TestFinalizeAbortNoLedger(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	s.RegisterResource(ctx, "R1", VoteYes, 1)
	s.BeginTxn(ctx, "T1", []string{"R1"}, 2)
	s.RecordPrepare(ctx, "T1", map[string]string{"R1": VoteNo}, DecisionAbort, 3)
	if err := s.FinalizeParticipant(ctx, "T1", "R1", FinalAborted, 4); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if n, _ := s.LedgerCount(ctx, "T1"); n != 0 {
		t.Fatalf("abort wrote ledger rows=%d want 0", n)
	}
}

func TestListNonFinalTxns(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	s.RegisterResource(ctx, "R1", VoteYes, 1)
	// T1 PREPARING
	s.BeginTxn(ctx, "T1", []string{"R1"}, 2)
	// T2 COMMITTING
	s.BeginTxn(ctx, "T2", []string{"R1"}, 3)
	s.RecordPrepare(ctx, "T2", map[string]string{"R1": VoteYes}, DecisionCommit, 4)
	// T3 COMMITTED (terminal)
	s.BeginTxn(ctx, "T3", []string{"R1"}, 5)
	s.RecordPrepare(ctx, "T3", map[string]string{"R1": VoteYes}, DecisionCommit, 6)
	s.FinalizeParticipant(ctx, "T3", "R1", FinalCommitted, 7)
	s.SetTxnFinalState(ctx, "T3", StateCommitted, 7)

	nonFinal, err := s.ListNonFinalTxns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nonFinal) != 2 {
		t.Fatalf("non-final count=%d want 2", len(nonFinal))
	}
}

func TestListTxnsOrderedByStartedAtThenTxnID(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	s.RegisterResource(ctx, "R1", VoteYes, 1)
	// Same started_at; txn_id breaks ties -> order Z, A would sort A, Z.
	s.BeginTxn(ctx, "ZT", []string{"R1"}, 100)
	s.BeginTxn(ctx, "AT", []string{"R1"}, 100)
	txns, err := s.ListTxns(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) != 2 || txns[0].TxnID != "AT" || txns[1].TxnID != "ZT" {
		t.Fatalf("order wrong: %v", txns)
	}
}

func countRows(t *testing.T, s *Store, table string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}
