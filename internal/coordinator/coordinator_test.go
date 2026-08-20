package coordinator

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"task105-2pc/internal/store"
)

// newTestCoordinator opens a fresh store + coordinator backed by a temp file
// and a mock clock pinned to a fixed base time.
func newTestCoordinator(t *testing.T) (*Coordinator, *store.Store, *MockClock) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	clk := NewMockClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c, err := New(st, clk)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, st, clk
}

func mustRegister(t *testing.T, c *Coordinator, name, vote string) {
	t.Helper()
	if err := c.RegisterResource(context.Background(), name, vote); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
}

func TestRegisterResourceValidation(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	if err := c.RegisterResource(ctx, "", "yes"); err == nil {
		t.Fatal("empty name should fail")
	}
	if err := c.RegisterResource(ctx, "R1", "maybe"); err == nil {
		t.Fatal("bad vote should fail")
	}
	if err := c.RegisterResource(ctx, "R1", "yes"); err != nil {
		t.Fatalf("register R1: %v", err)
	}
	if err := c.RegisterResource(ctx, "R1", "no"); !errors.Is(err, store.ErrResourceExists) {
		t.Fatalf("duplicate should be ErrResourceExists, got %v", err)
	}
}

func TestBeginUnknownResource(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	err := c.Begin(context.Background(), "T1", []string{"NOPE"})
	if !errors.Is(err, store.ErrResourceMissing) {
		t.Fatalf("expected ErrResourceMissing, got %v", err)
	}
}

func TestPrepareCommitDecision(t *testing.T) {
	c, st, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	mustRegister(t, c, "R2", "yes")
	if err := c.Begin(ctx, "T1", []string{"R1", "R2"}); err != nil {
		t.Fatal(err)
	}
	pr, err := c.Prepare(ctx, "T1")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if pr.Decision != store.DecisionCommit || pr.State != store.StateCommitting {
		t.Fatalf("prepare result: %+v", pr)
	}
	if pr.Votes["R1"] != store.VoteYes || pr.Votes["R2"] != store.VoteYes {
		t.Fatalf("votes: %+v", pr.Votes)
	}
	// Finish -> COMMITTED, 2 ledger entries, counters incremented.
	state, _, err := c.Finish(ctx, "T1")
	if err != nil || state != store.StateCommitted {
		t.Fatalf("finish: state=%s err=%v", state, err)
	}
	if n, _ := c.LedgerCount(ctx, "T1"); n != 2 {
		t.Fatalf("ledger count=%d want 2", n)
	}
	r1, _, _ := c.GetResource(ctx, "R1")
	if r1.CommittedCount != 1 {
		t.Fatalf("R1 committed_count=%d want 1", r1.CommittedCount)
	}
	// decision row persisted
	d, ok, _ := st.GetDecision(ctx, "T1")
	if !ok || d.Decision != store.DecisionCommit {
		t.Fatalf("decision not persisted: %+v ok=%v", d, ok)
	}
}

func TestPrepareAbortDecision(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	mustRegister(t, c, "R2", "no") // forces abort
	c.Begin(ctx, "T1", []string{"R1", "R2"})
	pr, err := c.Prepare(ctx, "T1")
	if err != nil {
		t.Fatal(err)
	}
	if pr.Decision != store.DecisionAbort || pr.State != store.StateAborting {
		t.Fatalf("prepare result: %+v", pr)
	}
	state, parts, err := c.Finish(ctx, "T1")
	if err != nil || state != store.StateAborted {
		t.Fatalf("finish: state=%s err=%v", state, err)
	}
	// abort consistency: all participants aborted
	for _, p := range parts {
		if p.Final != store.FinalAborted {
			t.Fatalf("mixed final: %+v", p)
		}
	}
	if n, _ := c.LedgerCount(ctx, "T1"); n != 0 {
		t.Fatalf("abort ledger count=%d want 0", n)
	}
	r2, _, _ := c.GetResource(ctx, "R2")
	if r2.AbortedCount != 1 {
		t.Fatalf("R2 aborted_count=%d want 1", r2.AbortedCount)
	}
}

func TestFinishOnPreparingReturnsErrNoDecision(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	c.Begin(ctx, "T1", []string{"R1"})
	_, _, err := c.Finish(ctx, "T1")
	if !errors.Is(err, store.ErrNoDecision) {
		t.Fatalf("expected ErrNoDecision, got %v", err)
	}
}

func TestAbortOnlyInPreparing(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	mustRegister(t, c, "R2", "yes")
	c.Begin(ctx, "T1", []string{"R1", "R2"})
	// Abort a PREPARING txn -> ABORTED
	state, _, err := c.Abort(ctx, "T1")
	if err != nil || state != store.StateAborted {
		t.Fatalf("abort PREPARING: state=%s err=%v", state, err)
	}
	// Abort a COMMITTING txn -> ErrInvalidState (cannot overturn commit decision)
	c.Begin(ctx, "T2", []string{"R1", "R2"})
	c.Prepare(ctx, "T2") // -> COMMITTING
	_, _, err = c.Abort(ctx, "T2")
	if !errors.Is(err, store.ErrInvalidState) {
		t.Fatalf("abort COMMITTING should be ErrInvalidState, got %v", err)
	}
}

func TestRunCommitAndAbort(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	mustRegister(t, c, "R2", "yes")
	mustRegister(t, c, "R3", "no")
	// commit path
	c.Begin(ctx, "TC", []string{"R1", "R2"})
	r1, err := c.Run(ctx, "TC")
	if err != nil || r1.State != store.StateCommitted {
		t.Fatalf("run TC: %+v err=%v", r1, err)
	}
	// abort path
	c.Begin(ctx, "TA", []string{"R1", "R3"})
	r2, err := c.Run(ctx, "TA")
	if err != nil || r2.State != store.StateAborted {
		t.Fatalf("run TA: %+v err=%v", r2, err)
	}
}

func TestRecoverCommittingAfterRestart(t *testing.T) {
	c, st, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	mustRegister(t, c, "R2", "yes")
	c.Begin(ctx, "T1", []string{"R1", "R2"})
	c.Prepare(ctx, "T1") // COMMITTING, not finished
	// Simulate restart: new coordinator over the same DB.
	c2, err := New(st, NewMockClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	recs, err := c2.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(recs) != 1 || recs[0].TxnID != "T1" || recs[0].FromState != store.StateCommitting || recs[0].ToState != store.StateCommitted {
		t.Fatalf("recover record: %+v", recs)
	}
	if n, _ := c2.LedgerCount(ctx, "T1"); n != 2 {
		t.Fatalf("post-recover ledger=%d want 2", n)
	}
}

func TestRecoverPreparingMustAbort(t *testing.T) {
	c, st, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	mustRegister(t, c, "R2", "yes")
	c.Begin(ctx, "T1", []string{"R1", "R2"}) // PREPARING, never prepared
	c2, _ := New(st, NewMockClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
	recs, err := c2.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].ToState != store.StateAborted {
		t.Fatalf("PREPARING must recover to ABORTED: %+v", recs)
	}
	txn, _, _ := c2.GetTxn(ctx, "T1")
	if txn.State != store.StateAborted || txn.Decision != store.DecisionAbort {
		t.Fatalf("txn state after recover: %+v", txn)
	}
	if n, _ := c2.LedgerCount(ctx, "T1"); n != 0 {
		t.Fatalf("PREPARING recover ledger=%d want 0", n)
	}
}

func TestRecoverIdempotent(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	mustRegister(t, c, "R2", "yes")
	c.Begin(ctx, "T1", []string{"R1", "R2"})
	c.Prepare(ctx, "T1") // COMMITTING
	// First recover drives it to COMMITTED.
	if _, err := c.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	// Second recover finds nothing to do.
	recs, err := c.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("idempotent recover found work: %+v", recs)
	}
	// Counters not double-incremented.
	r1, _, _ := c.GetResource(ctx, "R1")
	if r1.CommittedCount != 1 {
		t.Fatalf("committed_count after re-recover=%d want 1", r1.CommittedCount)
	}
}

func TestRecoverPreviewReadOnly(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	c.Begin(ctx, "T1", []string{"R1"}) // PREPARING
	c.Begin(ctx, "T2", []string{"R1"})
	c.Prepare(ctx, "T2") // COMMITTING
	pv, err := c.RecoverPreview(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pv) != 2 {
		t.Fatalf("preview count=%d want 2", len(pv))
	}
	// Preview must not have changed state.
	txn2, _, _ := c.GetTxn(ctx, "T2")
	if txn2.State != store.StateCommitting {
		t.Fatalf("preview mutated state: %s", txn2.State)
	}
}

func TestDeleteResourceInUse(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	c.Begin(ctx, "T1", []string{"R1"}) // PREPARING references R1
	active, err := c.DeleteResource(ctx, "R1")
	if !errors.Is(err, store.ErrResourceInUse) {
		t.Fatalf("expected ErrResourceInUse, got %v", err)
	}
	if len(active) != 1 || active[0] != "T1" {
		t.Fatalf("active txns=%v want [T1]", active)
	}
}

func TestDeleteResourceAfterDrain(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	c.Begin(ctx, "T1", []string{"R1"})
	c.Run(ctx, "T1") // COMMITTED (terminal)
	if _, err := c.DeleteResource(ctx, "R1"); err != nil {
		t.Fatalf("delete after drain: %v", err)
	}
	if _, ok, _ := c.GetResource(ctx, "R1"); ok {
		t.Fatal("R1 should be gone")
	}
}

func TestUpdateResourceVote(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	if err := c.UpdateResourceVote(ctx, "R1", "no"); err != nil {
		t.Fatal(err)
	}
	// A transaction over R1 should now abort (R1 votes no).
	c.Begin(ctx, "T1", []string{"R1"})
	pr, err := c.Prepare(ctx, "T1")
	if err != nil {
		t.Fatal(err)
	}
	if pr.Decision != store.DecisionAbort {
		t.Fatalf("after vote update decision=%s want abort", pr.Decision)
	}
}

func TestDeleteTerminalTxn(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	c.Begin(ctx, "T1", []string{"R1"})
	c.Run(ctx, "T1") // COMMITTED
	if err := c.DeleteTxn(ctx, "T1"); err != nil {
		t.Fatalf("delete terminal: %v", err)
	}
	if _, _, err := c.GetTxn(ctx, "T1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	// Counters preserved (cumulative history).
	r1, _, _ := c.GetResource(ctx, "R1")
	if r1.CommittedCount != 1 {
		t.Fatalf("committed_count after delete=%d want 1", r1.CommittedCount)
	}
}

func TestDeleteNonTerminalTxn(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	c.Begin(ctx, "T1", []string{"R1"})
	if err := c.DeleteTxn(ctx, "T1"); !errors.Is(err, store.ErrNotTerminal) {
		t.Fatalf("expected ErrNotTerminal, got %v", err)
	}
}

func TestStats(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "yes")
	mustRegister(t, c, "R2", "yes")
	c.Begin(ctx, "T1", []string{"R1", "R2"})
	c.Run(ctx, "T1") // COMMITTED
	c.Begin(ctx, "T2", []string{"R1"})
	c.Prepare(ctx, "T2") // COMMITTING
	st, err := c.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.TxnCount != 2 || st.ByState[store.StateCommitted] != 1 || st.ByState[store.StateCommitting] != 1 {
		t.Fatalf("stats: %+v", st)
	}
	if st.ResourceCount != 2 || st.LedgerCount != 2 || st.DecisionCount != 2 {
		t.Fatalf("stats counts: %+v", st)
	}
}
