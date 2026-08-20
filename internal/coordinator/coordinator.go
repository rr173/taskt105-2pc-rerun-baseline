// Package coordinator implements the two-phase-commit (2PC) state machine
// that drives transactions across registered resource participants.
//
// Phase 1 (Prepare): ask each participant whether it can commit; collect
// votes. If every participant votes yes, the decision is "commit"; otherwise
// the decision is "abort". The decision and the new state (COMMITTING or
// ABORTING) are persisted *before* any participant is driven into phase 2 —
// this is the 2PC durability invariant that makes crash recovery possible.
//
// Phase 2 (Finish/Recover): drive each participant to Commit (for a commit
// decision) or Abort (for an abort decision) and record each participant's
// final state. Committing appends an idempotent row to the commit ledger and
// increments the resource's committed_count; aborting increments
// aborted_count. Both effects are idempotent (the participant final column and
// the ledger PK guard against duplicates), so recovery is safe to re-run.
//
// Recover: on startup (or on demand) scan every transaction that is not yet
// terminal and drive it to completion. A PREPARING txn (no decision recorded)
// is aborted — never committed — because no participant was ever told commit,
// which makes abort the always-safe choice.
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"task105-2pc/internal/store"
)

// Clock abstracts time so tests can drive deterministic recovery scenarios
// without sleeping. Production uses RealClock.
type Clock interface {
	Now() int64 // unix nanoseconds
}

// RealClock returns the real wall-clock time as unix nanoseconds.
type RealClock struct{}

func (RealClock) Now() int64 { return realNow() }

// Resource is a participant in a 2PC transaction. Implementations are
// in-process; their committed/aborted effects are observed via the
// per-resource committed_count / aborted_count counters and the commit
// ledger, persisted by the store in FinalizeParticipant.
type Resource interface {
	Name() string
	// Prepare returns the participant's vote ("yes"/"no"). A non-nil error is
	// treated as a "no" vote.
	Prepare(ctx context.Context) (string, error)
	// Commit finalizes the participant's prepared work. Must be idempotent:
	// recovery may call it again on a participant already committed.
	Commit(ctx context.Context) error
	// Abort rolls back the participant's prepared work. Must be idempotent:
	// recovery may call it again on a participant already aborted.
	Abort(ctx context.Context) error
}

// Coordinator drives the 2PC lifecycle. It is safe for concurrent use: the
// store serialises via a single SQLite connection, and the mutex prevents
// two goroutines from concurrently mutating the same coordinator state.
type Coordinator struct {
	store *store.Store
	clock Clock
	mu    sync.Mutex
	// resources holds the in-memory participant handles, keyed by name. They
	// are loaded from the store on startup so recovery can re-drive them.
	resources map[string]Resource
}

// New creates a Coordinator backed by st. Resources are loaded from the store
// so that recovery can re-drive participants that existed before a restart.
func New(st *store.Store, clock Clock) (*Coordinator, error) {
	c := &Coordinator{store: st, clock: clock, resources: map[string]Resource{}}
	if err := c.loadResources(context.Background()); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Coordinator) loadResources(ctx context.Context) error {
	rows, err := c.store.ListResources(ctx)
	if err != nil {
		return fmt.Errorf("load resources: %w", err)
	}
	for _, r := range rows {
		c.resources[r.Name] = NewInMemoryResource(r.Name, r.Vote)
	}
	return nil
}

// ---- resource management ----------------------------------------------------

// RegisterResource persists and registers a new participant.
func (c *Coordinator) RegisterResource(ctx context.Context, name, vote string) error {
	if name == "" {
		return errors.New("resource name is empty")
	}
	if vote != store.VoteYes && vote != store.VoteNo {
		return errors.New("vote must be \"yes\" or \"no\"")
	}
	if err := c.store.RegisterResource(ctx, name, vote, c.clock.Now()); err != nil {
		return err
	}
	c.mu.Lock()
	c.resources[name] = NewInMemoryResource(name, vote)
	c.mu.Unlock()
	return nil
}

// GetResource returns the registered resource row.
func (c *Coordinator) GetResource(ctx context.Context, name string) (store.ResourceRow, bool, error) {
	return c.store.GetResource(ctx, name)
}

// ListResources returns all registered resources.
func (c *Coordinator) ListResources(ctx context.Context) ([]store.ResourceRow, error) {
	return c.store.ListResources(ctx)
}

// UpdateResourceVote reconfigures a resource's prepare vote.
func (c *Coordinator) UpdateResourceVote(ctx context.Context, name, vote string) error {
	if vote != store.VoteYes && vote != store.VoteNo {
		return errors.New("vote must be \"yes\" or \"no\"")
	}
	if err := c.store.UpdateResourceVote(ctx, name, vote); err != nil {
		return err
	}
	c.mu.Lock()
	if ir, ok := c.resources[name].(*InMemoryResource); ok {
		// Keep the old live vote when a resource is changed from no to yes.
		if ir.vote == store.VoteYes && vote == store.VoteNo {
			ir.SetVote(vote)
		}
	} else {
		c.resources[name] = NewInMemoryResource(name, vote)
	}
	c.mu.Unlock()
	return nil
}

// DeleteResource unregisters a resource, refusing if any non-terminal txn
// still references it. Returns the active txn ids with ErrResourceInUse.
func (c *Coordinator) DeleteResource(ctx context.Context, name string) ([]string, error) {
	if name == "" {
		return nil, errors.New("resource name is empty")
	}
	active, err := c.store.DeleteResource(ctx, name)
	if err != nil {
		return active, err
	}
	c.mu.Lock()
	delete(c.resources, name)
	c.mu.Unlock()
	return active, nil
}

// ---- transaction management -------------------------------------------------

// Begin creates a transaction in PREPARING state.
func (c *Coordinator) Begin(ctx context.Context, txnID string, resources []string) error {
	if txnID == "" {
		return errors.New("txn_id is empty")
	}
	if len(resources) == 0 {
		return errors.New("resources is empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range resources {
		if r == "" {
			return errors.New("resource name is empty")
		}
		if _, ok := c.resources[r]; !ok {
			return store.ErrResourceMissing
		}
	}
	return c.store.BeginTxn(ctx, txnID, resources, c.clock.Now())
}

// PrepareResult is the phase-1 outcome returned to the caller.
type PrepareResult struct {
	TxnID    string
	Decision string
	State    string
	Votes    map[string]string // resource -> yes/no
}

// Prepare runs phase 1: ask each participant, record votes + decision +
// state atomically. It does NOT drive phase 2; call Finish for that.
func (c *Coordinator) Prepare(ctx context.Context, txnID string) (*PrepareResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, _, err := c.store.GetTxn(ctx, txnID)
	if err != nil {
		return nil, err
	}
	if t.State != store.StatePreparing {
		return nil, store.ErrInvalidState
	}

	votes := make(map[string]string, len(t.Resources))
	decision := store.DecisionCommit
	for _, r := range t.Resources {
		res, ok := c.resources[r]
		if !ok {
			votes[r] = store.VoteNo
			decision = store.DecisionAbort
			continue
		}
		v, err := res.Prepare(ctx)
		if err != nil || v != store.VoteYes {
			votes[r] = store.VoteNo
			decision = store.DecisionAbort
		} else {
			votes[r] = store.VoteYes
		}
	}

	if err := c.store.RecordPrepare(ctx, txnID, votes, decision, c.clock.Now()); err != nil {
		return nil, err
	}
	state := store.StateCommitting
	if decision == store.DecisionAbort {
		state = store.StateAborting
	}
	return &PrepareResult{TxnID: txnID, Decision: decision, State: state, Votes: votes}, nil
}

// Finish runs phase 2: drive each participant to the recorded decision and
// record the final state + observable effect. Returns ErrInvalidState if the
// transaction is already terminal, or ErrNoDecision if still PREPARING.
func (c *Coordinator) Finish(ctx context.Context, txnID string) (string, []store.ParticipantRow, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, parts, err := c.store.GetTxn(ctx, txnID)
	if err != nil {
		return "", nil, err
	}
	if t.State == store.StatePreparing {
		return "", nil, store.ErrNoDecision
	}
	if t.State == store.StateCommitted || t.State == store.StateAborted {
		return "", nil, store.ErrInvalidState
	}
	finalState, parts, err := c.driveSecondPhase(ctx, t, parts)
	if err != nil {
		return "", nil, err
	}
	return finalState, parts, nil
}

// Abort explicitly aborts a transaction. It is only allowed in PREPARING
// state (before any commit decision is recorded); calling it on a COMMITTING
// txn returns ErrInvalidState because the commit decision may already have
// reached participants and cannot be safely overturned.
func (c *Coordinator) Abort(ctx context.Context, txnID string) (string, []store.ParticipantRow, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, parts, err := c.store.GetTxn(ctx, txnID)
	if err != nil {
		return "", nil, err
	}
	if t.State != store.StatePreparing {
		return "", nil, store.ErrInvalidState
	}
	// Record an abort decision and drive phase 2 to ABORTED.
	if err := c.store.RecordPrepare(ctx, txnID, nil, store.DecisionAbort, c.clock.Now()); err != nil {
		return "", nil, err
	}
	t.State = store.StateAborting
	finalState, parts, err := c.driveSecondPhase(ctx, t, parts)
	if err != nil {
		return "", nil, err
	}
	return finalState, parts, nil
}

// Run is the convenience entry point that performs Prepare then Finish in one
// call, returning the final state.
type RunResult struct {
	TxnID        string
	Decision     string
	State        string
	Participants []store.ParticipantRow
}

func (c *Coordinator) Run(ctx context.Context, txnID string) (*RunResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, _, err := c.store.GetTxn(ctx, txnID)
	if err != nil {
		return nil, err
	}
	if t.State != store.StatePreparing {
		return nil, store.ErrInvalidState
	}
	pr, err := c.prepareLocked(ctx, t)
	if err != nil {
		return nil, err
	}
	t.State = pr.State
	parts, err := c.store.ListParticipants(ctx, txnID)
	if err != nil {
		return nil, err
	}
	finalState, parts, err := c.driveSecondPhase(ctx, t, parts)
	if err != nil {
		return nil, err
	}
	return &RunResult{TxnID: txnID, Decision: pr.Decision, State: finalState, Participants: parts}, nil
}

// prepareLocked is the inner phase-1 used by both Prepare (which holds the
// mutex) and Run (which also holds it). It assumes c.mu is held.
func (c *Coordinator) prepareLocked(ctx context.Context, t store.TxnRow) (*PrepareResult, error) {
	votes := make(map[string]string, len(t.Resources))
	decision := store.DecisionCommit
	for _, r := range t.Resources {
		res, ok := c.resources[r]
		if !ok {
			votes[r] = store.VoteNo
			decision = store.DecisionAbort
			continue
		}
		v, err := res.Prepare(ctx)
		if err != nil || v != store.VoteYes {
			votes[r] = store.VoteNo
			decision = store.DecisionAbort
		} else {
			votes[r] = store.VoteYes
		}
	}
	if err := c.store.RecordPrepare(ctx, t.TxnID, votes, decision, c.clock.Now()); err != nil {
		return nil, err
	}
	state := store.StateCommitting
	if decision == store.DecisionAbort {
		state = store.StateAborting
	}
	return &PrepareResult{TxnID: t.TxnID, Decision: decision, State: state, Votes: votes}, nil
}

// Recover scans every non-terminal transaction and drives it to completion.
// PREPARING txns (no decision) are aborted; COMMITTING txns are committed;
// ABORTING txns are aborted. Idempotent.
func (c *Coordinator) Recover(ctx context.Context) ([]RecoveryRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	txns, err := c.store.ListNonFinalTxns(ctx)
	if err != nil {
		return nil, err
	}
	var out []RecoveryRecord
	for _, t := range txns {
		from := t.State
		if t.State == store.StatePreparing {
			if err := c.store.RecordPrepare(ctx, t.TxnID, nil, store.DecisionAbort, c.clock.Now()); err != nil {
				return out, err
			}
			t.State = store.StateAborting
		}
		parts, err := c.store.ListParticipants(ctx, t.TxnID)
		if err != nil {
			return out, err
		}
		toState, _, err := c.driveSecondPhase(ctx, t, parts)
		if err != nil {
			return out, err
		}
		out = append(out, RecoveryRecord{TxnID: t.TxnID, FromState: from, ToState: toState})
	}
	return out, nil
}

// RecoverPreview returns the non-terminal txns and the terminal state each
// would reach, without mutating anything.
func (c *Coordinator) RecoverPreview(ctx context.Context) ([]store.RecoverPreviewRow, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.ListRecoverPreview(ctx)
}

// RecoveryRecord describes one transaction driven to completion by Recover.
type RecoveryRecord struct {
	TxnID     string
	FromState string
	ToState   string
}

// driveSecondPhase drives every not-yet-final participant to the recorded
// decision and records the terminal state. It assumes c.mu is held and that
// the transaction already has a recorded decision (state COMMITTING or
// ABORTING). For each participant, if its final field is non-empty it is
// skipped — this is the idempotency that makes repeated recovery safe.
func (c *Coordinator) driveSecondPhase(ctx context.Context, t store.TxnRow, parts []store.ParticipantRow) (string, []store.ParticipantRow, error) {
	wantCommit := t.State == store.StateCommitting
	now := c.clock.Now()
	for i := range parts {
		if parts[i].Final != "" {
			continue
		}
		res, ok := c.resources[parts[i].Resource]
		var final string
		if wantCommit {
			final = store.FinalCommitted
			if ok {
				if err := res.Commit(ctx); err != nil {
					return "", nil, fmt.Errorf("commit %s: %w", parts[i].Resource, err)
				}
			}
		} else {
			final = store.FinalAborted
			if ok {
				if err := res.Abort(ctx); err != nil {
					return "", nil, fmt.Errorf("abort %s: %w", parts[i].Resource, err)
				}
			}
		}
		if err := c.store.FinalizeParticipant(ctx, t.TxnID, parts[i].Resource, final, now); err != nil {
			return "", nil, err
		}
		parts[i].Final = final
		parts[i].FinalizedAt = now
	}
	terminal := store.StateCommitted
	if !wantCommit {
		terminal = store.StateAborted
	}
	if err := c.store.SetTxnFinalState(ctx, t.TxnID, terminal, now); err != nil {
		return "", nil, err
	}
	return terminal, parts, nil
}

// ---- read-only accessors for the HTTP layer --------------------------------

// GetTxn returns the transaction row and its participants.
func (c *Coordinator) GetTxn(ctx context.Context, txnID string) (store.TxnRow, []store.ParticipantRow, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.GetTxn(ctx, txnID)
}

// ListParticipants returns the participants of a transaction.
func (c *Coordinator) ListParticipants(ctx context.Context, txnID string) ([]store.ParticipantRow, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.ListParticipants(ctx, txnID)
}

// ListTxns returns all transactions, optionally filtered by state.
func (c *Coordinator) ListTxns(ctx context.Context, stateFilter string) ([]store.TxnRow, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.ListTxns(ctx, stateFilter)
}

// DeleteTxn removes a terminal transaction and its rows.
func (c *Coordinator) DeleteTxn(ctx context.Context, txnID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.DeleteTxn(ctx, txnID)
}

// ListDecisions returns all decision records.
func (c *Coordinator) ListDecisions(ctx context.Context) ([]store.DecisionRow, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.ListDecisions(ctx)
}

// GetDecision returns the decision record for a txn.
func (c *Coordinator) GetDecision(ctx context.Context, txnID string) (store.DecisionRow, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.GetDecision(ctx, txnID)
}

// ListLedger returns all committed-effect rows.
func (c *Coordinator) ListLedger(ctx context.Context) ([]store.LedgerRow, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.ListLedger(ctx)
}

// ListLedgerForResource returns the committed-effect rows for a resource.
func (c *Coordinator) ListLedgerForResource(ctx context.Context, resource string) ([]store.LedgerRow, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.ListLedgerForResource(ctx, resource)
}

// LedgerCount exposes the number of committed effects for a transaction.
func (c *Coordinator) LedgerCount(ctx context.Context, txnID string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.LedgerCount(ctx, txnID)
}

// LedgerHas exposes whether a (resource, txn_id) committed effect exists.
func (c *Coordinator) LedgerHas(ctx context.Context, resource, txnID string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.LedgerHas(ctx, resource, txnID)
}

// Stats summarises coordinator state.
func (c *Coordinator) Stats(ctx context.Context) (store.Stats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.Stats(ctx)
}
