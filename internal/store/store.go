// Package store provides the SQLite-backed persistence layer for the
// two-phase-commit (2PC) transaction coordinator.
//
// The schema models the full 2PC decision log plus per-resource observable
// state counters:
//
//   - resources: registered participants with their prepare vote and the
//     committed_count / aborted_count counters that make a commit or abort
//     observable (not just ledger rows).
//   - transactions: the txn state machine (PREPARING -> COMMITTING/ABORTING
//     -> COMMITTED/ABORTED).
//   - participants: per (txn, resource) the vote and final state.
//   - decisions: the immutable, at-most-one-per-txn decision record.
//   - commit_ledger: the idempotent (resource, txn_id) PK committed-effect log.
//
// The core invariant the coordinator relies on is: a decision (commit or
// abort) is recorded in the decisions table and reflected in the
// transaction's state *before* any participant is driven into the second
// phase. RecordPrepare writes votes + decision + state in one transaction;
// the second phase (Finish/Recover) only ever reads that decision and drives
// participants, so a crash between "decision recorded" and "participants
// finalized" is recoverable from the log.
//
// FinalizeParticipant is the single place that both records a participant's
// final state AND applies its observable effect (incrementing the resource
// counter and, for commit, appending the ledger row). Its idempotency rests
// on two mechanisms: the participant's final column going from empty to a
// value (so re-driving an already-final participant is skipped) and the
// ledger's (resource, txn_id) primary key (so a committed effect is appended
// at most once even if the increment guard ever raced).
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

// Transaction and participant state constants. These string values are the
// authoritative representation persisted to SQLite; domain code and the HTTP
// layer must use them verbatim.
const (
	StatePreparing  = "PREPARING"  // txn created, phase 1 not run yet
	StateCommitting = "COMMITTING" // decision=commit recorded, phase 2 not complete
	StateAborting   = "ABORTING"   // decision=abort recorded, phase 2 not complete
	StateCommitted  = "COMMITTED"  // phase 2 complete, all participants committed
	StateAborted    = "ABORTED"    // phase 2 complete, all participants aborted

	VoteYes = "yes"
	VoteNo  = "no"

	DecisionCommit = "commit"
	DecisionAbort  = "abort"

	FinalCommitted = "committed"
	FinalAborted   = "aborted"
)

// Sentinel errors. Callers must use errors.Is to detect them; they are never
// wrapped so that errors.Is keeps working across layers.
var (
	// ErrNotFound means the named transaction does not exist.
	ErrNotFound = errors.New("txn not found")
	// ErrResourceNotFound means no resource with that name is registered.
	ErrResourceNotFound = errors.New("resource not found")
	// ErrResourceExists means a resource with this name is already registered.
	ErrResourceExists = errors.New("resource exists")
	// ErrTxnExists means a transaction with this txn_id already exists.
	ErrTxnExists = errors.New("txn exists")
	// ErrResourceMissing means a referenced resource has not been registered.
	ErrResourceMissing = errors.New("resource not registered")
	// ErrResourceInUse means a resource is referenced by a non-terminal txn
	// and so cannot be deleted.
	ErrResourceInUse = errors.New("resource in use")
	// ErrInvalidState means the transaction is not in a state required for
	// the requested operation.
	ErrInvalidState = errors.New("invalid state for operation")
	// ErrNoDecision means the transaction has no recorded decision (still
	// PREPARING), so the second phase cannot be driven.
	ErrNoDecision = errors.New("no decision recorded")
	// ErrNotTerminal means a cleanup was attempted on a non-terminal txn.
	ErrNotTerminal = errors.New("txn not terminal")
)

// ResourceRow is a registered resource, its prepare vote and the observable
// committed/aborted counters.
type ResourceRow struct {
	Name           string
	Vote           string
	CommittedCount int64
	AbortedCount   int64
	CreatedAt      int64 // unix nanoseconds
}

// ParticipantRow is one resource's participation in one transaction.
type ParticipantRow struct {
	TxnID       string
	Resource    string
	Vote        string // "" until prepare records the vote
	VotedAt     int64  // 0 until prepare
	Final       string // "" until finish records the final state
	FinalizedAt int64  // 0 until finish
}

// TxnRow is the persisted view of a transaction.
type TxnRow struct {
	TxnID     string
	Resources []string
	State     string
	Decision  string // "" until recorded
	StartedAt int64  // unix nanoseconds
	DecidedAt int64  // 0 until decided
}

// DecisionRow is one immutable decision record.
type DecisionRow struct {
	TxnID      string
	Decision   string
	RecordedAt int64
}

// LedgerRow is one committed effect.
type LedgerRow struct {
	Resource  string
	TxnID     string
	AppliedAt int64
}

// Stats summarises coordinator state for /admin/stats.
type Stats struct {
	TxnCount      int
	ByState       map[string]int
	ResourceCount int
	LedgerCount   int
	DecisionCount int
}

// Store wraps a *sql.DB that stores the 2PC coordinator log.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the 2PC database at path and applies the schema.
// path may be a file path or ":memory:". A single connection is used so that
// SQLite serialises every statement; this makes the check-then-write
// sequences in RecordPrepare and FinalizeParticipant race-free without
// relying on busy retries or application-level locks.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %q: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	if err := applyPragmas(db); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func applyPragmas(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA busy_timeout=5000",
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("pragma %q: %w", p, err)
		}
	}
	return nil
}

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS resources (
			name            TEXT PRIMARY KEY,
			vote            TEXT NOT NULL,
			committed_count INTEGER NOT NULL DEFAULT 0,
			aborted_count   INTEGER NOT NULL DEFAULT 0,
			created_at      INTEGER NOT NULL
		) WITHOUT ROWID`,
		`CREATE TABLE IF NOT EXISTS transactions (
			txn_id     TEXT PRIMARY KEY,
			resources  TEXT NOT NULL,
			state      TEXT NOT NULL,
			decision   TEXT,
			started_at INTEGER NOT NULL,
			decided_at INTEGER
		) WITHOUT ROWID`,
		`CREATE TABLE IF NOT EXISTS participants (
			txn_id       TEXT NOT NULL,
			resource     TEXT NOT NULL,
			vote         TEXT,
			voted_at     INTEGER,
			final        TEXT,
			finalized_at INTEGER,
			PRIMARY KEY (txn_id, resource)
		) WITHOUT ROWID`,
		`CREATE TABLE IF NOT EXISTS decisions (
			txn_id      TEXT PRIMARY KEY,
			decision    TEXT NOT NULL,
			recorded_at INTEGER NOT NULL
		) WITHOUT ROWID`,
		`CREATE TABLE IF NOT EXISTS commit_ledger (
			resource    TEXT NOT NULL,
			txn_id      TEXT NOT NULL,
			applied_at  INTEGER NOT NULL,
			PRIMARY KEY (resource, txn_id)
		) WITHOUT ROWID`,
		`CREATE INDEX IF NOT EXISTS idx_txns_state ON transactions(state)`,
		`CREATE INDEX IF NOT EXISTS idx_txns_started ON transactions(started_at, txn_id)`,
		`CREATE INDEX IF NOT EXISTS idx_participants_resource ON participants(resource)`,
		`CREATE INDEX IF NOT EXISTS idx_decisions_recorded ON decisions(recorded_at)`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error { return s.db.Close() }

// Ping verifies the database connection is usable. Used by /healthz.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// ---- resources ---------------------------------------------------------------

// RegisterResource persists a named resource with its configured prepare vote.
// Returns ErrResourceExists if the name is already taken.
func (s *Store) RegisterResource(ctx context.Context, name, vote string, now int64) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO resources (name, vote, committed_count, aborted_count, created_at) VALUES (?, ?, 0, 0, ?)`,
		name, vote, now)
	if err != nil {
		if isConstraintUnique(err) {
			return ErrResourceExists
		}
		return fmt.Errorf("register resource: %w", err)
	}
	return nil
}

// GetResource returns the registered resource row, or (ResourceRow{}, false)
// when no resource with that name exists.
func (s *Store) GetResource(ctx context.Context, name string) (ResourceRow, bool, error) {
	var r ResourceRow
	err := s.db.QueryRowContext(ctx,
		`SELECT name, vote, committed_count, aborted_count, created_at FROM resources WHERE name = ?`, name).
		Scan(&r.Name, &r.Vote, &r.CommittedCount, &r.AbortedCount, &r.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ResourceRow{}, false, nil
		}
		return ResourceRow{}, false, fmt.Errorf("get resource: %w", err)
	}
	return r, true, nil
}

// ListResources returns all registered resources ordered by name.
func (s *Store) ListResources(ctx context.Context) ([]ResourceRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, vote, committed_count, aborted_count, created_at FROM resources ORDER BY name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ResourceRow
	for rows.Next() {
		var r ResourceRow
		if err := rows.Scan(&r.Name, &r.Vote, &r.CommittedCount, &r.AbortedCount, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpdateResourceVote changes a resource's configured prepare vote. Returns
// ErrResourceNotFound when the resource does not exist.
func (s *Store) UpdateResourceVote(ctx context.Context, name, vote string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE resources SET vote = ? WHERE name = ?`, vote, name)
	if err != nil {
		return fmt.Errorf("update resource vote: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update resource vote rows: %w", err)
	}
	if n == 0 {
		return ErrResourceNotFound
	}
	return nil
}

// DeleteResource unregisters a resource. It refuses if any non-terminal txn
// references the resource, returning ErrResourceInUse with the active txn ids.
func (s *Store) DeleteResource(ctx context.Context, name string) ([]string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM resources WHERE name = ?`, name).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrResourceNotFound
		}
		return nil, fmt.Errorf("check resource: %w", err)
	}

	// Active txns referencing this resource block deletion.
	rows, err := tx.QueryContext(ctx,
		`SELECT DISTINCT p.txn_id FROM participants p
		 JOIN transactions t ON t.txn_id = p.txn_id
		 WHERE p.resource = ? AND t.state IN (?, ?, ?)
		 ORDER BY p.txn_id ASC`, name, StatePreparing, StateCommitting, StateAborting)
	if err != nil {
		return nil, fmt.Errorf("find active txns: %w", err)
	}
	var active []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		active = append(active, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(active) > 0 {
		return active, ErrResourceInUse
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM resources WHERE name = ?`, name); err != nil {
		return nil, fmt.Errorf("delete resource: %w", err)
	}
	return active, tx.Commit()
}

// ---- transactions ------------------------------------------------------------

// BeginTxn creates a transaction in PREPARING state with one participant row
// per resource. Returns ErrResourceMissing for unknown names, ErrTxnExists for
// a duplicate txn_id.
func (s *Store) BeginTxn(ctx context.Context, txnID string, resources []string, now int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, r := range resources {
		var tmp string
		err := tx.QueryRowContext(ctx, `SELECT name FROM resources WHERE name = ?`, r).Scan(&tmp)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrResourceMissing
			}
			return fmt.Errorf("check resource: %w", err)
		}
	}
	resJSON, err := marshalResources(resources)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO transactions (txn_id, resources, state, started_at) VALUES (?, ?, ?, ?)`,
		txnID, resJSON, StatePreparing, now)
	if err != nil {
		if isConstraintUnique(err) {
			return ErrTxnExists
		}
		return fmt.Errorf("insert txn: %w", err)
	}
	for _, r := range resources {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO participants (txn_id, resource) VALUES (?, ?)`, txnID, r); err != nil {
			return fmt.Errorf("insert participant: %w", err)
		}
	}
	return tx.Commit()
}

// RecordPrepare atomically records the phase-1 result: each participant's
// vote, the decision, the new state (COMMITTING/ABORTING) and decided_at. It
// refuses any transaction not in PREPARING state. votes may be empty for the
// recovery path that aborts a never-prepared PREPARING txn.
func (s *Store) RecordPrepare(ctx context.Context, txnID string, votes map[string]string, decision string, now int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var state string
	if err := tx.QueryRowContext(ctx, `SELECT state FROM transactions WHERE txn_id = ?`, txnID).Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read state: %w", err)
	}
	if state != StatePreparing {
		return ErrInvalidState
	}

	for resource, vote := range votes {
		if _, err := tx.ExecContext(ctx,
			`UPDATE participants SET vote = ?, voted_at = ? WHERE txn_id = ? AND resource = ?`,
			vote, now, txnID, resource); err != nil {
			return fmt.Errorf("update participant vote: %w", err)
		}
	}

	newState := StateCommitting
	if decision == DecisionAbort {
		newState = StateAborting
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO decisions (txn_id, decision, recorded_at) VALUES (?, ?, ?)`, txnID, decision, now); err != nil {
		if isConstraintUnique(err) {
			// Decision already recorded for this txn; treat as invalid state.
			return ErrInvalidState
		}
		return fmt.Errorf("insert decision: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE transactions SET state = ?, decision = ?, decided_at = ? WHERE txn_id = ?`,
		newState, decision, now, txnID); err != nil {
		return fmt.Errorf("update txn state: %w", err)
	}
	return tx.Commit()
}

// GetTxn returns the transaction row and its participants. Returns
// ErrNotFound when the txn does not exist.
func (s *Store) GetTxn(ctx context.Context, txnID string) (TxnRow, []ParticipantRow, error) {
	var t TxnRow
	var resJSON string
	var decision sql.NullString
	var decidedAt sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT txn_id, resources, state, decision, started_at, decided_at FROM transactions WHERE txn_id = ?`,
		txnID).Scan(&t.TxnID, &resJSON, &t.State, &decision, &t.StartedAt, &decidedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return TxnRow{}, nil, ErrNotFound
		}
		return TxnRow{}, nil, fmt.Errorf("get txn: %w", err)
	}
	t.Decision = decision.String
	t.DecidedAt = decidedAt.Int64
	t.Resources, err = unmarshalResources(resJSON)
	if err != nil {
		return TxnRow{}, nil, fmt.Errorf("unmarshal resources: %w", err)
	}
	parts, err := s.ListParticipants(ctx, txnID)
	if err != nil {
		return TxnRow{}, nil, err
	}
	return t, parts, nil
}

// ListParticipants returns the participant rows for a transaction, ordered by
// resource name.
func (s *Store) ListParticipants(ctx context.Context, txnID string) ([]ParticipantRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT txn_id, resource, vote, voted_at, final, finalized_at
		 FROM participants WHERE txn_id = ? ORDER BY resource ASC`, txnID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ParticipantRow
	for rows.Next() {
		var p ParticipantRow
		var vote sql.NullString
		var votedAt sql.NullInt64
		var final sql.NullString
		var finalizedAt sql.NullInt64
		if err := rows.Scan(&p.TxnID, &p.Resource, &vote, &votedAt, &final, &finalizedAt); err != nil {
			return nil, err
		}
		p.Vote = vote.String
		p.VotedAt = votedAt.Int64
		p.Final = final.String
		p.FinalizedAt = finalizedAt.Int64
		out = append(out, p)
	}
	return out, rows.Err()
}

// ListTxns returns all transactions ordered by (started_at, txn_id). If
// stateFilter is non-empty, only transactions in that state are returned.
func (s *Store) ListTxns(ctx context.Context, stateFilter string) ([]TxnRow, error) {
	q := `SELECT txn_id, resources, state, decision, started_at, decided_at FROM transactions`
	var args []any
	if stateFilter != "" {
		q += ` WHERE state = ?`
		args = append(args, stateFilter)
	}
	q += ` ORDER BY started_at ASC, txn_id ASC`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TxnRow
	for rows.Next() {
		var t TxnRow
		var resJSON string
		var decision sql.NullString
		var decidedAt sql.NullInt64
		if err := rows.Scan(&t.TxnID, &resJSON, &t.State, &decision, &t.StartedAt, &decidedAt); err != nil {
			return nil, err
		}
		t.Decision = decision.String
		t.DecidedAt = decidedAt.Int64
		t.Resources, err = unmarshalResources(resJSON)
		if err != nil {
			return nil, fmt.Errorf("unmarshal resources: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ListNonFinalTxns returns every transaction whose state is not COMMITTED or
// ABORTED. These are the ones Recover must drive to completion.
func (s *Store) ListNonFinalTxns(ctx context.Context) ([]TxnRow, error) {
	return s.listNonFinalTxnsDirect(ctx)
}

// listNonFinalTxnsDirect returns txns in any of the three non-terminal states,
// in (started_at, txn_id) order.
func (s *Store) listNonFinalTxnsDirect(ctx context.Context) ([]TxnRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT txn_id, resources, state, decision, started_at, decided_at
		 FROM transactions WHERE state IN (?, ?, ?) ORDER BY started_at ASC, txn_id ASC`,
		StatePreparing, StateCommitting, StateAborting)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TxnRow
	for rows.Next() {
		var t TxnRow
		var resJSON string
		var decision sql.NullString
		var decidedAt sql.NullInt64
		if err := rows.Scan(&t.TxnID, &resJSON, &t.State, &decision, &t.StartedAt, &decidedAt); err != nil {
			return nil, err
		}
		t.Decision = decision.String
		t.DecidedAt = decidedAt.Int64
		t.Resources, err = unmarshalResources(resJSON)
		if err != nil {
			return nil, fmt.Errorf("unmarshal resources: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// FinalizeParticipant records a participant's final state and applies its
// observable effect: for commit it appends a ledger row and increments
// committed_count (both idempotent via the ledger PK); for abort it
// increments aborted_count (idempotent via the participant final column).
// Re-running on an already-final participant is a no-op.
func (s *Store) FinalizeParticipant(ctx context.Context, txnID, resource, final string, now int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var curFinal sql.NullString
	if err := tx.QueryRowContext(ctx,
		`SELECT final FROM participants WHERE txn_id = ? AND resource = ?`, txnID, resource).Scan(&curFinal); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read final: %w", err)
	}
	var txnState string
	if err := tx.QueryRowContext(ctx, `SELECT state FROM transactions WHERE txn_id = ?`, txnID).Scan(&txnState); err != nil {
		return fmt.Errorf("read txn state: %w", err)
	}
	if curFinal.String != "" && txnState != StateCommitted && txnState != StateAborted {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE participants SET final = ?, finalized_at = ? WHERE txn_id = ? AND resource = ?`,
		final, now, txnID, resource); err != nil {
		return fmt.Errorf("finalize participant: %w", err)
	}
	if final == FinalCommitted {
		if _, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO commit_ledger (resource, txn_id, applied_at) VALUES (?, ?, ?)`,
			resource, txnID, now); err != nil {
			return fmt.Errorf("ledger append: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE resources SET committed_count = committed_count + 1 WHERE name = ?`, resource); err != nil {
			return fmt.Errorf("incr committed_count: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx,
			`UPDATE resources SET aborted_count = aborted_count + 1 WHERE name = ?`, resource); err != nil {
			return fmt.Errorf("incr aborted_count: %w", err)
		}
	}
	return tx.Commit()
}

// SetTxnFinalState sets the transaction's terminal state. Idempotent.
func (s *Store) SetTxnFinalState(ctx context.Context, txnID, state string, now int64) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE transactions SET state = ? WHERE txn_id = ?`, state, txnID); err != nil {
		return fmt.Errorf("set final state: %w", err)
	}
	return nil
}

// DeleteTxn removes a transaction and all its rows (participants, decision,
// ledger entries). It refuses with ErrNotTerminal unless the txn is COMMITTED
// or ABORTED, and ErrNotFound if the txn does not exist.
func (s *Store) DeleteTxn(ctx context.Context, txnID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var state string
	err = tx.QueryRowContext(ctx, `SELECT state FROM transactions WHERE txn_id = ?`, txnID).Scan(&state)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read state: %w", err)
	}
	if state != StateCommitted && state != StateAborted {
		return ErrNotTerminal
	}
	for _, q := range []string{
		`DELETE FROM participants WHERE txn_id = ?`,
		`DELETE FROM decisions WHERE txn_id = ?`,
		`DELETE FROM commit_ledger WHERE txn_id = ?`,
		`DELETE FROM transactions WHERE txn_id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, q, txnID); err != nil {
			return fmt.Errorf("delete txn cascade: %w", err)
		}
	}
	return tx.Commit()
}

// ---- decisions --------------------------------------------------------------

// ListDecisions returns all decision records ordered by recorded_at.
func (s *Store) ListDecisions(ctx context.Context) ([]DecisionRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT txn_id, decision, recorded_at FROM decisions ORDER BY recorded_at ASC, txn_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DecisionRow
	for rows.Next() {
		var d DecisionRow
		if err := rows.Scan(&d.TxnID, &d.Decision, &d.RecordedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// GetDecision returns the decision record for a txn, or (DecisionRow{}, false)
// when none exists.
func (s *Store) GetDecision(ctx context.Context, txnID string) (DecisionRow, bool, error) {
	var d DecisionRow
	err := s.db.QueryRowContext(ctx,
		`SELECT txn_id, decision, recorded_at FROM decisions WHERE txn_id = ?`, txnID).
		Scan(&d.TxnID, &d.Decision, &d.RecordedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DecisionRow{}, false, nil
		}
		return DecisionRow{}, false, fmt.Errorf("get decision: %w", err)
	}
	return d, true, nil
}

// ---- ledger ----------------------------------------------------------------

// ListLedger returns all committed-effect rows ordered by (resource, txn_id).
func (s *Store) ListLedger(ctx context.Context) ([]LedgerRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT resource, txn_id, applied_at FROM commit_ledger ORDER BY resource ASC, txn_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LedgerRow
	for rows.Next() {
		var l LedgerRow
		if err := rows.Scan(&l.Resource, &l.TxnID, &l.AppliedAt); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// ListLedgerForResource returns the committed-effect rows for one resource,
// ordered by applied_at.
func (s *Store) ListLedgerForResource(ctx context.Context, resource string) ([]LedgerRow, bool, error) {
	ok, err := s.resourceExists(ctx, resource)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT resource, txn_id, applied_at FROM commit_ledger WHERE resource = ? ORDER BY txn_id ASC, applied_at ASC`,
		resource)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var out []LedgerRow
	for rows.Next() {
		var l LedgerRow
		if err := rows.Scan(&l.Resource, &l.TxnID, &l.AppliedAt); err != nil {
			return nil, false, err
		}
		out = append(out, l)
	}
	return out, true, rows.Err()
}

func (s *Store) resourceExists(ctx context.Context, name string) (bool, error) {
	var tmp int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM resources WHERE name = ?`, name).Scan(&tmp)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// LedgerCount returns the number of committed effects for a transaction.
func (s *Store) LedgerCount(ctx context.Context, txnID string) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM commit_ledger WHERE txn_id = ?`, txnID).Scan(&n); err != nil {
		return 0, fmt.Errorf("ledger count: %w", err)
	}
	return n, nil
}

// LedgerHas reports whether a (resource, txn_id) committed effect exists.
func (s *Store) LedgerHas(ctx context.Context, resource, txnID string) (bool, error) {
	var tmp int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM commit_ledger WHERE resource = ? AND txn_id = ?`, resource, txnID).Scan(&tmp)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("ledger has: %w", err)
	}
	return true, nil
}

// ---- stats -----------------------------------------------------------------

// Stats summarises the coordinator's persisted state.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	st.ByState = map[string]int{}
	for _, state := range []string{StatePreparing, StateCommitting, StateAborting, StateCommitted, StateAborted} {
		var n int
		if err := s.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM transactions WHERE state = ?`, state).Scan(&n); err != nil {
			return Stats{}, fmt.Errorf("count state %s: %w", state, err)
		}
		st.ByState[state] = n
		st.TxnCount += n
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM resources`).Scan(&st.ResourceCount); err != nil {
		return Stats{}, fmt.Errorf("count resources: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM commit_ledger`).Scan(&st.LedgerCount); err != nil {
		return Stats{}, fmt.Errorf("count ledger: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM decisions`).Scan(&st.DecisionCount); err != nil {
		return Stats{}, fmt.Errorf("count decisions: %w", err)
	}
	return st, nil
}

// ---- helpers ---------------------------------------------------------------

// marshalResources / unmarshalResources serialize the resource list of a
// transaction as a JSON array of strings.
func marshalResources(rs []string) (string, error) {
	b, err := json.Marshal(rs)
	if err != nil {
		return "", fmt.Errorf("marshal resources: %w", err)
	}
	return string(b), nil
}

func unmarshalResources(s string) ([]string, error) {
	if s == "" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, fmt.Errorf("unmarshal resources: %w", err)
	}
	return out, nil
}

// isConstraintUnique reports whether err is a SQLite UNIQUE constraint
// violation. modernc.org/sqlite surfaces these with the "UNIQUE constraint"
// substring (SQLITE_CONSTRAINT_UNIQUE).
func isConstraintUnique(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint")
}

// RecoverPreviewRow is a non-terminal txn paired with the terminal state it
// would reach if Recover ran now.
type RecoverPreviewRow struct {
	TxnID        string
	CurrentState string
	TargetState  string
}

// ListRecoverPreview returns the non-final txns and the terminal state each
// would reach if recovered. PREPARING -> ABORTED, COMMITTING -> COMMITTED,
// ABORTING -> ABORTED. It does not mutate state.
func (s *Store) ListRecoverPreview(ctx context.Context) ([]RecoverPreviewRow, error) {
	txns, err := s.listNonFinalTxnsDirect(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]RecoverPreviewRow, 0, len(txns))
	for _, t := range txns {
		target := StateAborted
		if t.State == StateCommitting || t.State == StatePreparing {
			target = StateCommitted
		}
		out = append(out, RecoverPreviewRow{TxnID: t.TxnID, CurrentState: t.State, TargetState: target})
	}
	return out, nil
}
