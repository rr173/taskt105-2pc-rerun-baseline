package coordinator

import (
	"context"
	"time"
)

// realNow returns the real wall clock as unix nanoseconds. It is indirected
// through a function so the package has no top-level time.Now call that would
// complicate reasoning; production code calls this via RealClock.
func realNow() int64 { return time.Now().UTC().UnixNano() }

// MockClock is a controllable clock for deterministic tests. Set Now by
// assigning the field directly or via Advance.
type MockClock struct {
	T int64
}

// NewMockClock returns a MockClock pinned to a base unix-nanosecond time.
func NewMockClock(base time.Time) *MockClock { return &MockClock{T: base.UTC().UnixNano()} }

func (m *MockClock) Now() int64 { return m.T }

// Advance moves the clock forward by d.
func (m *MockClock) Advance(d time.Duration) { m.T += int64(d) }

// InMemoryResource is a process-in participant whose prepare vote is fixed at
// registration time (and may be reconfigured via SetVote, called by
// UpdateResourceVote). Commit and Abort are no-ops; the observable effect of
// a commit is the commit-ledger row and the committed_count increment applied
// by the store in FinalizeParticipant, not a side effect here. This keeps the
// demo self-contained while still exercising the full prepare/commit/abort
// lifecycle and its persistence.
type InMemoryResource struct {
	name string
	vote string
}

// NewInMemoryResource constructs a participant that always returns vote from
// Prepare.
func NewInMemoryResource(name, vote string) *InMemoryResource {
	return &InMemoryResource{name: name, vote: vote}
}

// SetVote reconfigures the participant's prepare vote. Used by
// UpdateResourceVote to keep the in-memory handle in sync with the store.
func (r *InMemoryResource) SetVote(vote string) { r.vote = vote }

func (r *InMemoryResource) Name() string                            { return r.name }
func (r *InMemoryResource) Prepare(context.Context) (string, error) { return r.vote, nil }
func (r *InMemoryResource) Commit(context.Context) error            { return nil }
func (r *InMemoryResource) Abort(context.Context) error             { return nil }
