package store

import (
	"context"
	"testing"
)

func TestRecoverPreviewAbortsNeverPreparedTransaction(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.RegisterResource(ctx, "R1", VoteYes, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginTxn(ctx, "T-preparing", []string{"R1"}, 2); err != nil {
		t.Fatal(err)
	}
	rows, err := s.ListRecoverPreview(ctx)
	if err != nil || len(rows) != 1 || rows[0].CurrentState != StatePreparing || rows[0].TargetState != StateAborted {
		t.Fatalf("preview=%+v err=%v", rows, err)
	}
}
