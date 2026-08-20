package coordinator

import (
	"context"
	"task105-2pc/internal/store"
	"testing"
)

func TestAnalysisCountsCommittedEffects(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", store.VoteYes)
	if err := c.Begin(ctx, "done", []string{"R1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Run(ctx, "done"); err != nil {
		t.Fatal(err)
	}
	a, err := c.Analyze(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if a.Decisions != 1 || a.LedgerRows != 1 || a.Summary().Healthy != true {
		t.Fatalf("analysis=%+v", a)
	}
}
