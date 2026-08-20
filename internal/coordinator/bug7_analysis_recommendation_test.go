package coordinator

import (
	"context"
	"strings"
	"task105-2pc/internal/store"
	"testing"
)

func TestAnalysisDoesNotFlagAbortDecisionAsMissingCommitLedger(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", store.VoteNo)
	if err := c.Begin(ctx, "T-abort", []string{"R1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Run(ctx, "T-abort"); err != nil {
		t.Fatal(err)
	}
	a, err := c.Analyze(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, recommendation := range a.Recommendations() {
		if strings.Contains(recommendation, "decisions without committed ledger") {
			t.Fatalf("abort decision produced false ledger warning: %v", a.Recommendations())
		}
	}
}
