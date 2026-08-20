package coordinator

import (
	"context"
	"task105-2pc/internal/store"
	"testing"
)

func TestAnalysisReportsPendingPreparingTransaction(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", store.VoteYes)
	if err := c.Begin(ctx, "pending", []string{"R1"}); err != nil {
		t.Fatal(err)
	}
	analysis, err := c.Analyze(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if analysis.RecoveryPending != 1 || analysis.Healthy {
		t.Fatalf("analysis=%+v", analysis)
	}
	if len(analysis.Recommendations()) == 0 || analysis.Fingerprint() == "" {
		t.Fatalf("missing report details: %+v", analysis)
	}
}
