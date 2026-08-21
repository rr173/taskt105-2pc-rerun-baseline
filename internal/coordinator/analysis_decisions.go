package coordinator

import (
	"context"

	"task105-2pc/internal/store"
)

func (c *Coordinator) decisionCount(ctx context.Context) (int, error) {
	rows, err := c.ListDecisions(ctx)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// commitDecisionCount counts only commit decisions. A commit decision carries a
// committed ledger effect; an abort decision carries no ledger row, so the
// "ledger missing" check must compare ledger rows against commit decisions
// alone — counting all decisions would flag every abort as a missing ledger.
func (c *Coordinator) commitDecisionCount(ctx context.Context) (int, error) {
	rows, err := c.ListDecisions(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, d := range rows {
		if d.Decision == store.DecisionCommit {
			n++
		}
	}
	return n, nil
}
