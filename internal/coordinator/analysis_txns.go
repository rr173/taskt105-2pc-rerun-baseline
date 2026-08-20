package coordinator

import (
	"context"
	"task105-2pc/internal/store"
)

func (c *Coordinator) transactionDiagnostics(ctx context.Context) ([]TxnDiagnostic, error) {
	rows, err := c.ListTxns(ctx, "")
	if err != nil {
		return nil, err
	}
	out := make([]TxnDiagnostic, 0, len(rows))
	for _, row := range rows {
		parts, err := c.ListParticipants(ctx, row.TxnID)
		if err != nil {
			return nil, err
		}
		finalized := 0
		for _, part := range parts {
			if part.Final != "" {
				finalized++
			}
		}
		completion := 100
		if len(parts) > 0 {
			completion = finalized * 100 / len(parts)
		}
		if row.State == store.StatePreparing {
			completion = 0
		}
		out = append(out, TxnDiagnostic{TxnID: row.TxnID, State: row.State, Decision: row.Decision, Participants: len(parts), Finalized: finalized, Completion: completion})
	}
	return out, nil
}
