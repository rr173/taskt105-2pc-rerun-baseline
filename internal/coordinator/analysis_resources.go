package coordinator

import (
	"context"
)

func (c *Coordinator) resourceDiagnostics(ctx context.Context) ([]ResourceDiagnostic, error) {
	rows, err := c.ListResources(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ResourceDiagnostic, 0, len(rows))
	for _, row := range rows {
		out = append(out, ResourceDiagnostic{Name: row.Name, Vote: row.Vote, Committed: row.CommittedCount, Aborted: row.AbortedCount})
	}
	return out, nil
}
