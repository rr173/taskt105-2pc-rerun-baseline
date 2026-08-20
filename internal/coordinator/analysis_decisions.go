package coordinator

import "context"

func (c *Coordinator) decisionCount(ctx context.Context) (int, error) {
	rows, err := c.ListDecisions(ctx)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}
