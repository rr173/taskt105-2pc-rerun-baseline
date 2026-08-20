package coordinator

import "context"

func (c *Coordinator) ledgerCount(ctx context.Context) (int, error) {
	rows, err := c.ListLedger(ctx)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}
