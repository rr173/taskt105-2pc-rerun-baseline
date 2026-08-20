package coordinator

import "context"

func (c *Coordinator) recoveryCount(ctx context.Context) (int, error) {
	rows, err := c.RecoverPreview(ctx)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}
