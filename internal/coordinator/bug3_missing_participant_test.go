package coordinator

import (
	"context"
	"task105-2pc/internal/store"
	"testing"
)

func TestFinishRefusesMissingParticipantHandle(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", store.VoteYes)
	if err := c.Begin(ctx, "T1", []string{"R1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Prepare(ctx, "T1"); err != nil {
		t.Fatal(err)
	}
	delete(c.resources, "R1")
	if _, _, err := c.Finish(ctx, "T1"); err == nil {
		t.Fatal("finish succeeded without a participant handle")
	}
	txn, parts, err := c.GetTxn(ctx, "T1")
	if err != nil || txn.State != store.StateCommitting || len(parts) != 1 || parts[0].Final != "" {
		t.Fatalf("missing handle changed durable state: txn=%+v parts=%+v err=%v", txn, parts, err)
	}
}
