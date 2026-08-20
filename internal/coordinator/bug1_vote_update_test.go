package coordinator

import (
	"context"
	"testing"
)

func TestVoteUpdateFromNoToYesRefreshesLiveParticipant(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()
	mustRegister(t, c, "R1", "no")
	if err := c.UpdateResourceVote(ctx, "R1", "yes"); err != nil {
		t.Fatal(err)
	}
	if err := c.Begin(ctx, "T1", []string{"R1"}); err != nil {
		t.Fatal(err)
	}
	result, err := c.Prepare(ctx, "T1")
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision != "commit" || result.Votes["R1"] != "yes" {
		t.Fatalf("updated vote was not used: %+v", result)
	}
}
