package httpapi

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"task105-2pc/internal/coordinator"
	"task105-2pc/internal/store"
)

func TestBeginRejectsDuplicateResourcesAsBadRequest(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c, err := coordinator.New(st, coordinator.NewMockClock(time.Unix(1, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterResource(context.Background(), "R1", store.VoteYes); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(NewRouter(New(c)))
	defer ts.Close()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/txns", bytes.NewBufferString(`{"txn_id":"T1","resources":["R1","R1"]}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("duplicate resources status=%d want %d", resp.StatusCode, http.StatusBadRequest)
	}
}
