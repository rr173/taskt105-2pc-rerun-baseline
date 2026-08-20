package httpapi

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"task105-2pc/internal/coordinator"
	"task105-2pc/internal/store"
	"testing"
	"time"
)

func TestJSONHandlerRejectsTrailingDocument(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c, err := coordinator.New(st, coordinator.NewMockClock(time.Unix(1, 0)))
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(NewRouter(New(c)))
	defer ts.Close()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/resources", bytes.NewBufferString(`{"name":"R1","vote":"yes"} {"name":"R2","vote":"yes"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("trailing JSON status=%d want %d", resp.StatusCode, http.StatusBadRequest)
	}
}
