package store

import (
	"context"
	"testing"
)

func TestLedgerQueryEmptyStore(t *testing.T) {
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rows, err := s.ListLedger(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("ledger=%v", rows)
	}
}
