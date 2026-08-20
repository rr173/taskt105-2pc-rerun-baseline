package store

import (
	"context"
	"testing"
)

func TestRecoverPreviewEmptyStore(t *testing.T) {
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rows, err := s.ListRecoverPreview(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows=%v", rows)
	}
}
