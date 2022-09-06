package testutil

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/bobg/bs"
	"github.com/bobg/bs/split"
)

// ReadWrite permits testing a Store implementation
// by split-writing some data to it,
// then reading it back out to make sure it's the same.
func ReadWrite(ctx context.Context, t *testing.T, store bs.Store, data []byte) {
	t1 := time.Now()

	w := split.NewWriter(ctx, store)
	_, err := io.Copy(w, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	ref := w.Root
	t.Logf("wrote %d bytes in %s", len(data), time.Since(t1))

	t2 := time.Now()

	r, err := split.NewReader(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("read %d bytes in %s", len(got), time.Since(t2))

	if len(got) != len(data) {
		t.Errorf("got length %d, want %d", len(got), len(data))
	} else {
		for i := 0; i < len(got); i++ {
			if got[i] != data[i] {
				t.Fatalf("mismatch at position %d (of %d)", i, len(got))
			}
		}
	}
}
