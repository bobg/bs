package testutil

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bobg/bs/split"
	"github.com/bobg/bs/typed"
)

// ReadWrite permits testing a Store implementation
// by split-writing some data to it,
// then reading it back out to make sure it's the same.
func ReadWrite(ctx context.Context, t *testing.T, store typed.Store, data []byte) {
	t1 := time.Now()
	ref, err := split.Write(ctx, store, bytes.NewReader(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %d bytes in %s", len(data), time.Since(t1))

	buf := new(bytes.Buffer)
	t2 := time.Now()
	err = split.Read(ctx, store, ref, buf)
	if err != nil {
		t.Fatal(err)
	}
	got := buf.Bytes()
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
