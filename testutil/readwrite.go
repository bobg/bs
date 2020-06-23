package testutil

import (
	"bytes"
	"context"
	"testing"

	"github.com/bobg/bs"
)

// ReadWrite permits testing a Store implementation
// by split-writing some data to it,
// then reading it back out to make sure it's the same.
func ReadWrite(ctx context.Context, t *testing.T, store bs.Store, data []byte) {
	ref, err := bs.SplitWrite(ctx, store, bytes.NewReader(data), nil)
	if err != nil {
		t.Fatal(err)
	}

	buf := new(bytes.Buffer)
	err = bs.SplitRead(ctx, store, ref, buf)
	if err != nil {
		t.Fatal(err)
	}

	got := buf.Bytes()
	if len(got) != len(data) {
		t.Errorf("got length %d, want %d", len(got), len(data))
	} else if !bytes.Equal(got, data) {
		t.Error("mismatch")
	}
}
