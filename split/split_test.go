package split

import (
	"context"
	"testing"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store/mem"
)

func TestSplitEmpty(t *testing.T) {
	m := mem.New()
	w := NewWriter(context.Background(), m)
	err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	if w.Root != bs.Zero {
		t.Errorf("got Root of %s, want %s", w.Root, bs.Zero)
	}
}
