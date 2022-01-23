package split

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"testing/quick"

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

func TestSplit(t *testing.T) {
	var (
		ctx = context.Background()
		s   = mem.New()
		w   = NewWriter(ctx, s, Bits(4), Fanout(2))
	)

	f, err := os.Open("../testdata/yubnub.opus")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	_, err = io.Copy(w, f)
	if err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	max, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(ctx, s, w.Root)
	if err != nil {
		t.Fatal(err)
	}

	err = quick.Check(func(offset int64, nbytes int) bool {
		offset %= max
		if offset < 0 {
			offset = -offset
		}
		if nbytes < 0 {
			nbytes = 1
		}
		if offset+int64(nbytes) > max {
			nbytes = int(max - offset)
		}

		var (
			want = make([]byte, nbytes)
			got  = make([]byte, nbytes)
		)

		_, err = f.Seek(offset, io.SeekStart)
		if err != nil {
			t.Log(err)
			return false
		}
		_, err = f.Read(want)
		if err != nil {
			t.Log(err)
			return false
		}

		_, err = r.Seek(offset, io.SeekStart)
		if err != nil {
			t.Log(err)
			return false
		}
		_, err = r.Read(got)
		if err != nil {
			t.Log(err)
			return false
		}

		if !bytes.Equal(got, want) {
			t.Logf("got %x, want %x", got, want)
			return false
		}

		return true
	}, nil)
	if err != nil {
		t.Error(err)
	}
}
