package split

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"testing/quick"

	"github.com/bobg/bs/v2"
	"github.com/bobg/bs/v2/store/mem"
)

func TestSplitEmpty(t *testing.T) {
	m := mem.New()

	root, err := Split(context.Background(), m, bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}

	if root != bs.Zero {
		t.Errorf("got Root of %s, want %s", root, bs.Zero)
	}
}

func TestSplit(t *testing.T) {
	var (
		ctx = context.Background()
		s   = mem.New()
	)

	f, err := os.Open("../testdata/yubnub.opus")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	root, err := Split(ctx, s, f, Bits(4), Fanout(2))
	if err != nil {
		t.Fatal(err)
	}

	max, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(ctx, s, root)
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
