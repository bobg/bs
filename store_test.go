package bs_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"testing"

	"github.com/bobg/bs"
	"github.com/bobg/bs/mem"
)

func TestStore(t *testing.T) {
	data, err := ioutil.ReadFile("testdata/yubnub.opus")
	if err != nil {
		t.Fatal(err)
	}
	store := mem.New()

	ctx := context.Background()

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
