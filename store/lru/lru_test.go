package lru

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/bobg/bs/store/mem"
	"github.com/bobg/bs/testutil"
)

func TestStore(t *testing.T) {
	s, err := New(mem.New(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	data, err := ioutil.ReadFile("../../testdata/yubnub.opus")
	if err != nil {
		t.Fatal(err)
	}
	testutil.ReadWrite(context.Background(), t, s, data)
}

func TestAnchors(t *testing.T) {
	s, err := New(mem.New(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	testutil.Anchors(context.Background(), t, s)
}
