package mem

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/bobg/bs/testutil"
)

func TestStore(t *testing.T) {
	data, err := ioutil.ReadFile("../../testdata/yubnub.opus")
	if err != nil {
		t.Fatal(err)
	}
	testutil.ReadWrite(context.Background(), t, New(), data)
}

func TestAnchors(t *testing.T) {
	testutil.Anchors(context.Background(), t, New())
}
