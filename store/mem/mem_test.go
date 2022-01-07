package mem

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/bobg/bs"
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
	testutil.Anchors(context.Background(), t, New(), true)
}

func TestAllRefs(t *testing.T) {
	testutil.AllRefs(context.Background(), t, func() bs.Store { return New() })
}

func TestTypes(t *testing.T) {
	testutil.Types(context.Background(), t, New())
}
