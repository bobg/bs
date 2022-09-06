package file

import (
	"context"
	"os"
	"testing"

	"github.com/bobg/bs/testutil"
)

func TestStore(t *testing.T) {
	dirname, err := os.MkdirTemp("", "filestore")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirname)

	data, err := os.ReadFile("../../testdata/yubnub.opus")
	if err != nil {
		t.Fatal(err)
	}

	testutil.ReadWrite(context.Background(), t, New(dirname), data)
}

func TestAnchors(t *testing.T) {
	dirname, err := os.MkdirTemp("", "filestore")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirname)

	testutil.Anchors(context.Background(), t, New(dirname), true)
}
