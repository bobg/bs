package file

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/bobg/bs/testutil"
)

func TestStore(t *testing.T) {
	dirname, err := ioutil.TempDir("", "filestore")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirname)

	data, err := ioutil.ReadFile("../testdata/yubnub.opus")
	if err != nil {
		t.Fatal(err)
	}

	testutil.ReadWrite(context.Background(), t, New(dirname), data)
}
