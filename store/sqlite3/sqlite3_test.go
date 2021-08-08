package sqlite3

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"
	"testing"

	"github.com/bobg/bs/testutil"
)

func TestStore(t *testing.T) {
	data, err := ioutil.ReadFile("../../testdata/yubnub.opus")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	err = withTestStore(ctx, func(s *Store) error {
		testutil.ReadWrite(ctx, t, s, data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAnchors(t *testing.T) {
	ctx := context.Background()
	err := withTestStore(ctx, func(s *Store) error {
		testutil.Anchors(ctx, t, s, true)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func withTestStore(ctx context.Context, fn func(*Store) error) error {
	f, err := ioutil.TempFile("", "bssqlite3test")
	if err != nil {
		return err
	}

	tmpfile := f.Name()
	f.Close()
	defer os.Remove(tmpfile)

	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		return err
	}
	defer db.Close()

	s, err := New(ctx, db)
	if err != nil {
		return err
	}

	return fn(s)
}
