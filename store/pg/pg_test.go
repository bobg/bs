package pg

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"
	"testing"

	_ "github.com/lib/pq"

	"github.com/bobg/bs/testutil"
)

func TestStore(t *testing.T) {
	withStore(t, func(ctx context.Context, store *Store) {
		data, err := ioutil.ReadFile("../../testdata/yubnub.opus")
		if err != nil {
			t.Fatal(err)
		}
		testutil.ReadWrite(ctx, t, store, data)
	})
}

func TestAnchors(t *testing.T) {
	withStore(t, func(ctx context.Context, store *Store) {
		testutil.Anchors(ctx, t, store, false)
	})
}

const connVar = "BS_PG_TESTING_CONN"

func withStore(t *testing.T, f func(context.Context, *Store)) {
	connstr := os.Getenv(connVar)
	if connstr == "" {
		t.Skipf("to run %s, set %s to a valid Postgresql connection string", t.Name(), connVar)
	}

	db, err := sql.Open("postgres", connstr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	store, err := New(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	f(ctx, store)
}
