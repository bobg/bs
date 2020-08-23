package gcs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	stderrs "errors"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/bobg/bs/testutil"
)

func TestEachHexPrefix(t *testing.T) {
	want := []string{
		"e67b", "e67c", "e67d", "e67e", "e67f",
		"e68", "e69", "e6a", "e6b", "e6c", "e6d", "e6e", "e6f",
		"e7", "e8", "e9", "ea", "eb", "ec", "ed", "ee", "ef",
		"f",
	}
	var got []string
	err := eachHexPrefix("e67a", false, func(prefix string) error {
		got = append(got, prefix)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

const (
	credsVar = "BS_GCS_TESTING_CREDS"
	projVar  = "BS_GCS_TESTING_PROJECT"
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
		testutil.Anchors(ctx, t, store)
	})
}

func withStore(t *testing.T, f func(context.Context, *Store)) {
	var (
		creds     = os.Getenv(credsVar)
		projectID = os.Getenv(projVar)
	)
	if creds == "" || projectID == "" {
		t.Skipf("to run %s, set %s to the name of a credentials file and %s to a project ID", t.Name(), credsVar, projVar)
	}
	var r [30]byte
	_, err := rand.Read(r[:])
	if err != nil {
		t.Fatal(err)
	}
	bucketName := hex.EncodeToString(r[:])

	ctx := context.Background()

	client, err := storage.NewClient(ctx, option.WithCredentialsFile(creds))
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("creating bucket %s in project %s", bucketName, projectID)

	bucket := client.Bucket(bucketName)
	err = bucket.Create(ctx, projectID, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		iter := bucket.Objects(ctx, nil)
		for {
			attrs, err := iter.Next()
			if stderrs.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				t.Logf("could not delete bucket %s (%s)", bucketName, err)
				return
			}
			obj := bucket.Object(attrs.Name)
			err = obj.Delete(ctx)
			if err != nil {
				t.Logf("could not delete bucket %s (%s)", bucketName, err)
				return
			}
		}
		err = bucket.Delete(ctx)
		if err != nil {
			t.Logf("could not delete bucket %s (%s)", bucketName, err)
		}
	}()

	f(ctx, New(bucket))
}
