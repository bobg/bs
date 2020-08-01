package gcs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"cloud.google.com/go/storage"
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
	var (
		creds     = os.Getenv(credsVar)
		projectID = os.Getenv(projVar)
	)
	if creds == "" || projectID == "" {
		t.Skipf("to run TestStore, set %s to the name of a credentials file and %s to a project ID", credsVar, projVar)
	}

	var r [30]byte
	_, err := rand.Read(r[:])
	if err != nil {
		t.Fatal(err)
	}
	bucketName := hex.EncodeToString(r[:])

	data, err := ioutil.ReadFile("../../testdata/yubnub.opus")
	if err != nil {
		t.Fatal(err)
	}

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
	defer bucket.Delete(ctx)

	testutil.ReadWrite(ctx, t, New(bucket), data)
}
