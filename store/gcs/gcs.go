// Package gcs implements a blob store on Google Cloud Storage.
package gcs

import (
	"context"
	"io"
	"net/http"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/store"
)

var _ anchor.Store = &Store{}

// Store is a Google Cloud Storage-based implementation of a blob store.
// All blobs and other information are stored in a single GCS bucket.
//
// A blob with ref R is stored in a bucket object named b:hex(R)
// (where hex(R) denotes the hexadecimal encoding of R).
//
// The anchor map ref is stored in a bucket object named anchormapref.
type Store struct {
	bucket *storage.BucketHandle
}

// New produces a new Store.
func New(bucket *storage.BucketHandle) *Store {
	return &Store{bucket: bucket}
}

// Get gets the blob with hash `ref`.
func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	name := blobObjName(ref)
	obj := s.bucket.Object(name)

	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "reading info of object %s", name)
	}
	defer r.Close()

	b := make([]byte, r.Attrs.Size)
	_, err = io.ReadFull(r, b)
	return b, errors.Wrapf(err, "reading contents of object %s", name)
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
	ref, added, err := s.putBlob(ctx, b)
	if err != nil {
		return bs.Zero, false, errors.Wrap(err, "storing blob")
	}

	return ref, added, nil
}

func (s *Store) putBlob(ctx context.Context, b bs.Blob) (bs.Ref, bool, error) {
	var (
		ref  = b.Ref()
		name = blobObjName(ref)
		obj  = s.bucket.Object(name).If(storage.Conditions{DoesNotExist: true})
		w    = obj.NewWriter(ctx)
	)
	defer w.Close()

	_, err := w.Write(b)
	var e *googleapi.Error
	if errors.As(err, &e) && e.Code == http.StatusPreconditionFailed {
		return ref, false, nil
	}
	return ref, true, err
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
	// Google Cloud Storage iterators have no API for starting in the middle of a bucket.
	// But they can filter by object-name prefix.
	// So we take (the hex encoding of) `start` and repeatedly compute prefixes for the objects we want.
	// If `start` is e67a, for example, the sequence of generated prefixes is:
	//   e67b e67c e67d e67e e67f
	//   e68 e69 e6a e6b e6c e6d e6e e6f
	//   e7 e8 e9 ea eb ec ed ee ef
	//   f
	return eachHexPrefix(start.String(), false, func(prefix string) error {
		return s.listRefs(ctx, prefix, f)
	})
}

func (s *Store) listRefs(ctx context.Context, prefix string, f func(bs.Ref) error) error {
	iter := s.bucket.Objects(ctx, &storage.Query{Prefix: "b:" + prefix})
	for {
		obj, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "iterating over blobs")
		}

		ref, err := refFromBlobObjName(obj.Name)
		if err != nil {
			return errors.Wrapf(err, "decoding obj name %s", obj.Name)
		}

		err = f(ref)
		if err != nil {
			return err
		}
	}
}

func eachHexPrefix(prefix string, incl bool, f func(string) error) error {
	prefix = strings.ToLower(prefix)
	for len(prefix) > 0 {
		end := hexval(prefix[len(prefix)-1:][0])
		if !incl {
			end++
		}
		prefix = prefix[:len(prefix)-1]
		for c := end; c < 16; c++ {
			err := f(prefix + string(hexdigit(c)))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func hexval(b byte) int {
	switch {
	case '0' <= b && b <= '9':
		return int(b - '0')
	case 'a' <= b && b <= 'f':
		return int(10 + b - 'a')
	case 'A' <= b && b <= 'F':
		return int(10 + b - 'A')
	}
	return 0
}

func hexdigit(n int) byte {
	if n < 10 {
		return byte(n + '0')
	}
	return byte(n - 10 + 'a')
}

const anchorMapRefObjName = "anchormapref"

// AnchorMapRef implements anchor.Getter.
func (s *Store) AnchorMapRef(ctx context.Context) (bs.Ref, error) {
	ref, _, err := s.anchorMapRef(ctx)
	return ref, err
}

func (s *Store) anchorMapRef(ctx context.Context) (bs.Ref, int64, error) {
	obj := s.bucket.Object(anchorMapRefObjName)
	r, err := obj.NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return bs.Zero, 0, anchor.ErrNoAnchorMap
	}
	if err != nil {
		return bs.Zero, 0, errors.Wrap(err, "getting anchor map ref object")
	}
	defer r.Close()

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return bs.Zero, 0, errors.Wrap(err, "getting anchor map ref object attrs")
	}

	var ref bs.Ref
	_, err = io.ReadFull(r, ref[:])
	return ref, attrs.Generation, errors.Wrap(err, "reading anchor map ref")
}

// UpdateAnchorMap implements anchor.Store.
func (s *Store) UpdateAnchorMap(ctx context.Context, f anchor.UpdateFunc) error {
	oldRef, gen, err := s.anchorMapRef(ctx)
	if errors.Is(err, anchor.ErrNoAnchorMap) {
		oldRef = bs.Zero
	} else if err != nil {
		return err
	}

	newRef, err := f(oldRef)
	if err != nil {
		return err
	}

	obj := s.bucket.Object(anchorMapRefObjName)
	if oldRef.IsZero() {
		obj = obj.If(storage.Conditions{DoesNotExist: true})
	} else {
		obj = obj.If(storage.Conditions{GenerationMatch: gen})
	}
	w := obj.NewWriter(ctx)
	defer w.Close()

	_, err = w.Write(newRef[:])
	var e *googleapi.Error
	if errors.As(err, &e) && e.Code == http.StatusPreconditionFailed {
		return anchor.ErrUpdateConflict
	}
	return err
}

func blobObjName(ref bs.Ref) string {
	return "b:" + ref.String()
}

func refFromBlobObjName(name string) (bs.Ref, error) {
	return bs.RefFromHex(name[2:])
}

func init() {
	store.Register("gcs", func(ctx context.Context, conf map[string]interface{}) (bs.Store, error) {
		var options []option.ClientOption
		creds, ok := conf["creds"].(string)
		if !ok {
			return nil, errors.New(`missing "creds" parameter`)
		}
		bucketName, ok := conf["bucket"].(string)
		if !ok {
			return nil, errors.New(`missing "bucket" parameter`)
		}
		options = append(options, option.WithCredentialsFile(creds))
		c, err := storage.NewClient(ctx, options...)
		if err != nil {
			return nil, errors.Wrap(err, "creating cloud storage client")
		}
		return New(c.Bucket(bucketName)), nil
	})
}
