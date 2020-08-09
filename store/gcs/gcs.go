// Package gcs implements a blob store on Google Cloud Storage.
package gcs

import (
	"context"
	stderrs "errors"
	"io"
	"net/http"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store"
)

var _ bs.Store = &Store{}

// Store is a Google Cloud Storage-based implementation of a blob store.
type Store struct {
	bucket *storage.BucketHandle
}

// New produces a new Store.
func New(bucket *storage.BucketHandle) *Store {
	return &Store{bucket: bucket}
}

const typeKey = "type"

// Get gets the blob with hash `ref`.
func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, bs.Ref, error) {
	name := blobObjName(ref)
	obj := s.bucket.Object(name)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, bs.Ref{}, errors.Wrapf(err, "getting object attrs for %s", name)
	}

	var typ bs.Ref
	if typHex, ok := attrs.Metadata[typeKey]; ok {
		err = typ.FromHex(typHex)
		if err != nil {
			return nil, bs.Ref{}, errors.Wrapf(err, "decoding type ref (%s) for %s", typHex, name)
		}
	}

	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, bs.Ref{}, errors.Wrapf(err, "reading info of object %s", name)
	}

	b := make([]byte, r.Attrs.Size)
	err = func() error {
		defer r.Close()

		_, err := io.ReadFull(r, b)
		return errors.Wrapf(err, "reading contents of object %s", name)
	}()
	return b, typ, err
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(ctx context.Context, b bs.Blob, typ *bs.Ref) (bs.Ref, bool, error) {
	var (
		ref   = b.Ref()
		name  = blobObjName(ref)
		obj   = s.bucket.Object(name).If(storage.Conditions{DoesNotExist: true})
		w     = obj.NewWriter(ctx)
		added bool
	)
	err := func() error {
		defer w.Close()

		if typ != nil {
			w.Metadata = map[string]string{typeKey: typ.String()}
		}

		_, err := w.Write(b) // TODO: are partial writes a possibility?
		var e *googleapi.Error
		if stderrs.As(err, &e) && e.Code == http.StatusPreconditionFailed {
			return nil
		}
		if err == nil { // sic
			added = true
		}
		return errors.Wrapf(err, "writing object %s", name)
	}()
	return ref, added, err
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(r, typ bs.Ref) error) error {
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

func (s *Store) listRefs(ctx context.Context, prefix string, f func(r, typ bs.Ref) error) error {
	iter := s.bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		obj, err := iter.Next()
		if stderrs.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return err
		}
		ref, err := bs.RefFromHex(obj.Name)
		if err != nil {
			return err
		}

		var typ bs.Ref
		if typHex, ok := obj.Metadata[typeKey]; ok {
			err = typ.FromHex(typHex)
			if err != nil {
				return errors.Wrapf(err, "decoding type ref (%s) for %s", typHex, obj.Name)
			}
		}

		err = f(ref, typ)
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

func blobObjName(ref bs.Ref) string {
	return ref.String()
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
