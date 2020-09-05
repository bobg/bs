// Package gcs implements a blob store on Google Cloud Storage.
package gcs

import (
	"context"
	"encoding/hex"
	stderrs "errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

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
type Store struct {
	bucket *storage.BucketHandle
}

// New produces a new Store.
func New(bucket *storage.BucketHandle) *Store {
	return &Store{bucket: bucket}
}

const typeKey = "type"

// Get gets the blob with hash `ref`.
func (s *Store) Get(ctx context.Context, ref bs.Ref) (bs.Blob, []bs.Ref, error) {
	name := blobObjName(ref)
	obj := s.bucket.Object(name)

	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "reading info of object %s", name)
	}
	defer r.Close()

	b := make([]byte, r.Attrs.Size)
	_, err = io.ReadFull(r, b)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "reading contents of object %s", name)
	}

	types, err := s.typesOfRef(ctx, ref)
	return b, types, errors.Wrapf(err, "getting types of %s", ref)
}

func (s *Store) typesOfRef(ctx context.Context, ref bs.Ref) ([]bs.Ref, error) {
	var types []bs.Ref
	iter := s.bucket.Objects(ctx, &storage.Query{Prefix: typePrefix(ref)})
	for {
		tobj, err := iter.Next()
		if stderrs.Is(err, iterator.Done) {
			return types, nil
		}
		if err != nil {
			return nil, errors.Wrapf(err, "iterating over types for %s", ref)
		}
		typRef, err := refFromTypeObjName(tobj.Name)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing type %s", tobj.Name)
		}
		types = append(types, typRef)
	}
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(ctx context.Context, b bs.Blob, typ *bs.Ref) (bs.Ref, bool, error) {
	ref, added, err := s.putBlob(ctx, b)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "storing blob")
	}

	if typ != nil {
		var (
			name = typeObjName(ref, *typ)
			obj  = s.bucket.Object(name).If(storage.Conditions{DoesNotExist: true})
			w    = obj.NewWriter(ctx)
		)
		err = w.Close()
		var e *googleapi.Error
		if stderrs.As(err, &e) && e.Code == http.StatusPreconditionFailed {
			// ok
		} else if err != nil {
			return bs.Ref{}, false, errors.Wrapf(err, "storing type info for %s", ref)
		}

		if added {
			err = anchor.Check(b, typ, func(a string, ref bs.Ref, when time.Time) error {
				var (
					name = anchorObjName(a, when)
					obj  = s.bucket.Object(name)
					w    = obj.NewWriter(ctx)
				)
				defer w.Close()

				_, err = w.Write(ref[:])
				return err
			})
			if err != nil {
				return bs.Ref{}, false, errors.Wrap(err, "writing anchor")
			}
		}
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
	if stderrs.As(err, &e) && e.Code == http.StatusPreconditionFailed {
		return ref, false, nil
	}
	return ref, true, err
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref, []bs.Ref) error) error {
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

func (s *Store) listRefs(ctx context.Context, prefix string, f func(bs.Ref, []bs.Ref) error) error {
	iter := s.bucket.Objects(ctx, &storage.Query{Prefix: "b:" + prefix})
	for {
		obj, err := iter.Next()
		if stderrs.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "iterating over blobs")
		}

		ref, err := refFromBlobObjName(obj.Name)
		if err != nil {
			return errors.Wrapf(err, "decoding obj name %s", obj.Name)
		}

		types, err := s.typesOfRef(ctx, ref)
		if err != nil {
			return errors.Wrapf(err, "getting types of %s", ref)
		}

		err = f(ref, types)
		if err != nil {
			return err
		}
	}
}

// GetAnchor gets the latest blob ref for a given anchor as of a given time.
func (s *Store) GetAnchor(ctx context.Context, a string, when time.Time) (bs.Ref, error) {
	var (
		prefix = anchorPrefix(a)
		iter   = s.bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	)

	// Anchors come back in reverse chronological order
	// (since we usually want the latest one).
	// Find the first one whose timestamp is `when` or earlier.
	for {
		attrs, err := iter.Next()
		if stderrs.Is(err, iterator.Done) {
			return bs.Ref{}, bs.ErrNotFound
		}
		if err != nil {
			return bs.Ref{}, errors.Wrap(err, "iterating over anchor objects")
		}
		_, atime, err := anchorTimeFromObjName(attrs.Name)
		if err != nil {
			return bs.Ref{}, errors.Wrapf(err, "decoding object name %s", attrs.Name)
		}
		if atime.After(when) {
			continue
		}

		ref, err := s.getAnchorRef(ctx, attrs.Name)
		return ref, errors.Wrapf(err, "reading object %s", attrs.Name)
	}
}

// ListAnchors implements anchor.Getter.
func (s *Store) ListAnchors(ctx context.Context, start string, f func(name string, ref bs.Ref, at time.Time) error) error {
	// Google Cloud Storage iterators have no API for starting in the middle of a bucket.
	// But they can filter by object-name prefix.
	// So we take (the hex encoding of) `start` and repeatedly compute prefixes for the objects we want.
	// If `start` is e67a, for example, the sequence of generated prefixes is:
	//   e67b e67c e67d e67e e67f
	//   e68 e69 e6a e6b e6c e6d e6e e6f
	//   e7 e8 e9 ea eb ec ed ee ef
	//   f
	startHex := hex.EncodeToString([]byte(start))
	return eachHexPrefix(startHex+"0", true, func(prefix string) error {
		return s.listAnchors(ctx, prefix, f)
	})
}

type anchorTuple struct {
	name string
	ref  bs.Ref
	at   time.Time
}

func (s *Store) listAnchors(ctx context.Context, prefix string, f func(name string, ref bs.Ref, at time.Time) error) error {
	iter := s.bucket.Objects(ctx, &storage.Query{Prefix: "a:" + prefix})
	var tuples []anchorTuple
	emit := func() error {
		if len(tuples) > 0 {
			for i := len(tuples) - 1; i >= 0; i-- {
				tup := tuples[i]
				err := f(tup.name, tup.ref, tup.at)
				if err != nil {
					return err
				}
			}
			tuples = nil
		}
		return nil
	}
	for {
		attrs, err := iter.Next()
		if stderrs.Is(err, iterator.Done) {
			return emit()
		}
		if err != nil {
			return err
		}
		name, at, err := anchorTimeFromObjName(attrs.Name)
		if err != nil {
			return err
		}
		if len(tuples) > 0 && name != tuples[0].name {
			err = emit()
			if err != nil {
				return err
			}
		}
		ref, err := s.getAnchorRef(ctx, attrs.Name)
		if err != nil {
			return err
		}
		tuples = append(tuples, anchorTuple{name: name, ref: ref, at: at})
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
	return "b:" + ref.String()
}

func refFromBlobObjName(name string) (bs.Ref, error) {
	return bs.RefFromHex(name[2:])
}

func (s *Store) getAnchorRef(ctx context.Context, objName string) (bs.Ref, error) {
	obj := s.bucket.Object(objName)
	r, err := obj.NewReader(ctx)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "reading info of object %s", objName)
	}
	defer r.Close()

	var ref bs.Ref
	if r.Attrs.Size != int64(len(ref)) {
		return bs.Ref{}, errors.Wrapf(err, "object %s has wrong size %d (want %d)", objName, r.Attrs.Size, len(ref))
	}

	_, err = io.ReadFull(r, ref[:])
	return ref, errors.Wrapf(err, "reading contents of object %s", objName)
}

func anchorPrefix(a string) string {
	return "a:" + hex.EncodeToString([]byte(a)) + ":"
}

func anchorObjName(a string, when time.Time) string {
	return fmt.Sprintf("%s%s", anchorPrefix(a), nanosToStr(timeToInvNanos(when)))
}

var anchorNameRegex = regexp.MustCompile(`^a:([0-9a-f]+):(\d+)$`)

func anchorTimeFromObjName(name string) (string, time.Time, error) {
	m := anchorNameRegex.FindStringSubmatch(name)
	if len(m) < 3 {
		return "", time.Time{}, errors.New("malformed name")
	}
	at := invNanosToTime(strToNanos(m[2]))
	a, err := hex.DecodeString(m[1])
	return string(a), at, errors.Wrap(err, "hex-decoding anchor")
}

func typePrefix(ref bs.Ref) string {
	return fmt.Sprintf("t:%s:", ref)
}

func typeObjName(ref, typ bs.Ref) string {
	return typePrefix(ref) + typ.String()
}

func refFromTypeObjName(name string) (bs.Ref, error) {
	parts := strings.Split(name, ":")
	if len(parts) != 3 {
		return bs.Ref{}, fmt.Errorf("got %d part(s), want 3", len(parts))
	}
	return bs.RefFromHex(parts[2])
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
