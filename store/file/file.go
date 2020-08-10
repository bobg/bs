// Package file implements a blob store as a file hierarchy.
package file

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"hash/adler32"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/store"
)

var _ anchor.Store = &Store{}

// Store is a file-based implementation of a blob store.
type Store struct {
	root string
}

// New produces a new Store storing data beneath `root`.
func New(root string) *Store {
	return &Store{root: root}
}

func (s *Store) blobroot() string {
	return filepath.Join(s.root, "blobs")
}

func (s *Store) blobpath(ref bs.Ref) string {
	h := ref.String()
	return filepath.Join(s.blobroot(), h[:2], h[:4], h)
}

func (s *Store) anchorroot() string {
	return filepath.Join(s.root, "anchors")
}

func (s *Store) anchorpath(name string) string {
	sum := adler32.Checksum([]byte(name))
	var sumbytes [4]byte
	binary.BigEndian.PutUint32(sumbytes[:], sum)
	sumhex := hex.EncodeToString(sumbytes[:])
	return filepath.Join(s.anchorroot(), sumhex[:2], sumhex, encodeAnchor(name))
}

// Get gets the blob with hash `ref`.
func (s *Store) Get(_ context.Context, ref bs.Ref) (bs.Blob, bs.Ref, error) {
	var (
		typ  bs.Ref
		path = s.blobpath(ref)
	)
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, bs.Ref{}, bs.ErrNotFound
	}
	if err != nil {
		return nil, bs.Ref{}, errors.Wrapf(err, "opening %s", path)
	}
	defer f.Close()

	_, err = io.ReadFull(f, typ[:])
	if err != nil {
		return nil, bs.Ref{}, errors.Wrapf(err, "reading type from %s", path)
	}

	b, err := ioutil.ReadAll(f)
	return b, typ, errors.Wrapf(err, "reading data from %s", path)
}

// GetAnchor gets the latest blob ref for a given anchor as of a given time.
func (s *Store) GetAnchor(ctx context.Context, name string, at time.Time) (bs.Ref, error) {
	dir := s.anchorpath(name)
	entries, err := ioutil.ReadDir(dir)
	if os.IsNotExist(err) {
		return bs.Ref{}, bs.ErrNotFound
	}
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "reading dir %s", dir)
	}

	// We might use sort.Search here (since ReadDir returns entries sorted by name),
	// which is O(log N),
	// but we want to be robust in the face of filenames that time.Parse fails to parse,
	// so O(N) it is.
	// Oh, also: filenames are times that may be expressed in different timezones.
	var best string
	for _, entry := range entries {
		name := entry.Name()
		parsed, err := time.Parse(time.RFC3339Nano, name)
		if err != nil {
			continue
		}
		if parsed.After(at) {
			break
		}
		best = name
	}
	if best == "" {
		return bs.Ref{}, bs.ErrNotFound
	}

	h, err := ioutil.ReadFile(filepath.Join(dir, best))
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "reading file %s/%s", dir, best)
	}

	ref, err := bs.RefFromHex(string(h))
	return ref, errors.Wrapf(err, "parsing ref %s", string(h))
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(_ context.Context, b bs.Blob, typ *bs.Ref) (bs.Ref, bool, error) {
	var (
		ref    = b.Ref()
		path   = s.blobpath(ref)
		dir    = filepath.Dir(path)
		typref bs.Ref
	)

	if typ != nil {
		typref = *typ
	}

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return ref, false, errors.Wrapf(err, "ensuring path %s exists", dir)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0444)
	if os.IsExist(err) {
		return ref, false, nil
	}
	if err != nil {
		return ref, true, errors.Wrapf(err, "creating %s", path)
	}
	defer f.Close()

	_, err = f.Write(typref[:])
	if err != nil {
		return ref, true, errors.Wrapf(err, "writing type to %s", path)
	}

	_, err = f.Write(b)
	if err != nil {
		return ref, true, errors.Wrapf(err, "writing data to %s", path)
	}

	err = anchor.Check(b, typ, func(name string, ref bs.Ref, at time.Time) error {
		dir := s.anchorpath(name)
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return errors.Wrapf(err, "ensuring path %s exists", dir)
		}
		return ioutil.WriteFile(filepath.Join(dir, at.Format(time.RFC3339Nano)), []byte(ref.String()), 0444)
	})

	return ref, true, errors.Wrap(err, "storing anchor")
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(r, typ bs.Ref) error) error {
	topLevel, err := ioutil.ReadDir(s.blobroot())
	if err != nil {
		return errors.Wrapf(err, "reading dir %s", s.blobroot())
	}

	startHex := start.String()
	topIndex := sort.Search(len(topLevel), func(n int) bool {
		return topLevel[n].Name() >= startHex[:2]
	})
	for i := topIndex; i < len(topLevel); i++ {
		topInfo := topLevel[i]
		if !topInfo.IsDir() {
			continue
		}
		topName := topInfo.Name()
		if len(topName) != 2 {
			continue
		}
		if _, err = strconv.ParseInt(topName, 16, 64); err != nil {
			continue
		}

		midLevel, err := ioutil.ReadDir(filepath.Join(s.blobroot(), topName))
		if err != nil {
			return errors.Wrapf(err, "reading dir %s/%s", s.blobroot(), topName)
		}
		midIndex := sort.Search(len(midLevel), func(n int) bool {
			return midLevel[n].Name() >= startHex[:4]
		})
		for j := midIndex; j < len(midLevel); j++ {
			midInfo := midLevel[j]
			if !midInfo.IsDir() {
				continue
			}
			midName := midInfo.Name()
			if len(midName) != 4 {
				continue
			}
			if _, err = strconv.ParseInt(midName, 16, 64); err != nil {
				continue
			}

			blobInfos, err := ioutil.ReadDir(filepath.Join(s.blobroot(), topName, midName))
			if err != nil {
				return errors.Wrapf(err, "reading dir %s/%s/%s", s.blobroot(), topName, midName)
			}

			index := sort.Search(len(blobInfos), func(n int) bool {
				return blobInfos[n].Name() > startHex
			})
			for k := index; k < len(blobInfos); k++ {
				blobInfo := blobInfos[k]
				if blobInfo.IsDir() {
					continue
				}

				ref, err := bs.RefFromHex(blobInfo.Name())
				if err != nil {
					continue
				}

				var typ bs.Ref
				file, err := os.Open(s.blobpath(ref))
				if err != nil {
					return errors.Wrapf(err, "opening %s", s.blobpath(ref))
				}
				err = func() error {
					defer file.Close()
					_, err := io.ReadFull(file, typ[:])
					return errors.Wrapf(err, "reading type from %s", s.blobpath(ref))
				}()
				if err != nil {
					return err
				}

				err = f(ref, typ)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func encodeAnchor(name string) string {
	return url.PathEscape(name)
}

func decodeAnchor(inp string) (string, error) {
	return url.PathUnescape(inp)
}

func init() {
	store.Register("file", func(_ context.Context, conf map[string]interface{}) (bs.Store, error) {
		root, ok := conf["root"].(string)
		if !ok {
			return nil, errors.New(`missing "root" parameter`)
		}
		return New(root), nil
	})
}
