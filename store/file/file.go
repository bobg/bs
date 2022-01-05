// Package file implements a blob store as a file hierarchy.
package file

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/bobg/flock"
	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/store"
)

var _ anchor.Store = &Store{}

// Store is a file-based implementation of a blob store.
type Store struct {
	root    string
	flocker flock.Locker
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

// Get gets the blob with hash `ref`.
func (s *Store) Get(_ context.Context, ref bs.Ref) (bs.Blob, error) {
	path := s.blobpath(ref)
	blob, err := ioutil.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, bs.ErrNotFound
	}
	return blob, errors.Wrapf(err, "opening %s", path)
}

// Put adds a blob to the store if it wasn't already present.
func (s *Store) Put(_ context.Context, b bs.Blob) (bs.Ref, bool, error) {
	var (
		ref  = b.Ref()
		path = s.blobpath(ref)
		dir  = filepath.Dir(path)
	)

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return ref, false, errors.Wrapf(err, "ensuring path %s exists", dir)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if os.IsExist(err) {
		return ref, false, nil
	}
	if err != nil {
		return bs.Zero, false, errors.Wrapf(err, "creating %s", path)
	}
	defer f.Close()

	_, err = f.Write(b)
	if err != nil {
		return bs.Zero, false, errors.Wrapf(err, "writing data to %s", path)
	}

	return ref, true, nil
}

func (s *Store) PutType(ctx context.Context, ref bs.Ref, typ []byte) error {
	return anchor.PutType(ctx, s, ref, typ)
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
	err := os.MkdirAll(s.blobroot(), 0755)
	if os.IsExist(err) {
		// ok
	} else if err != nil {
		return errors.Wrapf(err, "ensuring %s exists", s.blobroot())
	}

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

				err = f(ref)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

const anchorMapRefFileBaseName = "anchormapref"

func (s *Store) anchorMapRefFilePath() string {
	return filepath.Join(s.root, anchorMapRefFileBaseName)
}

func (s *Store) lockAnchorMapRef() error {
	return s.flocker.Lock(s.anchorMapRefFilePath())
}

func (s *Store) unlockAnchorMapRef() error {
	return s.flocker.Unlock(s.anchorMapRefFilePath())
}

// AnchorMapRef implements anchor.Getter.
func (s *Store) AnchorMapRef(ctx context.Context) (bs.Ref, error) {
	err := s.lockAnchorMapRef()
	if err != nil {
		return bs.Zero, errors.Wrap(err, "locking anchor map ref file")
	}
	defer s.unlockAnchorMapRef()

	return s.anchorMapRef(ctx)
}

// File lock must be held.
func (s *Store) anchorMapRef(ctx context.Context) (bs.Ref, error) {
	b, err := os.ReadFile(s.anchorMapRefFilePath())
	if errors.Is(err, os.ErrNotExist) {
		return bs.Zero, anchor.ErrNoAnchorMap
	}
	if err != nil {
		return bs.Zero, errors.Wrap(err, "reading anchor map ref")
	}
	return bs.RefFromBytes(b), nil
}

// UpdateAnchorMap implements anchor.Store.
func (s *Store) UpdateAnchorMap(ctx context.Context, f anchor.UpdateFunc) error {
	oldRef, err := s.AnchorMapRef(ctx)
	if errors.Is(err, anchor.ErrNoAnchorMap) {
		oldRef = bs.Zero
	} else if err != nil {
		return errors.Wrap(err, "getting old anchor map ref")
	}

	newRef, err := f(oldRef)
	if err != nil {
		return err
	}

	err = s.lockAnchorMapRef()
	if err != nil {
		return errors.Wrap(err, "relocking anchor map ref file")
	}
	defer s.unlockAnchorMapRef()

	if oldRef.IsZero() {
		f, err := os.OpenFile(s.anchorMapRefFilePath(), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			return errors.Wrap(err, "creating anchor map ref file")
		}
		defer f.Close()
		_, err = f.Write(newRef[:])
		return errors.Wrap(err, "writing anchor map ref file")
	}

	return os.WriteFile(s.anchorMapRefFilePath(), newRef[:], 0644)
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
