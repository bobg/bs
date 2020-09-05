// Package file implements a blob store as a file hierarchy.
package file

import (
	"context"
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

func (s *Store) typepath(ref bs.Ref) string {
	p := s.blobpath(ref)
	return p + ".types"
}

func (s *Store) anchorroot() string {
	return filepath.Join(s.root, "anchors")
}

func (s *Store) anchorpath(name string) string {
	return filepath.Join(s.anchorroot(), encodeAnchor(name))
}

// Get gets the blob with hash `ref`.
func (s *Store) Get(_ context.Context, ref bs.Ref) (bs.Blob, []bs.Ref, error) {
	path := s.blobpath(ref)
	blob, err := ioutil.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil, bs.ErrNotFound
	}
	if err != nil {
		return nil, nil, errors.Wrapf(err, "opening %s", path)
	}

	types, err := s.typesOfRef(ref)
	return blob, types, errors.Wrapf(err, "getting types for %s", ref)
}

func (s *Store) typesOfRef(ref bs.Ref) ([]bs.Ref, error) {
	var (
		types []bs.Ref
		dir   = s.typepath(ref)
	)
	typeInfos, err := ioutil.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "reading %s", dir)
	}
	for _, info := range typeInfos {
		typeRef, err := bs.RefFromHex(info.Name())
		if err != nil {
			return nil, errors.Wrapf(err, "parsing type %s for %s", info.Name(), ref)
		}
		types = append(types, typeRef)
	}
	return types, nil
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
		ref   = b.Ref()
		path  = s.blobpath(ref)
		dir   = filepath.Dir(path)
		added bool
	)

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return ref, false, errors.Wrapf(err, "ensuring path %s exists", dir)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if os.IsExist(err) {
		// ok
	} else if err != nil {
		return bs.Ref{}, false, errors.Wrapf(err, "creating %s", path)
	} else {
		defer f.Close()
		added = true

		_, err = f.Write(b)
		if err != nil {
			return bs.Ref{}, false, errors.Wrapf(err, "writing data to %s", path)
		}
	}

	if typ != nil {
		typeDir := s.typepath(ref)
		err = os.MkdirAll(typeDir, 0755)
		if os.IsExist(err) {
			// ok
		} else if err != nil {
			return bs.Ref{}, false, errors.Wrapf(err, "ensuring path %s exists", typeDir)
		}
		typeFile := filepath.Join(typeDir, typ.String())
		err = ioutil.WriteFile(typeFile, nil, 0644)
		if os.IsExist(err) {
			// ok
		} else if err != nil {
			return bs.Ref{}, false, errors.Wrapf(err, "writing type file %s", typeFile)
		}

		err = anchor.Check(b, typ, func(name string, ref bs.Ref, at time.Time) error {
			dir := s.anchorpath(name)
			err := os.MkdirAll(dir, 0755)
			if err != nil {
				return errors.Wrapf(err, "ensuring path %s exists", dir)
			}
			return ioutil.WriteFile(filepath.Join(dir, at.Format(time.RFC3339Nano)), []byte(ref.String()), 0644)
		})
		if err != nil {
			return bs.Ref{}, false, errors.Wrap(err, "adding anchor")
		}
	}

	return ref, added, nil
}

// ListAnchors implements anchor.Getter.
func (s *Store) ListAnchors(ctx context.Context, start string, f func(string, bs.Ref, time.Time) error) error {
	infos, err := ioutil.ReadDir(s.anchorroot())
	if err != nil {
		return errors.Wrap(err, "reading anchor root")
	}

	var names []string
	for _, info := range infos {
		if !info.IsDir() {
			continue
		}
		name, err := decodeAnchor(info.Name())
		if err != nil {
			return errors.Wrapf(err, "decoding anchor %s", info.Name())
		}
		if name <= start {
			continue
		}
		names = append(names, name)
	}

	sort.StringSlice(names).Sort()

	for _, name := range names {
		dir := s.anchorpath(name)
		aInfos, err := ioutil.ReadDir(dir)
		if err != nil {
			return errors.Wrapf(err, "reading dir %s", dir)
		}
		for _, aInfo := range aInfos {
			if err = ctx.Err(); err != nil {
				return err
			}
			at, err := time.Parse(time.RFC3339Nano, aInfo.Name())
			if err != nil {
				return errors.Wrapf(err, "parsing filename %s in %s", aInfo.Name(), dir)
			}
			refHex, err := ioutil.ReadFile(filepath.Join(dir, aInfo.Name()))
			if err != nil {
				return errors.Wrapf(err, "reading %s/%s", dir, aInfo.Name())
			}
			ref, err := bs.RefFromHex(string(refHex))
			if err != nil {
				return errors.Wrapf(err, "parsing ref from string %s", refHex)
			}
			err = f(name, ref, at)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ListRefs produces all blob refs in the store, in lexicographic order.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref, []bs.Ref) error) error {
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

				types, err := s.typesOfRef(ref)
				if err != nil {
					return errors.Wrapf(err, "getting types for %s", ref)
				}

				err = f(ref, types)
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
