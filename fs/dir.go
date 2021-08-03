// Package fs implements blob store structures for representing files and directories.
package fs

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/bobg/bs"
	"github.com/bobg/bs/schema"
	"github.com/bobg/bs/split"
)

// Dir is a directory of files, symlinks, and subdirs.
// It is implemented as a schema.Map,
// with each key the name of the entry
// and each payload a serialized Dirent.
type Dir schema.Map

// NewDir produces a new, blank Dir,
// not yet written to a blob store.
func NewDir() *Dir {
	return (*Dir)(schema.NewMap())
}

// Load loads the directory at ref into d.
func (d *Dir) Load(ctx context.Context, g bs.Getter, ref bs.Ref) error {
	return bs.GetProto(ctx, g, ref, (*schema.Map)(d))
}

// Each calls a callback on each name/Dirent pair in d
// (in an indeterminate order).
// If the callback returns an error,
// Each exits with that error.
func (d *Dir) Each(ctx context.Context, g bs.Getter, f func(string, *Dirent) error) error {
	return (*schema.Map)(d).Each(ctx, g, func(pair *schema.MapPair) error {
		var dirent Dirent
		err := proto.Unmarshal(pair.Payload, &dirent)
		if err != nil {
			return err
		}
		return f(string(pair.Key), &dirent)
	})
}

// Set sets the given name in d to the given Dirent.
// It returns the possibly updated Ref of d
// and the schema.Outcome resulting from the underlying "bs/schema".Map.Set call.
func (d *Dir) Set(ctx context.Context, store bs.Store, name string, dirent *Dirent) (bs.Ref, schema.Outcome, error) {
	direntBytes, err := proto.Marshal(dirent)
	if err != nil {
		return bs.Ref{}, schema.ONone, errors.Wrapf(err, "marshaling dirent for %s", name)
	}
	return (*schema.Map)(d).Set(ctx, store, []byte(name), direntBytes)
}

// Dirent finds the entry in d with the given name.
// It returns nil if no such entry exists.
// It does not traverse subdirs,
// and name must not be a multi-segment path.
// For that,
// use Find.
// Note: Dirent does not understand "." and "..".
func (d *Dir) Dirent(ctx context.Context, g bs.Getter, name string) (*Dirent, error) {
	dbytes, ok, err := (*schema.Map)(d).Lookup(ctx, g, []byte(name))
	if err != nil {
		return nil, errors.Wrapf(err, "looking up %s", name)
	}
	if !ok {
		return nil, nil
	}
	var dirent Dirent
	err = proto.Unmarshal(dbytes, &dirent)
	return &dirent, errors.Wrapf(err, "unmarshaling dirent at %s", name)
}

// Find resolves a path starting at d,
// traversing into subdirs as appropriate,
// and returning the entry found at the end of path.
// It does not resolve symlinks in its traversal.
// Leading path separators in `path` are ignored;
// traversal is always relative to d.
// Note: Find does not understand "." and "..".
func (d *Dir) Find(ctx context.Context, g bs.Getter, path string) (*Dirent, error) {
	path = strings.TrimLeft(path, "/")
	var name string
	path, name = filepath.Split(path)
	if path != "" {
		dirent, err := d.Find(ctx, g, path)
		if err != nil {
			return nil, errors.Wrapf(err, "finding %s", path)
		}
		d, err = dirent.Dir(ctx, g)
		if err != nil {
			return nil, errors.Wrapf(err, "resolving dir %s", path)
		}
	}
	return d.Dirent(ctx, g, name)
}

// Add adds the file, symlink, or dir at path to d.
// If path is a dir,
// this is recursive.
// It returns the possibly-updated Ref for d.
func (d *Dir) Add(ctx context.Context, store bs.Store, path string) (bs.Ref, error) {
	dirent, err := add(ctx, store, path)
	if err != nil {
		return bs.Ref{}, err
	}
	ref, _, err := d.Set(ctx, store, filepath.Base(path), dirent)
	return ref, err
}

func add(ctx context.Context, store bs.Store, path string) (*Dirent, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, errors.Wrapf(err, "statting %s", path)
	}

	mode := uint32(info.Mode())
	dirent := &Dirent{Mode: mode}

	if dirent.IsLink() {
		target, err := os.Readlink(path)
		if err != nil {
			return nil, errors.Wrapf(err, "reading symlink %s", path)
		}
		dirent.Item = target
		return dirent, nil
	}

	if dirent.IsDir() {
		m := make(map[string][]byte)
		err = addDirToMap(ctx, store, path, m)
		if err != nil {
			return nil, errors.Wrap(err, "populating dir map")
		}
		_, ref, err := schema.MapFromGo(ctx, store, m)
		if err != nil {
			return nil, errors.Wrap(err, "storing dir map")
		}
		dirent.Item = ref.String()
		return dirent, nil
	}

	if (mode & uint32(os.ModeType)) != 0 {
		return nil, errors.Wrapf(err, "unsupported file type 0%o for %s", mode&uint32(os.ModeType), path)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "opening file %s", path)
	}
	defer f.Close()

	w := split.NewWriter(ctx, store)
	_, err = io.Copy(w, f)
	if err != nil {
		return nil, errors.Wrapf(err, "split-writing to store from %s", path)
	}
	err = w.Close()
	if err != nil {
		return nil, errors.Wrapf(err, "finishing split-writing to store from %s", path)
	}

	dirent.Item = w.Root.String()
	return dirent, nil
}

// AddDir adds the members of the directory at path to d, recursively.
func (d *Dir) AddDir(ctx context.Context, store bs.Store, path string) (bs.Ref, error) {
	m := make(map[string][]byte)
	err := (*schema.Map)(d).Each(ctx, store, func(pair *schema.MapPair) error {
		m[string(pair.Key)] = pair.Payload
		return nil
	})
	if err != nil {
		return bs.Ref{}, errors.Wrap(err, "iterating over existing dir")
	}

	err = addDirToMap(ctx, store, path, m)
	if err != nil {
		return bs.Ref{}, errors.Wrap(err, "populating dir map")
	}

	_, ref, err := schema.MapFromGo(ctx, store, m)
	return ref, errors.Wrap(err, "storing dir map")
}

func addDirToMap(ctx context.Context, store bs.Store, path string, m map[string][]byte) error {
	infos, err := ioutil.ReadDir(path)
	if err != nil {
		return errors.Wrapf(err, "reading dir %s", path)
	}
	for _, info := range infos {
		name := info.Name()
		dirent, err := add(ctx, store, filepath.Join(path, name))
		if err != nil {
			return errors.Wrapf(err, "adding %s/%s", path, name)
		}
		direntBytes, err := proto.Marshal(dirent)
		if err != nil {
			return errors.Wrapf(err, "marshaling dirent for %s/%s", path, name)
		}
		m[name] = direntBytes
	}
	return nil
}

// Ref returns d's Ref.
func (d *Dir) Ref() (bs.Ref, error) {
	return bs.ProtoRef((*schema.Map)(d))
}
