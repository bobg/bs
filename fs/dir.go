// Package fs implements blob store structures for representing files and directories.
package fs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
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
func (d *Dir) Each(ctx context.Context, g bs.Getter, f func(name string, dirent *Dirent) error) error {
	return (*schema.Map)(d).Each(ctx, g, func(pair *schema.MapPair) error {
		var dirent Dirent
		err := proto.Unmarshal(pair.Payload, &dirent)
		if err != nil {
			return err
		}
		return f(string(pair.Key), &dirent)
	})
}

func (d *Dir) Set(ctx context.Context, store bs.Store, name string, dirent *Dirent) (bs.Ref, schema.Outcome, error) {
	direntBytes, err := proto.Marshal(dirent)
	if err != nil {
		return bs.Ref{}, schema.ONone, errors.Wrapf(err, "marshaling dirent for %s", name)
	}
	return (*schema.Map)(d).Set(ctx, store, []byte(name), direntBytes)
}

// Dirent finds the entry in d with the given name.
// It returns nil if no such entry exists.
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

func (d *Dir) Find(ctx context.Context, g anchor.Getter, path string, at time.Time) (*Dirent, error) {
	path = strings.TrimLeft(path, "/")
	var name string
	path, name = filepath.Split(path)
	if path != "" {
		dirent, err := d.Find(ctx, g, path, at)
		if err != nil {
			return nil, errors.Wrapf(err, "finding %s", path)
		}
		d, err = dirent.Dir(ctx, g, at)
		if err != nil {
			return nil, errors.Wrapf(err, "resolving dir %s", path)
		}
	}
	return d.Dirent(ctx, g, name)
}

type devInoPair struct {
	dev, ino uint64
}

// Add adds the file, symlink, or dir at path to d.
// If path is a dir,
// this is recursive.
// It returns the possibly-updated Ref for d.
func (d *Dir) Add(ctx context.Context, store anchor.Store, path string, at time.Time) (bs.Ref, error) {
	return d.add(ctx, store, path, at, map[devInoPair]string{})
}

func (d *Dir) add(ctx context.Context, store anchor.Store, path string, at time.Time, seen map[devInoPair]string) (bs.Ref, error) {
	log.Printf("enter add %s", path)
	defer log.Printf("leave add %s", path)

	info, err := os.Lstat(path)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "statting %s", path)
	}

	var (
		name = info.Name()
		mode = uint32(info.Mode())
	)

	if info.IsDir() {
		entry, err := d.Dirent(ctx, store, name)
		if err != nil {
			return bs.Ref{}, errors.Wrapf(err, "looking up entry %s", name)
		}

		var (
			subdir *Dir
			a      string
			isNew  bool
		)
		if entry == nil || !entry.IsDir() {
			subdir = NewDir()
			a = newAnchor()
			isNew = true
		} else {
			subdir, err = entry.Dir(ctx, store, at)
			if err != nil {
				return bs.Ref{}, errors.Wrapf(err, "resolving subdir %s", name)
			}
			a = entry.Item
		}

		sref, err := subdir.addDir(ctx, store, path, at, seen)
		if err != nil {
			return bs.Ref{}, errors.Wrapf(err, "adding subdir contents at %s", path)
		}

		_, _, err = anchor.Put(ctx, store, a, sref, at)
		if isNew {
			dref, _, err := d.Set(ctx, store, name, &Dirent{
				Mode: mode,
				Item: a,
			})
			return dref, errors.Wrapf(err, "adding subdir %s to dir", name)
		}

		// Return unchanged self ref.
		return d.Ref()
	}

	if (mode & uint32(os.ModeSymlink)) == uint32(os.ModeSymlink) {
		target, err := os.Readlink(path)
		if err != nil {
			return bs.Ref{}, errors.Wrapf(err, "reading symlink %s", path)
		}
		dref, _, err := d.Set(ctx, store, name, &Dirent{
			Mode: mode,
			Item: target,
		})
		return dref, errors.Wrapf(err, "adding symlink %s to dir", name)
	}

	if (mode & uint32(os.ModeType)) != 0 {
		return bs.Ref{}, errors.Wrapf(err, "unsupported file type 0%o for %s", mode&uint32(os.ModeType), path)
	}

	var diPair devInoPair
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		dev, ino := st.Dev, st.Ino
		diPair = devInoPair{dev: dev, ino: ino}
		if a, ok := seen[diPair]; ok {
			dref, _, err := d.Set(ctx, store, name, &Dirent{
				Mode: mode,
				Item: a,
			})
			return dref, errors.Wrapf(err, "adding hard link named %s (anchor %s) to dir", name, a)
		}
	}

	f, err := os.Open(path)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "opening %s", path)
	}
	defer f.Close()

	fref, err := split.Write(ctx, store, f, nil)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "split-writing to store from %s", path)
	}

	a := newAnchor()
	_, _, err = anchor.Put(ctx, store, a, fref, at)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "storing anchor %s for file at %s", a, fref)
	}

	if diPair != (devInoPair{}) {
		seen[diPair] = a
	}

	dref, _, err := d.Set(ctx, store, name, &Dirent{
		Mode: mode,
		Item: a,
	})
	return dref, errors.Wrapf(err, "adding file %s (anchor %s) to dir", name, a)
}

// AddDir adds the members of the directory at path to d, recursively.
func (d *Dir) AddDir(ctx context.Context, store anchor.Store, path string, at time.Time) (bs.Ref, error) {
	return d.addDir(ctx, store, path, at, map[devInoPair]string{})
}

func (d *Dir) addDir(ctx context.Context, store anchor.Store, path string, at time.Time, seen map[devInoPair]string) (bs.Ref, error) {
	infos, err := ioutil.ReadDir(path)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "reading dir %s", path)
	}

	if len(infos) == 0 {
		// Return unchanged self ref.
		return d.Ref()
	}

	var dref bs.Ref
	for _, info := range infos {
		dref, err = d.add(ctx, store, filepath.Join(path, info.Name()), at, seen)
		if err != nil {
			return bs.Ref{}, errors.Wrapf(err, "adding %s/%s", path, info.Name())
		}
	}

	return dref, nil
}

func (d *Dir) Ref() (bs.Ref, error) {
	return bs.ProtoRef((*schema.Map)(d))
}

func newAnchor() string {
	var buf [32]byte
	rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}
