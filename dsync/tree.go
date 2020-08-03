package dsync

import (
	"context"
	stderrs "errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/split"
)

// Tree represents a synchronizable tree of files and directories.
type Tree struct {
	// S is the blob store where a representation of the file tree is stored.
	S bs.Store

	// Root is the path to the root of the file tree.
	Root string
}

// Ingest adds dir and all of its children to Tree.
// Return value is the ref of the resulting Dir blob.
func (t *Tree) Ingest(ctx context.Context, dir string) (bs.Ref, error) {
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "reading dir %s", dir)
	}

	// Ingest everything under this dir before writing its anchor.
	// Hopefully the replica receives things in roughly the same order.
	// If it receives things too badly out of order,
	// it might have to create a directory entry for a file that doesn't exist yet.

	dirents := make(map[string]*Dirent)

	for _, info := range infos {
		if isIgnoreInfo(info) {
			continue
		}
		dirent := &Dirent{
			Name: info.Name(),
			Mode: uint32(info.Mode()),
		}
		dirents[info.Name()] = dirent
		if info.IsDir() {
			subref, err := t.Ingest(ctx, filepath.Join(dir, info.Name()))
			if err != nil {
				return bs.Ref{}, errors.Wrapf(err, "ingesting dir %s/%s", dir, info.Name())
			}
			dirent.Ref = subref[:]
			continue
		}
		subref, err := t.ingestFile(ctx, filepath.Join(dir, info.Name()))
		if err != nil {
			return bs.Ref{}, errors.Wrapf(err, "ingesting file %s/%s", dir, info.Name())
		}
		dirent.Ref = subref[:]
	}

	da, err := t.dirAnchor(dir)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "computing anchor for dir %s", dir)
	}
	dp, err := t.infosToDirProto(ctx, dir, infos, dirents)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "turning FileInfos for %s into Dir proto", dir)
	}
	dirRef, _, err := bs.PutProto(ctx, t.S, dp)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "storing blob for dir %s", dir)
	}
	err = t.S.PutAnchor(ctx, dirRef, da, time.Now())
	return dirRef, errors.Wrapf(err, "storing anchor for dir %s", dir)
}

func (t *Tree) infosToDirProto(ctx context.Context, dir string, infos []os.FileInfo, dirents map[string]*Dirent) (*Dir, error) {
	if dirents == nil {
		dirents = map[string]*Dirent{}
	}

	dp := new(Dir)
	now := time.Now()
	for _, info := range infos {
		if isIgnoreInfo(info) {
			continue
		}

		dirent, ok := dirents[info.Name()]
		if !ok {
			var (
				a   bs.Anchor
				err error
			)
			if info.Mode().IsDir() {
				a, err = t.dirAnchor(filepath.Join(dir, info.Name()))
				if err != nil {
					return nil, errors.Wrapf(err, "computing dir anchor for %s/%s", dir, info.Name())
				}
			} else {
				a, err = t.fileAnchor(filepath.Join(dir, info.Name()))
				if err != nil {
					return nil, errors.Wrapf(err, "computing file anchor for %s/%s", dir, info.Name())
				}
			}
			ref, _, err := t.S.GetAnchor(ctx, a, now)
			if err != nil {
				return nil, errors.Wrapf(err, "getting ref for %s at %s", a, now)
			}

			dirent = &Dirent{
				Name: info.Name(),
				Mode: uint32(info.Mode()),
				Ref:  ref[:],
			}
		}
		dp.Entries = append(dp.Entries, dirent)
	}
	// No need to sort; ReadDir returns entries already sorted by name.
	return dp, nil
}

func isIgnoreInfo(info os.FileInfo) bool {
	if info.IsDir() {
		switch info.Name() {
		case ".", "..", ".git":
			return true
		}
		return false
	}
	return !info.Mode().IsRegular()
}

func (t *Tree) ingestFile(ctx context.Context, fpath string) (bs.Ref, error) {
	f, err := os.Open(fpath)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "opening %s for reading", fpath)
	}
	defer f.Close()

	ref, err := split.Write(ctx, t.S, f, nil)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "storing blobs for file %s", fpath)
	}

	fa, err := t.fileAnchor(fpath)
	if err != nil {
		return bs.Ref{}, errors.Wrapf(err, "computing anchor for file %s", fpath)
	}
	err = t.S.PutAnchor(ctx, ref, fa, time.Now())
	return ref, errors.Wrapf(err, "storing anchor for file %s", fpath)
}

func (t *Tree) fileAnchor(file string) (bs.Anchor, error) {
	rel, err := filepath.Rel(t.Root, file)
	return bs.Anchor(rel), err
}

func (t *Tree) dirAnchor(dir string) (bs.Anchor, error) {
	rel, err := filepath.Rel(t.Root, dir)
	return bs.Anchor(rel + "/"), err
}

// FileChanged causes file to be re-added to Tree.
// If this results in any changes,
// or if file is new or does not exist
// (perhaps it existed before and has been removed),
// then its containing directory is re-added with DirChanged.
func (t *Tree) FileChanged(ctx context.Context, file string) error {
	info, err := os.Lstat(file)
	if os.IsNotExist(err) {
		// Perhaps file was removed, which means its containing dir has changed.
		return t.DirChanged(ctx, filepath.Dir(file))
	}

	if info.IsDir() {
		return t.DirChanged(ctx, file)
	}
	if !info.Mode().IsRegular() {
		// Ignore non-regular files.
		return nil
	}

	fa, err := t.fileAnchor(file)
	if err != nil {
		return errors.Wrapf(err, "computing anchor for file %s", file)
	}

	var doParent bool

	oldRef, _, err := t.S.GetAnchor(ctx, fa, time.Now())
	if stderrs.Is(err, bs.ErrNotFound) {
		// Perhaps file was added, which means its dir has (also) changed.
		doParent = true
	} else if err != nil {
		return errors.Wrapf(err, "getting anchor for file %s", file)
	}

	f, err := os.Open(file)
	if err != nil {
		return errors.Wrapf(err, "opening %s for reading", file)
	}
	defer f.Close()

	newRef, err := split.Write(ctx, t.S, f, nil)
	if err != nil {
		return errors.Wrapf(err, "storing blobtree for file %s", file)
	}

	if oldRef != newRef {
		err = t.S.PutAnchor(ctx, newRef, fa, time.Now())
		if err != nil {
			return errors.Wrapf(err, "updating anchor for file %s", file)
		}
	}

	if doParent {
		because := &Dirent{
			Name: filepath.Base(file),
			Mode: uint32(info.Mode()),
			Ref:  newRef[:],
		}
		err = t.dirChanged(ctx, filepath.Dir(file), map[string]*Dirent{info.Name(): because})
		if err != nil {
			return errors.Wrapf(err, "computing parent-dir changes from possibly new file %s", file)
		}
	}

	return nil
}

// DirChanged causes dir to be re-added to Tree.
// If this results in any differences,
// or if dir does not exist
// (perhaps it existed before and has been removed),
// dir's parent is recursively re-added
// (up to and including t.Root but not beyond).
func (t *Tree) DirChanged(ctx context.Context, dir string) error {
	return t.dirChanged(ctx, dir, nil)
}

func (t *Tree) dirChanged(ctx context.Context, dir string, because map[string]*Dirent) error {
	if len(t.Root) > len(dir) {
		// Dir is higher than root; ignore.
		return nil
	}

	da, err := t.dirAnchor(dir)
	if err != nil {
		return errors.Wrapf(err, "computing anchor for dir %s", dir)
	}

	var doParent bool

	oldRef, _, err := t.S.GetAnchor(ctx, da, time.Now())
	if stderrs.Is(err, bs.ErrNotFound) {
		// Perhaps dir was added, which means its containing dir has (also) changed.
		log.Printf("GetAnchor(%s) -> empty", da)
		doParent = true
	} else if err != nil {
		return errors.Wrapf(err, "getting anchor for dir %s", dir)
	}

	infos, err := ioutil.ReadDir(dir)
	if os.IsNotExist(err) {
		// Perhaps dir was removed, which means its containing dir has changed.
		doParent = true
	} else if err != nil {
		return errors.Wrapf(err, "reading dir %s", dir)
	}

	dp, err := t.infosToDirProto(ctx, dir, infos, because)
	if err != nil {
		return errors.Wrapf(err, "turning infos for %s into Dir proto", dir)
	}

	newRef, _, err := bs.PutProto(ctx, t.S, dp)
	if err != nil {
		return errors.Wrapf(err, "storing blob for dir %s", dir)
	}

	if newRef != oldRef {
		err = t.S.PutAnchor(ctx, newRef, da, time.Now())
		if err != nil {
			return errors.Wrapf(err, "updating anchor for dir %s", dir)
		}
		doParent = true
	}

	if doParent {
		err = t.dirChanged(ctx, filepath.Dir(dir), nil)
		if err != nil {
			return errors.Wrapf(err, "recording change of %s in %s", filepath.Base(dir), filepath.Dir(dir))
		}
	}

	return nil
}
