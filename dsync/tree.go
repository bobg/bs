package dsync

import (
	"context"
	stderrs "errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
)

type Tree struct {
	S    bs.Store
	Root string
}

func (t *Tree) Ingest(ctx context.Context, dir string) error {
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		return errors.Wrapf(err, "reading dir %s", dir)
	}

	dp := infosToDirProto(infos)

	dirRef, _, err := bs.PutProto(ctx, t.S, dp)
	if err != nil {
		return errors.Wrapf(err, "storing blob for dir %s", dir)
	}

	// Ingest everything under this dir before writing its anchor.
	// Hopefully the replica receives things in roughly the same order.
	// If it receives things too badly out of order,
	// it might have to create a directory entry for a file that doesn't exist yet.

	for _, entry := range dp.Entries {
		mode := os.FileMode(entry.Mode)
		if mode.IsDir() {
			err = t.Ingest(ctx, filepath.Join(dir, entry.Name))
			if err != nil {
				return err
			}
			continue
		}
		// Regular file.
		fpath := filepath.Join(dir, entry.Name)
		f, err := os.Open(fpath)
		if err != nil {
			return errors.Wrapf(err, "opening %s for reading", fpath)
		}
		ref, err := bs.SplitWrite(ctx, t.S, f, nil)
		f.Close() // in lieu of defer f.Close() above, which would require a new closure
		if err != nil {
			return errors.Wrapf(err, "ingesting blob store from file %s", fpath)
		}

		fa, err := t.fileAnchor(fpath)
		if err != nil {
			return errors.Wrapf(err, "computing anchor for file %s", fpath)
		}
		err = t.S.PutAnchor(ctx, ref, fa, time.Now())
		if err != nil {
			return errors.Wrapf(err, "storing anchor for file %s", fpath)
		}
	}

	da, err := t.dirAnchor(dir)
	if err != nil {
		return errors.Wrapf(err, "computing anchor for dir %s", dir)
	}
	err = t.S.PutAnchor(ctx, dirRef, da, time.Now())
	return errors.Wrapf(err, "storing anchor for dir %s", dir)
}

func infosToDirProto(infos []os.FileInfo) *Dir {
	dp := new(Dir)
	for _, info := range infos {
		name, mode := info.Name(), info.Mode()
		if name == "." || name == ".." {
			continue
		}
		if info.IsDir() {
			if name == ".git" {
				continue
			}
		} else if !mode.IsRegular() {
			continue
		}
		dp.Entries = append(dp.Entries, &Dirent{Name: name, Mode: uint32(mode)})
	}
	// No need to sort; ReadDir returns entries already sorted by name.
	return dp
}

func (t *Tree) fileAnchor(file string) (bs.Anchor, error) {
	rel, err := filepath.Rel(t.Root, file)
	return bs.Anchor(rel), err
}

func (t *Tree) dirAnchor(dir string) (bs.Anchor, error) {
	rel, err := filepath.Rel(t.Root, dir)
	return bs.Anchor(rel + "/"), err
}

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
	oldRef, err := t.S.GetAnchor(ctx, fa, time.Now())
	if stderrs.Is(err, bs.ErrNotFound) {
		// Perhaps file was added, which means its dir has (also) changed.
		err = t.DirChanged(ctx, filepath.Dir(file))
		if err != nil {
			return errors.Wrapf(err, "computing parent-dir changes from possibly new file %s", file)
		}
	} else if err != nil {
		return errors.Wrapf(err, "getting anchor for file %s", file)
	}

	f, err := os.Open(file)
	if err != nil {
		return errors.Wrapf(err, "opening %s for reading", file)
	}
	defer f.Close()

	newRef, err := bs.SplitWrite(ctx, t.S, f, nil)
	if err != nil {
		return errors.Wrapf(err, "storing blobtree for file %s", file)
	}

	if oldRef != newRef {
		err = t.S.PutAnchor(ctx, newRef, fa, time.Now())
		if err != nil {
			return errors.Wrapf(err, "updating anchor for file %s", file)
		}
	}

	return nil
}

func (t *Tree) DirChanged(ctx context.Context, dir string) error {
	if len(t.Root) > len(dir) {
		// Dir is higher than root; ignore.
		return nil
	}

	da, err := t.dirAnchor(dir)
	if err != nil {
		return errors.Wrapf(err, "computing anchor for dir %s", dir)
	}

	oldRef, err := t.S.GetAnchor(ctx, da, time.Now())
	if stderrs.Is(err, bs.ErrNotFound) {
		// Perhaps dir was added, which means its containing dir has (also) changed.
		err = t.DirChanged(ctx, filepath.Dir(dir))
		if err != nil {
			return errors.Wrapf(err, "computing parent-dir changes from possibly new dir %s", dir)
		}
	} else if err != nil {
		return errors.Wrapf(err, "getting anchor for dir %s", dir)
	}

	infos, err := ioutil.ReadDir(dir)
	if os.IsNotExist(err) {
		// Perhaps dir was removed, which means its containing dir has changed.
		return t.DirChanged(ctx, filepath.Dir(dir))
	}
	if err != nil {
		return errors.Wrapf(err, "reading dir %s", dir)
	}

	dp := infosToDirProto(infos)
	newRef, _, err := bs.PutProto(ctx, t.S, dp)
	if err != nil {
		return errors.Wrapf(err, "storing blob for dir %s", dir)
	}

	if newRef != oldRef {
		err = t.S.PutAnchor(ctx, newRef, da, time.Now())
		if err != nil {
			return errors.Wrapf(err, "updating anchor for dir %s", dir)
		}
	}

	return nil
}
