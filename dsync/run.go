package dsync

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	sync "sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rjeczalik/notify"

	"github.com/bobg/bs"
	"github.com/bobg/bs/split"
)

// RunPrimary causes t to act as the primary for file-tree synchronization.
// It creates a wrapper for t that uses a Streamer calling `newRef` and `newAnchor` for new refs and anchors.
// It then ingests the file tree at t.Root.
// It also launches a notify.Watch goroutine generating Tree.FileChanged calls on that wrapper.
// The function returns when `ctx` is canceled.
func (t *Tree) RunPrimary(ctx context.Context, newRef func(bs.Ref) error, newAnchor func(AnchorTuple) error) error {
	var (
		refs    = make(chan bs.Ref)
		anchors = make(chan AnchorTuple)
	)
	defer close(refs)
	defer close(anchors)

	streamer := NewStreamer(t.S, refs, anchors)
	tt := &Tree{
		S:    streamer,
		Root: t.Root,
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		WatchRefsAnchors(ctx, refs, anchors, newRef, newAnchor)
	}()

	// This must come after the launch of the goroutine above
	// because it wants to write to the `refs` and `anchors` channels
	// and will block if there's nothing to consume them.
	rootRef, err := tt.Ingest(ctx, tt.Root)
	if err != nil {
		return errors.Wrapf(err, "ingesting %s", tt.Root)
	}
	log.Printf("ingested %s tree at %s", tt.Root, rootRef)

	fsch := make(chan notify.EventInfo, 100)

	// This goroutine monitors the filesystem watcher.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer notify.Stop(fsch)

		for {
			select {
			case <-ctx.Done():
				log.Print("context canceled, exiting filesystem watcher")
				return

			case ev, ok := <-fsch:
				if !ok {
					log.Printf("file-events channel closed, exiting filesystem watcher")
				}
				err := tt.FileChanged(ctx, ev.Path())
				if err != nil {
					log.Printf("ERROR handling change of file %s: %s", ev.Path(), err)
				}
			}
		}
	}()

	err = notify.Watch(tt.Root+"/...", fsch, notify.All)
	if err != nil {
		return errors.Wrapf(err, "watching %s/...", tt.Root)
	}

	wg.Wait()

	return nil
}

// WatchRefsAnchors runs as a goroutine until `ctx` is canceled.
// It calls newRef for each Ref arriving on `refs`,
// and newAnchor for each AnchorTuple arriving on `anchors`.
// When refs and anchors are available simultaneously,
// it favors consuming all the refs first before any of the anchors.
// This is meant to help ensure all needed blobs are present before a call to ReplicaAnchor.
func WatchRefsAnchors(ctx context.Context, refs <-chan bs.Ref, anchors <-chan AnchorTuple, newRef func(bs.Ref) error, newAnchor func(AnchorTuple) error) {
	for {
		// There are two selects here
		// (the first one non-blocking),
		// to ensure that all pending blobs are handled
		// before any pending anchors.

		select {
		case <-ctx.Done():
			log.Print("context canceled, exiting WatchRefsAnchors")
			return

		case ref, ok := <-refs:
			if !ok {
				log.Print("refs channel closed, exiting WatchRefsAnchors")
				return
			}
			err := newRef(ref)
			if err != nil {
				log.Printf("ERROR sending blob %s: %s", ref, err)
			}

			// Prevent falling through to the next select
			// if there's another blob to process.
			continue

		default:
		}

		select {
		case <-ctx.Done():
			log.Print("context canceled, exiting WatchRefsAnchors")
			return

		case ref, ok := <-refs:
			if !ok {
				log.Print("refs channel closed, exiting WatchRefsAnchors")
				return
			}
			err := newRef(ref)
			if err != nil {
				log.Printf("ERROR sending blob %s: %s", ref, err)
			}

		case tuple, ok := <-anchors:
			if !ok {
				log.Print("anchors channel closed, exiting WatchRefsAnchors")
				return
			}
			err := newAnchor(tuple)
			if err != nil {
				log.Printf("ERROR sending anchor %s (ref %s): %s", tuple.A, tuple.Ref, err)
			}
		}
	}
}

// ReplicaAnchor handles the arrival of a new anchor for a Tree acting as a sync replica.
// It will add, remove, and update files and directories under t.Root as needed,
// provided the tree of blobs rooted at `ref` are already present.
// (If they are not, an error will be returned, and t's file tree may be partially updated.)
func (t *Tree) ReplicaAnchor(ctx context.Context, a bs.Anchor, ref bs.Ref) error {
	err := t.S.PutAnchor(ctx, a, time.Now(), ref)
	if err != nil {
		return errors.Wrapf(err, "storing anchor %s (ref %s)", a, ref)
	}

	return t.constitute(ctx, string(a), ref)
}

func (t *Tree) constitute(ctx context.Context, a string, ref bs.Ref) error {
	path := filepath.Join(t.Root, a)

	if strings.HasSuffix(a, "/") {
		return t.constituteDir(ctx, path, ref)
	}

	// Constitute a plain file.

	dir := filepath.Dir(path)
	// TODO: each dir should have its proper mode
	// (and will, once its containing dir is sync'd, but this is a hack)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return errors.Wrapf(err, "making dir %s", dir)
	}

	// TODO: each file should have its proper mode
	// (and will, once its containiner dir is sync'd, but this is a hack)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return errors.Wrapf(err, "opening file %s for writing", path)
	}
	defer f.Close()

	pr, pw := io.Pipe()

	var innerErr error
	go func() {
		innerErr = split.Read(ctx, t.S, ref, pw)
		pw.Close()
	}()

	_, err = io.Copy(f, pr)
	if err != nil {
		return errors.Wrapf(err, "copying from blobstore to file %s", path)
	}
	return errors.Wrapf(innerErr, "reading blobtree %s", ref)
}

func (t *Tree) constituteDir(ctx context.Context, dir string, ref bs.Ref) error {
	var dp Dir
	err := bs.GetProto(ctx, t.S, ref, &dp)
	if err != nil {
		return errors.Wrapf(err, "getting dir proto %s", ref)
	}

	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		return errors.Wrapf(err, "reading dir %s", dir)
	}

	i, j := 0, 0
	for i < len(dp.Entries) || j < len(infos) {
		var (
			iname, jname string
			imode, jmode os.FileMode
		)

		if i < len(dp.Entries) {
			iname, imode = dp.Entries[i].Name, os.FileMode(dp.Entries[i].Mode)
		}
		if j < len(infos) {
			jname, jmode = infos[j].Name(), infos[j].Mode()
		}

		if i == len(dp.Entries) || iname > jname {
			log.Printf("constituting dir %s: removing entry %s", dir, jname)
			err = os.RemoveAll(filepath.Join(dir, jname))
			if err != nil {
				return errors.Wrapf(err, "removing %s/%s (removed from primary)", dir, jname)
			}
			j++
			continue
		}
		if j == len(infos) || iname < jname {
			log.Printf("constituting dir %s: do not yet have entry %s", dir, iname)
			i++
			continue
		}

		// iname == jname
		i++
		j++
		if imode.IsDir() && !jmode.IsDir() {
			log.Printf("constituting dir %s: removing file %s to make way for dir", dir, jname)
			full := filepath.Join(dir, jname)
			err = os.Remove(full)
			if err != nil {
				return errors.Wrapf(err, "removing file %s (to make way for dir)", full)
			}
			err = os.Mkdir(full, imode)
			if err != nil {
				return errors.Wrapf(err, "making dir %s", full)
			}
			continue
		}
		if !imode.IsDir() && jmode.IsDir() {
			log.Printf("constituting dir %s: removing dir %s to make way for file", dir, jname)
			err = os.RemoveAll(filepath.Join(dir, jname))
			if err != nil {
				return errors.Wrapf(err, "removing dir %s/%s (to make way for file)", dir, jname)
			}
			continue
		}

		if imode != jmode {
			log.Printf("constituting dir %s: updating mode of %s from 0%o to 0%o", dir, jname, jmode, imode)
			err = os.Chmod(filepath.Join(dir, jname), imode)
			if err != nil {
				return errors.Wrapf(err, "changing mode of %s/%s from 0%o to 0%o", dir, jname, jmode, imode)
			}
		}
	}

	return nil
}
