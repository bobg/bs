package main

import (
	"bytes"
	"context"
	stderrs "errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rjeczalik/notify"

	"github.com/bobg/bs"
	"github.com/bobg/bs/file"
)

type primary struct {
	s    bs.Store
	root string
}

func runPrimary(ctx context.Context, root, replicaURL string) error {
	bsdir, err := ioutil.TempDir("", "dsync-primary")
	if err != nil {
		return errors.Wrap(err, "creating tempdir")
	}
	defer os.RemoveAll(bsdir)

	var (
		refs    = make(chan bs.Ref)
		anchors = make(chan anchorInfo)
		fstore  = file.New(bsdir)
		rec     = newRecorder(fstore, refs, anchors)
		p       = &primary{s: rec, root: root}
		fsch    = make(chan notify.EventInfo, 100)
	)

	anchorURL, err := url.Parse(replicaURL + "/anchor")
	if err != nil {
		return errors.Wrapf(err, "parsing URL %s/anchor", replicaURL)
	}

	var wg sync.WaitGroup

	// This goroutine monitors the recorder channels for new blobs and anchors,
	// turning them into HTTP requests to the replica.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(anchors)
		defer close(refs)

		// TODO: Might be able to get a little better performance
		// with grpc streaming instead of separate http requests.

		getSendBlob := func(ref bs.Ref) error {
			blob, err := p.s.Get(ctx, ref)
			if err != nil {
				return errors.Wrapf(err, "getting blob %s", ref)
			}

			resp, err := http.Post(replicaURL+"/blob", "application/octet-stream", bytes.NewReader(blob))
			if err != nil {
				return errors.Wrapf(err, "sending POST to %s/blob", replicaURL)
			}
			if resp.Body != nil {
				resp.Body.Close()
			}
			return nil
		}

		for {
			// There are two selects here
			// (the first one non-blocking),
			// to ensure that all pending blobs are handled
			// before any pending anchors.

			select {
			case <-ctx.Done():
				log.Print("context canceled, exiting send goroutine")
				return

			case ref := <-refs:
				err := getSendBlob(ref)
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
				log.Print("context canceled, exiting send goroutine")
				return

			case ref := <-refs:
				err := getSendBlob(ref)
				if err != nil {
					log.Printf("ERROR sending blob %s: %s", ref, err)
				}

			case info := <-anchors:
				v := url.Values{}
				v.Set("ref", info.ref.String())
				v.Set("a", string(info.a))

				anchorURL.RawQuery = v.Encode()

				req, err := http.NewRequest("POST", anchorURL.String(), nil)
				if err != nil {
					log.Printf("ERROR constructing POST request for %s: %s", anchorURL, err)
					continue
				}

				var client http.Client
				resp, err := client.Do(req)
				if err != nil {
					log.Printf("ERROR sending anchor %s (ref %s): %s", info.a, info.ref, err)
					continue
				}
				if resp.Body != nil {
					resp.Body.Close()
				}
			}
		}
	}()

	// This goroutine monitors the filesystem watcher.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer notify.Stop(fsch)

		for {
			select {
			case <-ctx.Done():
				log.Print("context canceled, exiting watcher goroutine")
				return

			case ev, ok := <-fsch:
				if !ok {
					log.Fatal("EOF on watcher events channel")
				}

				log.Printf("got event %s", ev.Event())

				err := p.changesFromFile(ctx, ev.Path())
				if err != nil {
					log.Printf("ERROR handling changes from file %s: %s", ev.Path(), err)
				}
			}
		}
	}()

	err = notify.Watch(root+"/...", fsch, notify.All)
	if err != nil {
		return errors.Wrap(err, "watching filesystem")
	}

	err = p.populate(ctx, root)
	if err != nil {
		return errors.Wrapf(err, "populating root %s", root)
	}

	wg.Wait()

	return nil
}

func (p *primary) populate(ctx context.Context, dir string) error {
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		return errors.Wrapf(err, "reading dir %s", dir)
	}

	dp := infosToDirProto(infos)

	dirRef, _, err := bs.PutProto(ctx, p.s, dp)
	if err != nil {
		return errors.Wrapf(err, "storing blob for dir %s", dir)
	}

	// Populate everything under this dir before writing its anchor.
	// Hopefully the replica receives things in roughly the same order.
	// If it receives things too badly out of order,
	// it might have to create a directory entry for a file that doesn't exist yet.

	for _, entry := range dp.Entries {
		mode := os.FileMode(entry.Mode)
		if mode.IsDir() {
			err = p.populate(ctx, filepath.Join(dir, entry.Name))
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
		ref, err := bs.SplitWrite(ctx, p.s, f, nil)
		f.Close() // in lieu of defer f.Close() above, which would require a new closure
		if err != nil {
			return errors.Wrapf(err, "populating blob store from file %s", fpath)
		}

		fa, err := p.fileAnchor(fpath)
		if err != nil {
			return errors.Wrapf(err, "computing anchor for file %s", fpath)
		}
		err = p.s.PutAnchor(ctx, ref, fa, time.Now())
		if err != nil {
			return errors.Wrapf(err, "storing anchor for file %s", fpath)
		}
	}

	da, err := p.dirAnchor(dir)
	if err != nil {
		return errors.Wrapf(err, "computing anchor for dir %s", dir)
	}
	err = p.s.PutAnchor(ctx, dirRef, da, time.Now())
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

// File is somewhere in the root tree and is thought to have changed somehow.
// Identify all the new blobrefs and anchors needed to represent the change.
func (p *primary) changesFromFile(ctx context.Context, file string) error {
	info, err := os.Lstat(file)
	if os.IsNotExist(err) {
		// Perhaps file was removed, which means its containing dir has changed.
		return p.changesFromDir(ctx, filepath.Dir(file))
	}

	if info.IsDir() {
		return p.changesFromDir(ctx, file)
	}
	if !info.Mode().IsRegular() {
		return nil
	}

	fa, err := p.fileAnchor(file)
	if err != nil {
		return errors.Wrapf(err, "computing anchor for file %s", file)
	}
	oldRef, err := p.s.GetAnchor(ctx, fa, time.Now())
	if stderrs.Is(err, bs.ErrNotFound) {
		// Perhaps file was added, which means its dir has changed.
		err = p.changesFromDir(ctx, filepath.Dir(file))
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

	newRef, err := bs.SplitWrite(ctx, p.s, f, nil)
	if err != nil {
		return errors.Wrapf(err, "storing blobtree for file %s", file)
	}

	if oldRef != newRef {
		err = p.s.PutAnchor(ctx, newRef, fa, time.Now())
		if err != nil {
			return errors.Wrapf(err, "updating anchor for file %s", file)
		}
	}

	return nil
}

func (p *primary) changesFromDir(ctx context.Context, dir string) error {
	if len(p.root) > len(dir) {
		// Dir is higher than root; ignore.
		return nil
	}

	da, err := p.dirAnchor(dir)
	if err != nil {
		return errors.Wrapf(err, "computing anchor for dir %s", dir)
	}

	oldRef, err := p.s.GetAnchor(ctx, da, time.Now())
	if stderrs.Is(err, bs.ErrNotFound) {
		// Perhaps dir was added, which means its containing dir has changed.
		err = p.changesFromDir(ctx, filepath.Dir(dir))
		if err != nil {
			return errors.Wrapf(err, "computing parent-dir changes from possibly new dir %s", dir)
		}
	} else if err != nil {
		return errors.Wrapf(err, "getting anchor for dir %s", dir)
	}

	infos, err := ioutil.ReadDir(dir)
	if os.IsNotExist(err) {
		// Perhaps dir was removed, which means its containing dir has changed.
		return p.changesFromDir(ctx, filepath.Dir(dir))
	}
	if err != nil {
		return errors.Wrapf(err, "reading dir %s", dir)
	}

	dp := infosToDirProto(infos)
	newRef, _, err := bs.PutProto(ctx, p.s, dp)
	if err != nil {
		return errors.Wrapf(err, "storing blob for dir %s", dir)
	}

	if newRef != oldRef {
		err = p.s.PutAnchor(ctx, newRef, da, time.Now())
		if err != nil {
			return errors.Wrapf(err, "updating anchor for dir %s", dir)
		}
	}

	return nil
}

func (p *primary) fileAnchor(file string) (bs.Anchor, error) {
	rel, err := filepath.Rel(p.root, file)
	return bs.Anchor(rel), err
}

func (p *primary) dirAnchor(dir string) (bs.Anchor, error) {
	rel, err := filepath.Rel(p.root, dir)
	return bs.Anchor(rel + "/"), err
}
