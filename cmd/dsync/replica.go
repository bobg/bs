package main

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/dsync"
)

type replica dsync.Tree

func (rep *replica) handleBlob(w http.ResponseWriter, req *http.Request) {
	blob, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ref, added, err := rep.S.Put(req.Context(), blob)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if added {
		log.Printf("received blob %s", ref)
	} else {
		log.Printf("received duplicate blob %s", ref)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (rep *replica) handleAnchor(w http.ResponseWriter, req *http.Request) {
	vals := req.URL.Query()
	a := vals.Get("a")
	refHex := vals.Get("ref")
	ref, err := bs.RefFromHex(refHex)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = rep.S.PutAnchor(req.Context(), ref, bs.Anchor(a), time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("received anchor %s: %s", a, ref)

	// Now constitute the file or dir indicated by the anchor.
	// TODO: There is a possible ordering problem:
	// not all blobs for a file may be present yet,
	// and not all files and subdirs may be present for a directory yet.

	err = rep.constitute(req.Context(), a, ref)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (rep *replica) constitute(ctx context.Context, a string, ref bs.Ref) error {
	path := filepath.Join(rep.Root, a)

	if strings.HasSuffix(a, "/") {
		return rep.constituteDir(ctx, path, ref)
	}

	// Constitute a plain file.

	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, 0755) // xxx each dir should have its proper mode
	if err != nil {
		return errors.Wrapf(err, "making dir %s", dir)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644) // xxx file should have its proper mode
	if err != nil {
		return errors.Wrapf(err, "opening file %s for writing", path)
	}
	defer f.Close()

	pr, pw := io.Pipe()

	var innerErr error
	go func() {
		innerErr = bs.SplitRead(ctx, rep.S, ref, pw)
		pw.Close()
	}()

	_, err = io.Copy(f, pr)
	if err != nil {
		return errors.Wrapf(err, "copying from blobstore to file %s", path)
	}
	return errors.Wrapf(innerErr, "reading blobtree %s", ref)
}

func (rep *replica) constituteDir(ctx context.Context, dir string, ref bs.Ref) error {
	var dp dsync.Dir
	err := bs.GetProto(ctx, rep.S, ref, &dp)
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
