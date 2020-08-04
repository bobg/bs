package dsync

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store/mem"
)

func TestDsync(t *testing.T) {
	replicaRoot, err := ioutil.TempDir("", "dsynctest-replica")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(replicaRoot)

	replica := &Tree{
		S:    mem.New(),
		Root: replicaRoot,
	}

	primaryRoot, err := ioutil.TempDir("", "dsynctest-primary")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(primaryRoot)

	copyFrom, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	copyFrom = filepath.Dir(copyFrom)

	err = copyDir(primaryRoot, copyFrom)
	if err != nil {
		t.Fatal(err)
	}

	err = dirsEqual(primaryRoot, copyFrom)
	if err != nil {
		t.Fatal(err)
	}

	var primary *Tree

	ctx := context.Background()
	newRef := func(ref bs.Ref) error {
		// t.Logf("new ref %s", ref)

		blob, err := primary.S.Get(ctx, ref)
		if err != nil {
			return errors.Wrapf(err, "getting ref %s from primary", ref)
		}
		_, _, err = replica.S.Put(ctx, blob)
		return errors.Wrapf(err, "storing blob %s to replica", ref)
	}
	newAnchor := func(tuple AnchorTuple) error {
		// t.Logf("new anchor %s (%s)", tuple.A, tuple.Ref)

		return replica.ReplicaAnchor(ctx, tuple.A, tuple.Ref)
	}

	var (
		refs    = make(chan bs.Ref)
		anchors = make(chan AnchorTuple)
	)
	defer close(refs)
	defer close(anchors)

	primary = &Tree{
		S:    NewStreamer(mem.New(), refs, anchors),
		Root: primaryRoot,
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go WatchRefsAnchors(ctx, refs, anchors, newRef, newAnchor)

	rootRef, err := primary.Ingest(ctx, primaryRoot)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ingested %s at %s", primaryRoot, rootRef)

	time.Sleep(time.Second)
	err = dirsEqual(replicaRoot, primaryRoot)
	if err != nil {
		t.Fatal(err)
	}

	editFile := filepath.Join(primaryRoot, "testdata", "yubnub.opus")
	t.Logf("editing %s", editFile)

	f, err := os.OpenFile(editFile, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	_, err = f.WriteString("foo")
	if err != nil {
		t.Fatal(err)
	}
	err = f.Sync() // the test is flaky without this Sync!
	if err != nil {
		t.Fatal(err)
	}
	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = primary.FileChanged(ctx, editFile)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	err = dirsEqual(replicaRoot, primaryRoot)
	if err != nil {
		t.Fatal(err)
	}

	addFile := filepath.Join(primaryRoot, "testdata", "added")
	t.Logf("adding file %s", addFile)

	err = ioutil.WriteFile(addFile, []byte("xyzzy"), 0644)
	if err != nil {
		t.Fatal(err)
	}
	err = primary.FileChanged(ctx, addFile)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("removing file %s", editFile)

	err = os.Remove(editFile)
	if err != nil {
		t.Fatal(err)
	}

	err = primary.FileChanged(ctx, editFile)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	err = dirsEqual(replicaRoot, primaryRoot)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("removing dir %s", filepath.Dir(editFile))
	err = os.RemoveAll(filepath.Dir(editFile))
	if err != nil {
		t.Fatal(err)
	}

	err = primary.FileChanged(ctx, filepath.Dir(editFile))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)
	err = dirsEqual(replicaRoot, primaryRoot)
	if err != nil {
		t.Fatal(err)
	}
}

func dirsEqual(a, b string) error {
	aEntries, err := ioutil.ReadDir(a)
	if err != nil {
		return errors.Wrapf(err, "reading dir %s", a)
	}
	bEntries, err := ioutil.ReadDir(b)
	if err != nil {
		return errors.Wrapf(err, "reading dir %s", b)
	}

	i, j := 0, 0
	for i < len(aEntries) && j < len(bEntries) {
		aEntry := aEntries[i]
		if isIgnoreEntry(aEntry) {
			i++
			continue
		}

		bEntry := bEntries[j]
		if isIgnoreEntry(bEntry) {
			j++
			continue
		}

		if aEntry.Name() != bEntry.Name() {
			return fmt.Errorf("%s has %s where %s has %s", a, aEntry.Name(), b, bEntry.Name())
		}
		name := aEntry.Name()
		if aEntry.IsDir() {
			if bEntry.IsDir() {
				err := dirsEqual(filepath.Join(a, name), filepath.Join(b, name))
				if err != nil {
					return errors.Wrapf(err, "comparing dirs %s/%s and %s/%s", a, name, b, name)
				}
			} else {
				return fmt.Errorf("%s is a dir and %s is not", aEntry.Name(), bEntry.Name())
			}
		} else if bEntry.IsDir() {
			return fmt.Errorf("%s is not a dir and %s is", aEntry.Name(), bEntry.Name())
		} else {
			err := filesEqual(a, aEntry, b, bEntry)
			if err != nil {
				return errors.Wrapf(err, "comparing files %s/%s and %s/%s", a, name, b, name)
			}
		}

		i++
		j++
	}
	for i < len(aEntries) {
		if !isIgnoreEntry(aEntries[i]) {
			return fmt.Errorf("extra entry on left: %s", aEntries[i].Name())
		}
		i++
	}
	for j < len(bEntries) {
		if !isIgnoreEntry(bEntries[j]) {
			return fmt.Errorf("extra entry on right: %s", bEntries[j].Name())
		}
		j++
	}

	return nil
}

func filesEqual(dir1 string, entry1 os.FileInfo, dir2 string, entry2 os.FileInfo) error {
	if entry1.Size() != entry2.Size() {
		return fmt.Errorf("sizes %d and %d do not match", entry1.Size(), entry2.Size())
	}

	f1, err := os.Open(filepath.Join(dir1, entry1.Name()))
	if err != nil {
		return errors.Wrapf(err, "opening %s/%s for reading", dir1, entry1.Name())
	}
	defer f1.Close()

	f2, err := os.Open(filepath.Join(dir2, entry2.Name()))
	if err != nil {
		return errors.Wrapf(err, "opening %s/%s for reading", dir2, entry2.Name())
	}
	defer f2.Close()

	var (
		bf1 = bufio.NewReader(f1)
		bf2 = bufio.NewReader(f2)
		pos = 0
	)

	for {
		b1, err1 := bf1.ReadByte()
		b2, err2 := bf2.ReadByte()

		if err1 == io.EOF && err2 == io.EOF {
			return nil
		}
		if err1 != nil {
			return errors.Wrapf(err1, "reading from %s/%s", dir1, entry1.Name())
		}
		if err2 != nil {
			return errors.Wrapf(err2, "reading from %s/%s", dir2, entry2.Name())
		}
		if b1 != b2 {
			return fmt.Errorf("files %s/%s and %s/%s disagree at position %d", dir1, entry1.Name(), dir2, entry2.Name(), pos)
		}

		pos++
	}
}

func isIgnoreEntry(entry os.FileInfo) bool {
	if entry.IsDir() {
		name := entry.Name()
		switch name {
		case ".", "..", ".git":
			return true
		}
		return false
	}

	return !entry.Mode().IsRegular()
}

func copyDir(dst, src string) error {
	infos, err := ioutil.ReadDir(src)
	if err != nil {
		return errors.Wrapf(err, "reading dir %s", src)
	}
	for _, info := range infos {
		if isIgnoreEntry(info) {
			continue
		}

		var (
			name    = info.Name()
			srcName = filepath.Join(src, name)
			dstName = filepath.Join(dst, name)
		)
		if info.IsDir() {
			err = os.Mkdir(dstName, 0755)
			if err != nil {
				return errors.Wrapf(err, "making dir %s", dstName)
			}
			err = copyDir(dstName, srcName)
			if err != nil {
				return errors.Wrapf(err, "copying dir %s to %s", srcName, dstName)
			}
			continue
		}

		err = copyFile(dstName, srcName)
		if err != nil {
			return errors.Wrapf(err, "copying file %s to %s", srcName, dstName)
		}
	}

	return nil
}

func copyFile(dst, src string) error {
	inp, err := os.Open(src)
	if err != nil {
		return errors.Wrapf(err, "opening %s for reading", src)
	}
	defer inp.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return errors.Wrapf(err, "opening %s for writing", dst)
	}
	defer out.Close()

	_, err = io.Copy(out, inp)
	return errors.Wrap(err, "in io.Copy")
}
