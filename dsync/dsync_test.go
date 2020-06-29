package dsync

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bobg/bs"
	"github.com/bobg/bs/mem"
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

	eq, err := dirsEqual(primaryRoot, copyFrom)
	if err != nil {
		t.Fatal(err)
	}
	if !eq {
		t.Fatalf("dirs not equal after populating %s from %s", primaryRoot, copyFrom)
	}

	primary := &Tree{
		S:    mem.New(),
		Root: primaryRoot,
	}

	ctx := context.Background()

	newRef := func(ref bs.Ref) error {
		blob, err := primary.S.Get(ctx, ref)
		if err != nil {
			return err
		}
		_, _, err = replica.S.Put(ctx, blob)
		return err
	}
	newAnchor := func(tuple AnchorTuple) error {
		return replica.ReplicaAnchor(ctx, tuple.A, tuple.Ref)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error)

	go func() {
		defer close(errCh)
		err := primary.RunPrimary(ctx, newRef, newAnchor)
		if err != nil {
			errCh <- err
		}
	}()

	sleepDur := 2 * time.Second
	t.Logf("sleeping %s for initial sync", sleepDur)
	time.Sleep(sleepDur)

	eq, err = dirsEqual(replicaRoot, primaryRoot)
	if err != nil {
		t.Fatal(err)
	}
	if !eq {
		t.Fatal("dirs not equal after initial sync")
	}

	editFile := filepath.Join(primaryRoot, "testdata", "yubnub.opus")
	f, err := os.OpenFile(editFile, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	_, err = f.WriteString("foo")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	err = primary.FileChanged(ctx, editFile)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("sleeping %s for file sync", sleepDur)
	time.Sleep(sleepDur)

	eq, err = dirsEqual(replicaRoot, primaryRoot)
	if err != nil {
		t.Fatal(err)
	}
	if !eq {
		t.Fatal("dirs not equal after file edit")
	}

	err = os.Remove(editFile)
	if err != nil {
		t.Fatal(err)
	}

	err = primary.FileChanged(ctx, editFile)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("sleeping %s for file sync", sleepDur)
	time.Sleep(sleepDur)

	eq, err = dirsEqual(replicaRoot, primaryRoot)
	if err != nil {
		t.Fatal(err)
	}
	if !eq {
		t.Fatal("dirs not equal after file deletion")
	}
}

func dirsEqual(a, b string) (bool, error) {
	aEntries, err := ioutil.ReadDir(a)
	if err != nil {
		return false, err
	}
	bEntries, err := ioutil.ReadDir(b)
	if err != nil {
		return false, err
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
			return false, nil
		}
		name := aEntry.Name()
		if aEntry.IsDir() {
			if bEntry.IsDir() {
				eq, err := dirsEqual(filepath.Join(a, name), filepath.Join(b, name))
				if err != nil || !eq {
					return eq, err
				}
			} else {
				return false, nil
			}
		} else if bEntry.IsDir() {
			return false, nil
		} else {
			eq, err := filesEqual(a, aEntry, b, bEntry)
			if err != nil || !eq {
				return eq, err
			}
		}

		i++
		j++
	}
	for i < len(aEntries) {
		if !isIgnoreEntry(aEntries[i]) {
			return false, nil
		}
		i++
	}
	for j < len(bEntries) {
		if !isIgnoreEntry(bEntries[j]) {
			return false, nil
		}
		j++
	}

	return true, nil
}

func filesEqual(dir1 string, entry1 os.FileInfo, dir2 string, entry2 os.FileInfo) (bool, error) {
	if entry1.Size() != entry2.Size() {
		return false, nil
	}

	f1, err := os.Open(filepath.Join(dir1, entry1.Name()))
	if err != nil {
		return false, err
	}
	defer f1.Close()

	f2, err := os.Open(filepath.Join(dir2, entry2.Name()))
	if err != nil {
		return false, err
	}
	defer f2.Close()

	bf1 := bufio.NewReader(f1)
	bf2 := bufio.NewReader(f2)

	for {
		b1, err1 := bf1.ReadByte()
		b2, err2 := bf2.ReadByte()

		if err1 == io.EOF && err2 == io.EOF {
			return true, nil
		}
		if err1 != nil {
			return false, err1
		}
		if err2 != nil {
			return false, err2
		}
		if b1 != b2 {
			return false, nil
		}
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
		return err
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
				return err
			}
			err = copyDir(dstName, srcName)
			if err != nil {
				return err
			}
			continue
		}

		err = copyFile(dstName, srcName)
		if err != nil {
			return err
		}
	}

	return nil
}

func copyFile(dst, src string) error {
	inp, err := os.Open(src)
	if err != nil {
		return err
	}
	defer inp.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, inp)
	return err
}