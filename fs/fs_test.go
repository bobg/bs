package fs

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
	"github.com/bobg/bs/split"
	"github.com/bobg/bs/store/mem"
)

const testDir = "."

func TestFS(t *testing.T) {
	var (
		s   = mem.New()
		d   = NewDir()
		ctx = context.Background()
	)

	ref, err := d.Ingest(ctx, s, testDir)
	if err != nil {
		t.Fatal(err)
	}

	tmpdir, err := ioutil.TempDir("", "bsfstest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	d = NewDir()
	err = d.Load(ctx, s, ref)
	if err != nil {
		t.Fatal(err)
	}

	err = extractInto(ctx, s, d, tmpdir)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	f := func(ch chan<- string) filepath.WalkFunc {
		return func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if (info.Mode() & os.ModeSymlink) == os.ModeSymlink {
				// TODO: compare symlinks
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- path:
				return nil
			}
		}
	}

	var (
		ch1 = make(chan string)
		ch2 = make(chan string)
	)

	go func() {
		defer close(ch1)
		err := filepath.Walk(testDir, f(ch1))
		if err != nil {
			t.Error(err)
		}
	}()
		
	go func() {
		defer close(ch2)
		err := filepath.Walk(tmpdir, f(ch2))
		if err != nil {
			t.Error(err)
		}
	}()

	for path1 := range ch1 {
		t.Logf("path1: %s", path1)

		path2, ok := <-ch2
		if !ok {
			t.Fatalf("%s exists in %s but not %s", path1, testDir, tmpdir)
		}

		t.Logf("path2: %s", path2)

		err = compareFiles(path1, path2)
		if err != nil {
			t.Fatal(err)
		}
	}

	path2, ok := <-ch2
	if ok {
		t.Fatalf("%s exists in %s but not %s", path2, tmpdir, testDir)
	}
}

func compareFiles(path1, path2 string) error {
	f1, err := os.Open(path1)
	if err != nil {
		return errors.Wrapf(err, "opening %s", path1)
	}
	defer f1.Close()

	f2, err := os.Open(path2)
	if err != nil {
		return errors.Wrapf(err, "opening %s", path2)
	}
	defer f2.Close()

	b1 := bufio.NewReader(f1)
	b2 := bufio.NewReader(f2)

	pos := 0
	for {
		pos++

		c1, err1 := b1.ReadByte()
		c2, err2 := b2.ReadByte()

		if err1 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return nil
			}
			return errors.Wrapf(err1, "reading from %s", path1)
		}
		if err2 != nil {
			return errors.Wrapf(err1, "reading from %s", path2)
		}

		if c1 != c2 {
			return fmt.Errorf("%s differs from %s at position %d", path1, path2, pos)
		}
	}
}

func extractInto(ctx context.Context, g bs.Getter, d *Dir, dest string) error {
	return d.Each(ctx, g, func(name string, dirent *Dirent) error {
		mode := os.FileMode(dirent.Mode)
		if mode.IsDir() {
			subdirName := filepath.Join(dest, name)
			err := os.Mkdir(subdirName, mode)
			if err != nil {
				return errors.Wrapf(err, "making subdir %s", subdirName)
			}

			subdirRef, err := g.GetAnchor(ctx, bs.Anchor(dirent.Item), time.Now())
			if err != nil {
				return errors.Wrapf(err, "resolving anchor %s for subdir %s", dirent.Item, subdirName)
			}

			var subdir Dir
			err = subdir.Load(ctx, g, subdirRef)
			if err != nil {
				return errors.Wrapf(err, "loading ref %s for subdir %s", subdirRef, subdirName)
			}

			err = extractInto(ctx, g, &subdir, subdirName)
			return errors.Wrapf(err, "extracting subdir %s", subdirName)
		}

		if (mode & os.ModeSymlink) == os.ModeSymlink {
			err := os.Symlink(filepath.Join(dest, name), dirent.Item)
			return errors.Wrapf(err, "creating symlink %s/%s -> %s", dest, name, dirent.Item)
		}

		ref, err := g.GetAnchor(ctx, bs.Anchor(dirent.Item), time.Now())
		if err != nil {
			return errors.Wrapf(err, "resolving anchor %s for file %s/%s", dirent.Item, dest, name)
		}

		f, err := os.OpenFile(filepath.Join(dest, name), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
		if err != nil {
			return errors.Wrapf(err, "opening %s/%s", dest, name)
		}
		defer f.Close()
		return split.Read(ctx, g, ref, f)
	})
}
