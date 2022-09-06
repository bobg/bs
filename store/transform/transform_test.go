package transform

import (
	"compress/lzw"
	"context"
	"os"
	"testing"

	"github.com/bobg/bs/store/mem"
	"github.com/bobg/bs/testutil"
)

func TestTransform(t *testing.T) {
	ctx := context.Background()

	data, err := os.ReadFile("../../testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("lzw", func(t *testing.T) {
		t.Run("lsb", func(t *testing.T) {
			s, err := New(ctx, mem.New(), LZW{Order: lzw.LSB}, "anchor")
			if err != nil {
				t.Fatal(err)
			}
			testutil.ReadWrite(ctx, t, s, data)
		})
		t.Run("msb", func(t *testing.T) {
			s, err := New(ctx, mem.New(), LZW{Order: lzw.MSB}, "anchor")
			if err != nil {
				t.Fatal(err)
			}
			testutil.ReadWrite(ctx, t, s, data)
		})
	})
	t.Run("flate", func(t *testing.T) {
		for i := -2; i <= 9; i++ {
			s, err := New(ctx, mem.New(), Flate{Level: i}, "anchor")
			if err != nil {
				t.Fatal(err)
			}
			testutil.ReadWrite(ctx, t, s, data)
		}
	})
}
