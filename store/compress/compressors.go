package compress

import (
	"bytes"
	"compress/flate"
	"compress/lzw"
	"io/ioutil"
)

type LZW struct {
	Order lzw.Order
}

func (l LZW) Compress(inp []byte) []byte {
	buf := new(bytes.Buffer)
	w := lzw.NewWriter(buf, l.Order, 8)
	w.Write(inp)
	w.Close()
	return buf.Bytes()
}

func (l LZW) Uncompress(inp []byte) ([]byte, error) {
	r := bytes.NewReader(inp)
	rr := lzw.NewReader(r, l.Order, 8)
	defer rr.Close()
	return ioutil.ReadAll(rr)
}

type Flate struct {
	Level int
}

func (f Flate) Compress(inp []byte) []byte {
	buf := new(bytes.Buffer)
	level := f.Level
	if level < -2 || level > 9 {
		level = -1
	}
	w, _ := flate.NewWriter(buf, f.Level)
	w.Write(inp)
	w.Close()
	return buf.Bytes()
}

func (f Flate) Uncompress(inp []byte) ([]byte, error) {
	r := bytes.NewReader(inp)
	rr := flate.NewReader(r)
	defer rr.Close()
	return ioutil.ReadAll(rr)
}
