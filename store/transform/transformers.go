package transform

import (
	"bytes"
	"compress/flate"
	"compress/lzw"
	"context"
	"io/ioutil"
)

// LZW is a Transformer implementing lzw compression.
type LZW struct {
	Order lzw.Order
}

// In implements Transformer.In.
func (l LZW) In(_ context.Context, inp []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	w := lzw.NewWriter(buf, l.Order, 8)
	w.Write(inp)
	w.Close()
	return buf.Bytes(), nil
}

// Out implements Transformer.Out.
func (l LZW) Out(_ context.Context, inp []byte) ([]byte, error) {
	r := bytes.NewReader(inp)
	rr := lzw.NewReader(r, l.Order, 8)
	defer rr.Close()
	return ioutil.ReadAll(rr)
}

// Flate is a Transformer implementing RFC1951 DEFLATE compression.
type Flate struct {
	Level int
}

// In implements Transformer.In.
func (f Flate) In(_ context.Context, inp []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	level := f.Level
	if level < -2 || level > 9 {
		level = -1
	}
	w, _ := flate.NewWriter(buf, f.Level)
	w.Write(inp)
	w.Close()
	return buf.Bytes(), nil
}

// Out implements Transformer.Out.
func (f Flate) Out(_ context.Context, inp []byte) ([]byte, error) {
	r := bytes.NewReader(inp)
	rr := flate.NewReader(r)
	defer rr.Close()
	return ioutil.ReadAll(rr)
}
