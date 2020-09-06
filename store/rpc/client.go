package rpc

import (
	context "context"
	"io"

	"github.com/pkg/errors"
	grpc "google.golang.org/grpc"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store"
)

var _ bs.Store = &Client{}

type Client struct {
	sc StoreClient
}

func NewClient(cc grpc.ClientConnInterface) *Client {
	return &Client{sc: NewStoreClient(cc)}
}

func (c *Client) Get(ctx context.Context, ref bs.Ref) (bs.Blob, []bs.Ref, error) {
	resp, err := c.sc.Get(ctx, &GetRequest{Ref: ref[:]})
	// xxx distinguish not-found error
	if err != nil {
		return nil, nil, err
	}

	var types []bs.Ref
	for _, t := range resp.Types {
		types = append(types, bs.RefFromBytes(t))
	}

	return resp.Blob, types, nil
}

func (c *Client) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref, []bs.Ref) error) error {
	lc, err := c.sc.ListRefs(ctx, &ListRefsRequest{Start: start[:]})
	if err != nil {
		return err
	}
	for {
		resp, err := lc.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "receiving response")
		}
		var types []bs.Ref
		for _, t := range resp.Types {
			types = append(types, bs.RefFromBytes(t))
		}
		err = f(bs.RefFromBytes(resp.Ref), types)
		if err != nil {
			return err
		}
	}
}

func (c *Client) Put(ctx context.Context, blob bs.Blob, typ *bs.Ref) (bs.Ref, bool, error) {
	var typeBytes []byte
	if typ != nil {
		typeBytes = (*typ)[:]
	}
	resp, err := c.sc.Put(ctx, &PutRequest{Blob: blob, Type: typeBytes})
	if err != nil {
		return bs.Ref{}, false, err
	}
	return bs.RefFromBytes(resp.Ref), resp.Added, nil
}

func init() {
	store.Register("rpc", func(_ context.Context, conf map[string]interface{}) (bs.Store, error) {
		addr, ok := conf["addr"].(string)
		if !ok {
			return nil, errors.New(`missing "addr" parameter`)
		}
		cc, err := grpc.Dial(addr)
		if err != nil {
			return nil, errors.Wrapf(err, "connecting to %s", addr)
		}
		return NewClient(cc), nil
	})
}
