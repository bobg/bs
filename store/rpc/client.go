package rpc

import (
	context "context"
	"io"
	"time"

	"github.com/pkg/errors"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/store"
)

var _ anchor.Store = &Client{}

type Client struct {
	sc StoreClient
}

func NewClient(cc grpc.ClientConnInterface) *Client {
	return &Client{sc: NewStoreClient(cc)}
}

func (c *Client) Get(ctx context.Context, ref bs.Ref) (bs.Blob, []bs.Ref, error) {
	resp, err := c.sc.Get(ctx, &GetRequest{Ref: ref[:]})
	if code := status.Code(err); code == codes.NotFound {
		return nil, nil, bs.ErrNotFound
	}
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

func (c *Client) GetAnchor(ctx context.Context, name string, at time.Time) (bs.Ref, error) {
	resp, err := c.sc.GetAnchor(ctx, &GetAnchorRequest{Name: name, At: timestamppb.New(at)})
	code := status.Code(err)
	switch code {
	case codes.NotFound:
		return bs.Ref{}, bs.ErrNotFound
	case codes.Unimplemented:
		return bs.Ref{}, ErrNotAnchorStore
	}
	if err != nil {
		return bs.Ref{}, err
	}
	return bs.RefFromBytes(resp.Ref), nil
}

func (c *Client) ListAnchors(ctx context.Context, start string, f func(string, bs.Ref, time.Time) error) error {
	lc, err := c.sc.ListAnchors(ctx, &ListAnchorsRequest{Start: start})
	if code := status.Code(err); code == codes.Unimplemented {
		return ErrNotAnchorStore
	}
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
		err = f(resp.Name, bs.RefFromBytes(resp.Ref), resp.At.AsTime())
		if err != nil {
			return err
		}
	}
}

func init() {
	store.Register("rpc", func(_ context.Context, conf map[string]interface{}) (bs.Store, error) {
		addr, ok := conf["addr"].(string)
		if !ok {
			return nil, errors.New(`missing "addr" parameter`)
		}
		insecure, _ := conf["insecure"].(bool)
		var opts []grpc.DialOption
		if insecure {
			opts = append(opts, grpc.WithInsecure())
		}
		cc, err := grpc.Dial(addr, opts...)
		if err != nil {
			return nil, errors.Wrapf(err, "connecting to %s", addr)
		}
		return NewClient(cc), nil
	})
}
