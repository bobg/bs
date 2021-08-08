package rpc

import (
	context "context"
	"io"

	"github.com/pkg/errors"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/schema"
	"github.com/bobg/bs/store"
)

var _ anchor.Store = &Client{}

type Client struct {
	sc StoreClient
}

func NewClient(cc grpc.ClientConnInterface) *Client {
	return &Client{sc: NewStoreClient(cc)}
}

func (c *Client) Get(ctx context.Context, ref bs.Ref) (bs.Blob, error) {
	resp, err := c.sc.Get(ctx, &GetRequest{Ref: ref[:]})
	if code := status.Code(err); code == codes.NotFound {
		return nil, bs.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return resp.Blob, nil
}

func (c *Client) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
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
		err = f(bs.RefFromBytes(resp.Ref))
		if err != nil {
			return err
		}
	}
}

func (c *Client) Put(ctx context.Context, blob bs.Blob) (bs.Ref, bool, error) {
	resp, err := c.sc.Put(ctx, &PutRequest{Blob: blob})
	if err != nil {
		return bs.Ref{}, false, err
	}
	return bs.RefFromBytes(resp.Ref), resp.Added, nil
}

func (c *Client) AnchorMapRef(ctx context.Context) (bs.Ref, error) {
	resp, err := c.sc.AnchorMapRef(ctx, &AnchorMapRefRequest{})
	if code := status.Code(err); code == codes.NotFound {
		return bs.Ref{}, anchor.ErrNoAnchorMap
	}
	if err != nil {
		return bs.Ref{}, err
	}
	return bs.RefFromBytes(resp.Ref), nil
}

func (c *Client) UpdateAnchorMap(ctx context.Context, f anchor.UpdateFunc) error {
	var m *schema.Map
	oldRef, err := c.AnchorMapRef(ctx)
	if errors.Is(err, anchor.ErrNoAnchorMap) {
		m = schema.NewMap()
		oldRef = bs.Ref{}
	} else {
		if err != nil {
			return errors.Wrap(err, "getting anchor map ref")
		}
		m, err = schema.LoadMap(ctx, c, oldRef)
		if err != nil {
			return errors.Wrap(err, "loading anchor map")
		}
	}

	newRef, err := f(oldRef, m)
	if err != nil {
		return err
	}

	_, err = c.sc.UpdateAnchorMap(ctx, &UpdateAnchorMapRequest{OldRef: oldRef[:], NewRef: newRef[:]})
	return err
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
