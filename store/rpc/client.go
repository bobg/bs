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
	"github.com/bobg/bs/store"
)

var _ anchor.Store = (*Client)(nil)

// Client is the type of a client for Server.
// It implements anchor.Store.
type Client struct {
	sc StoreClient
}

// NewClient produces a new Client capable of communicating with a Server.
// The ClientConnInterface specifies the necessary communication channel
// and can be obtained by calling gprc.Dial or grpc.DialContext (qv).
func NewClient(cc grpc.ClientConnInterface) *Client {
	return &Client{sc: NewStoreClient(cc)}
}

// Get implements bs.Getter.Get.
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

// ListRefs implements bs.Getter.ListRefs.
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

// Put implements bs.Store.Put.
func (c *Client) Put(ctx context.Context, blob bs.Blob) (bs.Ref, bool, error) {
	resp, err := c.sc.Put(ctx, &PutRequest{Blob: blob})
	if err != nil {
		return bs.Zero, false, err
	}
	return bs.RefFromBytes(resp.Ref), resp.Added, nil
}

// Put implements bs.TStore.PutType.
func (c *Client) PutType(ctx context.Context, ref bs.Ref, typ []byte) error {
	_, err := c.sc.PutType(ctx, &PutTypeRequest{Ref: ref[:], Type: typ})
	if code := status.Code(err); code == codes.Unimplemented {
		return bs.ErrNotTStore
	}
	return err
}

// AnchorMapRef implements anchor.Getter.
func (c *Client) AnchorMapRef(ctx context.Context) (bs.Ref, error) {
	resp, err := c.sc.AnchorMapRef(ctx, &AnchorMapRefRequest{})

	code := status.Code(err)
	switch code {
	case codes.NotFound:
		return bs.Zero, anchor.ErrNoAnchorMap
	case codes.Unimplemented:
		return bs.Zero, anchor.ErrNotAnchorStore
	}
	if err != nil {
		return bs.Zero, err
	}
	return bs.RefFromBytes(resp.Ref), nil
}

// UpdateAnchorMap implements anchor.Store.
func (c *Client) UpdateAnchorMap(ctx context.Context, f anchor.UpdateFunc) error {
	oldRef, err := c.AnchorMapRef(ctx)
	if errors.Is(err, anchor.ErrNoAnchorMap) {
		oldRef = bs.Zero
	} else if err != nil {
		return errors.Wrap(err, "getting anchor map ref")
	}

	newRef, err := f(oldRef)
	if err != nil {
		return err
	}

	_, err = c.sc.UpdateAnchorMap(ctx, &UpdateAnchorMapRequest{OldRef: oldRef[:], NewRef: newRef[:]})
	code := status.Code(err)
	switch code {
	case codes.Unimplemented:
		return anchor.ErrNotAnchorStore
	case codes.FailedPrecondition:
		return anchor.ErrUpdateConflict
	}
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
