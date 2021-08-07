package rpc

import (
	context "context"

	"github.com/pkg/errors"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
	"github.com/bobg/bs/schema"
)

var _ StoreServer = &Server{}

type Server struct {
	UnimplementedStoreServer // "All implementations must embed UnimplementedStoreServer for forward compatibility."

	s bs.Store
}

func NewServer(s bs.Store) *Server {
	return &Server{s: s}
}

func (s *Server) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	blob, err := s.s.Get(ctx, bs.RefFromBytes(req.Ref))
	if errors.Is(err, bs.ErrNotFound) {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &GetResponse{Blob: blob}, err
}

func (s *Server) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	ref, added, err := s.s.Put(ctx, req.Blob)
	if err != nil {
		return nil, err
	}
	return &PutResponse{Ref: ref[:], Added: added}, nil
}

func (s *Server) ListRefs(req *ListRefsRequest, srv Store_ListRefsServer) error {
	return s.s.ListRefs(srv.Context(), bs.RefFromBytes(req.Start), func(ref bs.Ref) error {
		return srv.Send(&ListRefsResponse{Ref: ref[:]})
	})
}

func (s *Server) AnchorMapRef(ctx context.Context, req *AnchorMapRefRequest) (*AnchorMapRefResponse, error) {
	astore, ok := s.s.(anchor.Getter)
	if !ok {
		return nil, status.Error(codes.Unimplemented, anchor.ErrNotAnchorStore.Error())
	}
	ref, err := astore.AnchorMapRef(ctx)
	if errors.Is(err, anchor.ErrNoAnchorMap) {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if err != nil {
		return nil, err
	}
	return &AnchorMapRefResponse{Ref: ref[:]}, nil
}

func (s *Server) UpdateAnchorMap(ctx context.Context, req *UpdateAnchorMapRequest) (*UpdateAnchorMapResponse, error) {
	astore, ok := s.s.(anchor.Store)
	if !ok {
		return nil, status.Error(codes.Unimplemented, anchor.ErrNotAnchorStore.Error())
	}
	err := astore.UpdateAnchorMap(ctx, func(m *schema.Map) (bs.Ref, error) {
		reqOldRef := bs.RefFromBytes(req.OldRef)
		if reqOldRef == (bs.Ref{}) {
			if !m.IsEmpty() {
				return bs.Ref{}, anchor.ErrUpdateConflict
			}
		} else {
			oldRef, err := bs.ProtoRef(m)
			if err != nil {
				return bs.Ref{}, err
			}
			if oldRef != reqOldRef {
				return bs.Ref{}, anchor.ErrUpdateConflict
			}
		}
		return bs.RefFromBytes(req.NewRef), nil
	})
	if errors.Is(err, anchor.ErrUpdateConflict) {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &UpdateAnchorMapResponse{}, err
}
