package rpc

import (
	context "context"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
)

var _ StoreServer = &Server{}

type Server struct {
	UnimplementedStoreServer // "All implementations must embed UnimplementedStoreServer for forward compatibility."

	s bs.Store
}

func NewServer(s bs.Store) *Server {
	return &Server{s: s}
}

// TODO: use errors from grpc/status.

func (s *Server) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	blob, types, err := s.s.Get(ctx, bs.RefFromBytes(req.Ref))
	if err != nil {
		return nil, err
	}
	var tbytes [][]byte
	for _, t := range types {
		tbytes = append(tbytes, t[:])
	}
	return &GetResponse{Blob: blob, Types: tbytes}, nil
}

func (s *Server) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	var typ *bs.Ref
	if len(req.Type) > 0 {
		t := bs.RefFromBytes(req.Type)
		typ = &t
	}
	ref, added, err := s.s.Put(ctx, req.Blob, typ)
	if err != nil {
		return nil, err
	}
	return &PutResponse{Ref: ref[:], Added: added}, nil
}

func (s *Server) ListRefs(req *ListRefsRequest, srv Store_ListRefsServer) error {
	return s.s.ListRefs(srv.Context(), bs.RefFromBytes(req.Start), func(ref bs.Ref, types []bs.Ref) error {
		var typeBytes [][]byte
		for _, t := range types {
			typeBytes = append(typeBytes, t[:])
		}
		return srv.Send(&ListRefsResponse{Ref: ref[:], Types: typeBytes})
	})
}

var ErrNotAnchorStore = errors.New("not an anchor store")

func (s *Server) GetAnchor(ctx context.Context, req *GetAnchorRequest) (*GetAnchorResponse, error) {
	astore, ok := s.s.(anchor.Store)
	if !ok {
		return nil, ErrNotAnchorStore
	}

	ref, err := astore.GetAnchor(ctx, req.Name, req.At.AsTime())
	if err != nil {
		return nil, err
	}

	return &GetAnchorResponse{Ref: ref[:]}, nil
}

func (s *Server) ListAnchors(req *ListAnchorsRequest, srv Store_ListAnchorsServer) error {
	astore, ok := s.s.(anchor.Store)
	if !ok {
		return ErrNotAnchorStore
	}

	return astore.ListAnchors(srv.Context(), req.Start, func(name string, ref bs.Ref, at time.Time) error {
		return srv.Send(&ListAnchorsResponse{Name: name, Ref: ref[:], At: timestamppb.New(at)})
	})
}
