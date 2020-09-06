package rpc

import (
	context "context"

	"github.com/bobg/bs"
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
