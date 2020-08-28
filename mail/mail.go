package mail

import (
	"context"
	"strings"

	"github.com/bobg/rmime"
	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/split"
)

func fromRmimePart(ctx context.Context, store bs.Store, part *rmime.Part) (bs.Ref, error) {
	headerRef, err := fromRmimeHeader(ctx, store, part.Header)
	if err != nil {
		return bs.Ref{}, errors.Wrap(err, "storing header")
	}

	var bodyRef bs.Ref

	switch body := part.B.(type) {
	case *rmime.Message:
		bodyRef, err := fromRmimePart(ctx, store, (*rmime.Part)(body))
		if err != nil {
			return bs.Ref{}, errors.Wrap(err, "storing body")
		}

	case *rmime.DeliveryStatus:
	case *rmime.Multipart:
	case string:
		bodyRef, err := split.Write(ctx, store, strings.NewReader(body), nil)
		if err != nil {
			return bs.Ref{}, errors.Wrap(err, "storing body")
		}

	default:
		// xxx error
	}
}
