package main

import (
	"bytes"
	"context"
	"net/http"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/dsync"
)

func getSendBlob(ctx context.Context, t *dsync.Tree, ref bs.Ref, replicaURL string) error {
	blob, err := t.S.Get(ctx, ref)
	if err != nil {
		return errors.Wrapf(err, "getting blob %s", ref)
	}
	resp, err := http.Post(replicaURL+"/blob", "application/octet-stream", bytes.NewReader(blob))
	if err != nil {
		return errors.Wrapf(err, "sending POST to %s/blob", replicaURL)
	}
	if resp.Body != nil {
		resp.Body.Close()
	}
	return nil
}
