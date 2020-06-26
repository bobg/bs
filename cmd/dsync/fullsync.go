package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
)

type syncPayload struct {
	Dir    *Dir   `json:"dir"`
	Anchor string `json:"anchor"`
}

func (p *primary) fullSync(ctx context.Context, dir string) error {
	var dp Dir

	a, err := p.dirAnchor(dir)
	if err != nil {
		return errors.Wrapf(err, "computing anchor for %s", dir)
	}

	ref, err := p.s.GetAnchor(ctx, a, time.Now())
	if err != nil {
		return errors.Wrapf(err, "getting anchor %s", a)
	}
	err = bs.GetProto(ctx, p.s, ref, &dp)
	if err != nil {
		return errors.Wrapf(err, "getting dir proto for %s (%s)", a, ref)
	}

	j, err := json.Marshal(&dp)
	if err != nil {
		return errors.Wrapf(err, "marshaling dir proto for %s (%s)", a, ref)
	}

	resp, err := http.Post(p.replicaURL+"/sync", "application/json", bytes.NewReader(j))
	if err != nil {
		// xxx
	}
	defer resp.Body.Close()

	// xxx
	return nil
}
