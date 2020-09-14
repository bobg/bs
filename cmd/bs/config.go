package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store"
)

func storeFromConfig(ctx context.Context, filename string) (bs.Store, error) {
	var conf map[string]interface{}
	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "opening config file %s", filename)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.UseNumber()
	err = dec.Decode(&conf)
	if err != nil {
		return nil, errors.Wrapf(err, "decoding config file %s", filename)
	}

	typ, ok := conf["type"].(string)
	if !ok {
		return nil, fmt.Errorf("config file %s missing `type` parameter", filename)
	}

	return store.Create(ctx, typ, conf)
}
