// Package store is a registry for Store factories.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
)

// Factory is a function that can create a blob store from a configuration object.
type Factory = func(context.Context, map[string]interface{}) (bs.Store, error)

var registry = make(map[string]Factory)

// Register registers f as a factory for creating blob stores of the type named by key.
func Register(key string, f Factory) {
	registry[key] = f
}

// Create creates a bs.Store of the type indicated by key,
// using the supplied configuration.
func Create(ctx context.Context, key string, conf map[string]interface{}) (bs.Store, error) {
	f, ok := registry[key]
	if !ok {
		return nil, fmt.Errorf("key %s not found in registry", key)
	}
	return f(ctx, conf)
}

// FromConfigFile loads a config file in JSON format from the given filename.
// It creates a bs.Store of the type indicated by its `type` key.
// The rest of the JSON object must be the config for a store of the given type.
func FromConfigFile(ctx context.Context, filename string) (bs.Store, error) {
	var conf map[string]interface{}
	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "opening %s", filename)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.UseNumber()
	err = dec.Decode(&conf)
	if err != nil {
		return nil, errors.Wrapf(err, "decoding %s", filename)
	}

	typ, ok := conf["type"].(string)
	if !ok {
		return nil, fmt.Errorf("config file %s missing `type` parameter", filename)
	}

	return Create(ctx, typ, conf)
}
