package store

import (
	"context"
	"fmt"

	"github.com/bobg/bs"
)

// Factory is a function that can create a blob store from a configuration object.
type Factory func(context.Context, map[string]interface{}) (bs.Store, error)

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
