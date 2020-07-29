package store

import (
	"context"
	"fmt"

	"github.com/bobg/bs"
)

type Factory func(context.Context, map[string]interface{}) (bs.Store, error)

var registry = make(map[string]Factory)

func Register(key string, f Factory) {
	registry[key] = f
}

func Create(ctx context.Context, key string, conf map[string]interface{}) (bs.Store, error) {
	f, ok := registry[key]
	if !ok {
		return nil, fmt.Errorf("key %s not found in registry", key)
	}
	return f(ctx, conf)
}
