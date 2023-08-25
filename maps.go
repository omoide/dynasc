package dynasc

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// MapToMap converts a map to another map with the specified function.
func MapToMap[S comparable, T any, U comparable, V any](elements map[S]T, fn func(S, T) (U, V)) map[U]V {
	results := make(map[U]V, len(elements))
	for k, v := range elements {
		kk, vv := fn(k, v)
		results[kk] = vv
	}
	return results
}

// MapToMapE converts a map to another map with the specified function that can return an error.
func MapToMapE[S comparable, T any, U comparable, V any](elements map[S]T, fn func(S, T) (U, V, error)) (map[U]V, error) {
	results := make(map[U]V, len(elements))
	for k, v := range elements {
		kk, vv, err := fn(k, v)
		if err != nil {
			return nil, errors.Wrap(err, "failed to execute a callback function")
		}
		results[kk] = vv
	}
	return results, nil
}

// ParallelMapToMap converts a map to another map in parallel with the specified function.
func ParallelMapToMap[S comparable, T any, U comparable, V any](elements map[S]T, fn func(S, T) (U, V)) map[U]V {
	results := make(map[U]V, len(elements))
	mu := sync.RWMutex{}
	wg := &sync.WaitGroup{}
	for k, v := range elements {
		wg.Add(1)
		go func(k S, v T) {
			kk, vv := fn(k, v)
			mu.Lock()
			defer mu.Unlock()
			results[kk] = vv
			wg.Done()
		}(k, v)
	}
	wg.Wait()
	return results
}

// ParallelMapToMapE converts a map to another map in parallel with the specified function that can return an error.
func ParallelMapToMapE[S comparable, T any, U comparable, V any](ctx context.Context, elements map[S]T, fn func(context.Context, S, T) (U, V, error)) (map[U]V, error) {
	results := make(map[U]V, len(elements))
	mu := sync.RWMutex{}
	eg, ctx := errgroup.WithContext(ctx)
	for k, v := range elements {
		eg.Go(func(k S, v T) func() error {
			return func() error {
				kk, vv, err := fn(ctx, k, v)
				if err != nil {
					return errors.Wrap(err, "failed to execute a callback function")
				}
				mu.Lock()
				defer mu.Unlock()
				results[kk] = vv
				return nil
			}
		}(k, v))
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}
