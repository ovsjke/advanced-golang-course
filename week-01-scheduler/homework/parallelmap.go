// SPDX-License-Identifier: Apache-2.0

// Package parallelmap applies a function to every element of a slice, in
// parallel, with a bound on how many calls run at once.
package parallelmap

import (
	"context"
	"sync"
	"sync/atomic"
)

// Map calls f on every element of in and returns the results in order:
// out[i] is the result of f(ctx, in[i]). f is called exactly once per element.
//
// At most workers calls of f run at once, and Map keeps workers calls running
// as long as there are elements it has not started. The number of goroutines
// Map starts depends on workers, not on len(in).
//
// Every call of f gets a context derived from ctx. If a call returns an
// error, Map cancels that context, starts no more calls, and returns nil and
// the error. If several calls fail, Map returns the error that happened first
// in time. If ctx is cancelled, Map stops the same way and returns nil and
// ctx.Err(), or the first error from f if there was one. If ctx is cancelled
// after every call has already succeeded, Map may return either the results
// or ctx.Err().
//
// Map returns promptly once it stops, provided f returns when its context is
// cancelled. When Map returns, no call of f is still running and no goroutine
// it started is still alive.
//
// Edge cases, checked in this order:
//   - workers < 1: Map panics without calling f.
//   - ctx is already done: Map returns nil and ctx.Err() without calling f.
//   - len(in) == 0: Map returns an empty slice (nil is fine) and nil.
//
// workers > len(in) is allowed. ctx must not be nil, and f must not panic.

//USING CHANEL:
// func Map[T, R any](ctx context.Context, in []T, workers int, f func(context.Context, T) (R, error)) ([]R, error) {
// 	workers = min(workers,len(in))
// 	var _ atomic.Int64 //so as not delete atomic from import
// 	if workers < 1 {
// 		panic("workers < 1")
// 	}

// 	if ctx.Err() != nil {
// 		return nil, ctx.Err()
// 	}
// 	if len(in) == 0 {
// 		return nil, nil
// 	}

// 	type Task struct {
// 		index int
// 		value T
// 	}
// 	var wg sync.WaitGroup
// 	var firstErr error
// 	var once sync.Once
// 	ctx, cancel := context.WithCancel(ctx)
// 	defer cancel()
// 	tasks := make(chan Task, workers)
// 	out := make([]R, len(in))
// 	for range workers {
// 		wg.Add(1)
// 		go func(ctx context.Context) {
// 			defer wg.Done()
// 			for {
// 				select {
// 				case task, ok := <-tasks:
// 					if !ok || ctx.Err() != nil {
// 						return
// 					}
// 					r, err := f(ctx, task.value)
// 					if err != nil {
// 						once.Do(func() {
// 							firstErr = err
// 							cancel()
// 						})
// 						return
// 					}
// 					out[task.index] = r

// 				case <-ctx.Done():
// 					return
// 				}
// 			}
// 		}(ctx)
// 	}

// broker:
// 	for i, v := range in {
// 		select {
// 		case <-ctx.Done():
// 			break broker
// 		case tasks <- Task{index: i, value: v}:
// 		}
// 	}
// 	close(tasks)
// 	wg.Wait()
// 	if firstErr != nil {
// 		return nil, firstErr
// 	}

// 	if ctx.Err() != nil {
//     return nil, ctx.Err()
// }
// 	return out, nil
// }






//USING ATOMIC:
func Map[T, R any](ctx context.Context, in []T, workers int, f func(context.Context, T) (R, error)) ([]R, error) {
	if workers < 1 {
		panic("workers < 1")
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(in) == 0 {
		return nil, nil
	}
	workers = min(workers, len(in))
	var wg sync.WaitGroup
	var firstErr error
	var once sync.Once
	var next atomic.Int64
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	out := make([]R, len(in))
	for range workers {
		wg.Add(1)
		go func(ctx context.Context) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				i := next.Add(1) - 1
				if i >= int64(len(in)) || ctx.Err() != nil {
					return
				}
				r, err := f(ctx, in[i])
				if err != nil {
					once.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
				out[i] = r
			}
		}(ctx)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return out, nil
}
