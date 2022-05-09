// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ServeMux is a multiplexer for asynchronous tasks.
// It matches the type of each task against a list of registered patterns
// and calls the handler for the pattern that most closely matches the
// task's type name.
//
// Longer patterns take precedence over shorter ones, so that if there are
// handlers registered for both "images" and "images:thumbnails",
// the latter handler will be called for tasks with a type name beginning with
// "images:thumbnails" and the former will receive tasks with type name beginning
// with "images".
type ServeMux struct {
	mu  sync.RWMutex
	m   map[string]*muxEntry
	es  []*muxEntry // slice of entries sorted from longest to shortest.
	mws []MiddlewareFunc
}

type muxEntry struct {
	hs      []Handler
	pattern string
}

func (e *muxEntry) Append(h Handler) {
	e.hs = append(e.hs, h)
}

// MiddlewareFunc is a function which receives an asynq.Handler and returns another asynq.Handler.
// Typically, the returned handler is a closure which does something with the context and task passed
// to it, and then calls the handler passed as parameter to the MiddlewareFunc.
type MiddlewareFunc func(Handler) Handler

// NewServeMux allocates and returns a new ServeMux.
func NewServeMux() *ServeMux {
	return new(ServeMux)
}

// ProcessTask dispatches the task to the handler whose
// pattern most closely matches the task type.
func (mux *ServeMux) ProcessTask(ctx context.Context, task *Task) error {
	hs, _ := mux.Handler(task)
	for _, h := range hs {
		if err := h.ProcessTask(ctx, task); err != nil {
			return err
		}
	}
	return nil
}

// Handler returns the handler to use for the given task.
// It always return a non-nil handler.
//
// Handler also returns the registered pattern that matches the task.
//
// If there is no registered handler that applies to the task,
// handler returns a 'not found' handler which returns an error.
func (mux *ServeMux) Handler(t *Task) (hs []Handler, pattern string) {
	mux.mu.RLock()
	defer mux.mu.RUnlock()

	hs, pattern = mux.match(t.Type())
	if hs == nil {
		hs, pattern = NotFoundHandler(), ""
	}
	for j, h := range hs {
		for i := len(mux.mws) - 1; i >= 0; i-- {
			hs[j] = mux.mws[i](h)
		}
	}

	return hs, pattern
}

// Find a handler on a handler map given a typename string.
// Most-specific (longest) pattern wins.
func (mux *ServeMux) match(typename string) (hs []Handler, pattern string) {
	// Check for exact match first.
	v, ok := mux.m[typename]
	if ok {
		return v.hs, v.pattern
	}

	// Check for longest valid match.
	// mux.es contains all patterns from longest to shortest.
	for _, e := range mux.es {
		if strings.HasPrefix(typename, e.pattern) {
			return e.hs, e.pattern
		}
	}
	return nil, ""

}

// Handle registers the handler for the given pattern.
// If a handler already exists for pattern, Handle panics.
func (mux *ServeMux) Handle(pattern string, handler Handler) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if strings.TrimSpace(pattern) == "" {
		panic("asynq: invalid pattern")
	}
	if handler == nil {
		panic("asynq: nil handler")
	}

	if mux.m == nil {
		mux.m = make(map[string]*muxEntry)
	}

	if _, exist := mux.m[pattern]; !exist {
		me := &muxEntry{pattern: pattern}
		mux.m[pattern] = me
		mux.es = appendSorted(mux.es, me)
	}
	e := mux.m[pattern]
	e.Append(handler)
}

func appendSorted(es []*muxEntry, e *muxEntry) []*muxEntry {
	n := len(es)
	i := sort.Search(n, func(i int) bool {
		return len(es[i].pattern) < len(e.pattern)
	})
	if i == n {
		return append(es, e)
	}
	// we now know that i points at where we want to insert.
	es = append(es, &muxEntry{}) // try to grow the slice in place, any entry works.
	copy(es[i+1:], es[i:])       // shift shorter entries down.
	es[i] = e
	return es
}

// HandleFunc registers the handler function for the given pattern.
func (mux *ServeMux) HandleFunc(pattern string, handler func(context.Context, *Task) error) {
	if handler == nil {
		panic("asynq: nil handler")
	}
	mux.Handle(pattern, HandlerFunc(handler))
}

// Use appends a MiddlewareFunc to the chain.
// Middlewares are executed in the order that they are applied to the ServeMux.
func (mux *ServeMux) Use(mws ...MiddlewareFunc) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	for _, fn := range mws {
		mux.mws = append(mux.mws, fn)
	}
}

// NotFound returns an error indicating that the handler was not found for the given task.
func NotFound(ctx context.Context, task *Task) error {
	return fmt.Errorf("handler not found for task %q", task.Type())
}

// NotFoundHandler returns a simple task handler that returns a ``not found`` error.
func NotFoundHandler() []Handler { return []Handler{HandlerFunc(NotFound)} }
