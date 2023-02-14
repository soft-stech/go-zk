package zk

import (
	"context"
	gopath "path"
)

// ChildrenFunc is a function that returns the children of a node.
type ChildrenFunc func(ctx context.Context, path string) ([]string, *Stat, error)

// VisitorFunc is a function that is called for each node visited.
type VisitorFunc func(path string, stat *Stat) error

// VisitorCtxFunc is like VisitorFunc, but it takes a context.
type VisitorCtxFunc func(ctx context.Context, path string, stat *Stat) error

// VisitEvent is the event that is sent to the channel returned by various walk functions.
// If Err is not nil, it indicates that an error occurred while walking the tree.
type VisitEvent struct {
	Path string
	Stat *Stat
	Err  error
}

// NewTreeWalker creates a new TreeWalker with the given fetcher function and root path.
func NewTreeWalker(fetcher ChildrenFunc, path string) *TreeWalker {
	return &TreeWalker{
		fetcher: fetcher,
		path:    path,
	}
}

// TreeWalker provides traversal of a tree of nodes rooted at a specific path.
type TreeWalker struct {
	fetcher ChildrenFunc
	path    string
}

// Walk begins traversing the tree and calls the visitor function for each node visited.
// Note: The DepthFirstParallel and BreadthFirstParallel traversals require the visitor function to be thread-safe.
func (w *TreeWalker) Walk(visitor VisitorFunc) error {
	// Adapt VisitorFunc to VisitorCtxFunc.
	vc := func(ctx context.Context, path string, stat *Stat) error {
		return visitor(path, stat)
	}
	return w.WalkCtx(context.Background(), vc)
}

// WalkCtx is like Walk, but takes a context that can be used to cancel the walk.
func (w *TreeWalker) WalkCtx(ctx context.Context, visitor VisitorCtxFunc) error {
	return w.walkBreadthFirst(ctx, w.path, visitor)
}

// WalkChan begins traversing the tree and sends the results to the returned channel.
// The channel will be buffered with the given size.
// The channel is closed when the traversal is complete.
// If an error occurs, an error event will be sent to the channel before it is closed.
func (w *TreeWalker) WalkChan(bufferSize int) <-chan VisitEvent {
	return w.WalkChanCtx(context.Background(), bufferSize)
}

// WalkChanCtx is like WalkChan, but it takes a context that can be used to cancel the walk.
func (w *TreeWalker) WalkChanCtx(ctx context.Context, bufferSize int) <-chan VisitEvent {
	ch := make(chan VisitEvent, bufferSize)
	visitor := func(ctx context.Context, path string, stat *Stat) error {
		ch <- VisitEvent{Path: path, Stat: stat}
		return nil
	}
	go func() {
		defer close(ch)
		if err := w.WalkCtx(ctx, visitor); err != nil {
			ch <- VisitEvent{Err: err}
		}
	}()
	return ch
}

// walkBreadthFirst walks the tree rooted at path in breadth-first order.
func (w *TreeWalker) walkBreadthFirst(ctx context.Context, path string, visitor VisitorCtxFunc) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	children, stat, err := w.fetcher(ctx, path)
	if err != nil {
		if err == ErrNoNode {
			return nil // Ignore ErrNoNode.
		}
		return err
	}

	if err = visitor(ctx, path, stat); err != nil {
		return err
	}

	for _, child := range children {
		childPath := gopath.Join(path, child)
		if err = w.walkBreadthFirst(ctx, childPath, visitor); err != nil {
			return err
		}
	}

	return nil
}
