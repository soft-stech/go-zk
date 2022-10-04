package zk

import (
	"context"
	gopath "path"

	"golang.org/x/sync/errgroup"
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

// InitTreeWalker initializes a TreeWalker with the given fetcher function and root path.
func InitTreeWalker(fetcher ChildrenFunc, path string) TreeWalker {
	return TreeWalker{
		fetcher:     fetcher,
		path:        path,
		includeRoot: true,
		walker:      walkBreadthFirst,
		decorator:   func(v VisitorCtxFunc) VisitorCtxFunc { return v }, // Identity.
	}
}

// TreeWalker provides flexible traversal of a tree of nodes rooted at a specific path.
// The traversal can be configured by calling one of DepthFirst, DepthFirstParallel, BreadthFirst, or BreadthFirstParallel.
// By default, the walker will visit the root node, but this can be changed by calling IncludeRoot.
// The walker can also be configured to only visit leaf nodes by calling LeavesOnly.
type TreeWalker struct {
	fetcher     ChildrenFunc
	path        string
	includeRoot bool
	walker      walkFunc
	decorator   visitorDecoratorFunc
}

// DepthFirst configures the walker for a sequential traversal in depth-first order.
func (w TreeWalker) DepthFirst() TreeWalker {
	return TreeWalker{
		fetcher:     w.fetcher,
		path:        w.path,
		includeRoot: w.includeRoot,
		walker:      walkDepthFirst,
		decorator:   w.decorator,
	}
}

// DepthFirstParallel configures the walker a parallel traversal in depth-first order.
// Note: Parallel traversal will break strict depth-first ordering, since children are visited in parallel.
func (w TreeWalker) DepthFirstParallel() TreeWalker {
	return TreeWalker{
		fetcher:     w.fetcher,
		path:        w.path,
		includeRoot: w.includeRoot,
		walker:      walkDepthFirstParallel,
		decorator:   w.decorator,
	}
}

// BreadthFirst configures the walker for a sequential traversal in breadth-first order.
func (w TreeWalker) BreadthFirst() TreeWalker {
	return TreeWalker{
		fetcher:     w.fetcher,
		path:        w.path,
		includeRoot: w.includeRoot,
		walker:      walkBreadthFirst,
		decorator:   w.decorator,
	}
}

// BreadthFirstParallel configures the walker for a parallel traversal in breadth-first order.
// Note: Parallel traversal will break strict breadth-first ordering, since children are visited in parallel.
func (w TreeWalker) BreadthFirstParallel() TreeWalker {
	return TreeWalker{
		fetcher:     w.fetcher,
		path:        w.path,
		includeRoot: w.includeRoot,
		walker:      walkBreadthFirstParallel,
		decorator:   w.decorator,
	}
}

// IncludeRoot configures the walker to visit the root node or not.
func (w TreeWalker) IncludeRoot(included bool) TreeWalker {
	return TreeWalker{
		fetcher:     w.fetcher,
		path:        w.path,
		includeRoot: included,
		walker:      w.walker,
		decorator:   w.decorator,
	}
}

// LeavesOnly configures the walker to only visit leaf nodes.
func (w TreeWalker) LeavesOnly() TreeWalker {
	return TreeWalker{
		fetcher:     w.fetcher,
		path:        w.path,
		includeRoot: w.includeRoot,
		walker:      w.walker,
		decorator: func(v VisitorCtxFunc) VisitorCtxFunc {
			// Only call the original visitor if the node has no children.
			return func(ctx context.Context, path string, stat *Stat) error {
				if stat.NumChildren == 0 {
					return v(ctx, path, stat)
				}
				return nil
			}
		},
	}
}

// Walk begins traversing the tree and calls the visitor function for each node visited.
// Note: The DepthFirstParallel and BreadthFirstParallel traversals require the visitor function to be thread-safe.
func (w TreeWalker) Walk(visitor VisitorFunc) error {
	// Adapt VisitorFunc to VisitorCtxFunc.
	vc := func(ctx context.Context, path string, stat *Stat) error {
		return visitor(path, stat)
	}
	return w.WalkCtx(context.Background(), vc)
}

// WalkCtx is like Walk, but takes a context that can be used to cancel the walk.
func (w TreeWalker) WalkCtx(ctx context.Context, visitor VisitorCtxFunc) error {
	visitor = w.decorator(visitor) // Apply decorator.
	return w.walker(ctx, w.fetcher, w.path, w.includeRoot, visitor)
}

// WalkChan begins traversing the tree and sends the results to the returned channel.
// The channel will be buffered with the given size.
// The channel is closed when the traversal is complete.
// If an error occurs, an error event will be sent to the channel before it is closed.
func (w TreeWalker) WalkChan(bufferSize int) <-chan VisitEvent {
	return w.WalkChanCtx(context.Background(), bufferSize)
}

// WalkChanCtx is like WalkChan, but it takes a context that can be used to cancel the walk.
func (w TreeWalker) WalkChanCtx(ctx context.Context, bufferSize int) <-chan VisitEvent {
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

// walkFunc is a function that implements a recursive tree traversal.
type walkFunc func(ctx context.Context, fetcher ChildrenFunc, path string, includeSelf bool, visitor VisitorCtxFunc) error

// visitorDecoratorFunc is a function that decorates a visitor function.
type visitorDecoratorFunc func(v VisitorCtxFunc) VisitorCtxFunc

// walkDepthFirst walks the tree rooted at path in depth-first order.
func walkDepthFirst(ctx context.Context, fetcher ChildrenFunc, path string, includeSelf bool, visitor VisitorCtxFunc) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	children, stat, err := fetcher(ctx, path)
	if err != nil {
		if err == ErrNoNode {
			return nil // Ignore ErrNoNode.
		}
		return err
	}

	for _, child := range children {
		childPath := gopath.Join(path, child)
		if err = walkDepthFirst(ctx, fetcher, childPath, true, visitor); err != nil {
			return err
		}
	}

	if includeSelf {
		if err = visitor(ctx, path, stat); err != nil {
			return err
		}
	}

	return nil
}

// walkDepthFirstParallel walks the tree rooted at path in depth-first order, but in parallel.
func walkDepthFirstParallel(ctx context.Context, fetcher ChildrenFunc, path string, includeSelf bool, visitor VisitorCtxFunc) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	children, stat, err := fetcher(ctx, path)
	if err != nil {
		if err == ErrNoNode {
			return nil // Ignore ErrNoNode.
		}
		return err
	}

	eg, egctx := errgroup.WithContext(ctx)
	for _, child := range children {
		childPath := gopath.Join(path, child)
		eg.Go(func() error {
			return walkDepthFirstParallel(egctx, fetcher, childPath, true, visitor)
		})
	}

	if includeSelf {
		eg.Go(func() error {
			return visitor(egctx, path, stat)
		})
	}

	return eg.Wait()
}

// walkBreadthFirst walks the tree rooted at path in breadth-first order.
func walkBreadthFirst(ctx context.Context, fetcher ChildrenFunc, path string, includeSelf bool, visitor VisitorCtxFunc) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	children, stat, err := fetcher(ctx, path)
	if err != nil {
		if err == ErrNoNode {
			return nil // Ignore ErrNoNode.
		}
		return err
	}

	if includeSelf {
		if err = visitor(ctx, path, stat); err != nil {
			return err
		}
	}

	for _, child := range children {
		childPath := gopath.Join(path, child)
		if err = walkBreadthFirst(ctx, fetcher, childPath, true, visitor); err != nil {
			return err
		}
	}

	return nil
}

// walkBreadthFirstParallel walks the tree rooted at path in breadth-first order, but in parallel.
func walkBreadthFirstParallel(ctx context.Context, fetcher ChildrenFunc, path string, includeSelf bool, visitor VisitorCtxFunc) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	children, stat, err := fetcher(ctx, path)
	if err != nil {
		if err == ErrNoNode {
			return nil // Ignore ErrNoNode.
		}
		return err
	}

	eg, egctx := errgroup.WithContext(ctx)

	if includeSelf {
		eg.Go(func() error {
			return visitor(egctx, path, stat)
		})
	}

	for _, child := range children {
		childPath := gopath.Join(path, child)
		eg.Go(func() error {
			return walkBreadthFirstParallel(egctx, fetcher, childPath, true, visitor)
		})
	}

	return eg.Wait()
}
