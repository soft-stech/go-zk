package zk

import (
	"fmt"
	gopath "path"
	"strings"
	"sync"
	"sync/atomic"
)

type CachedLeavesWalker struct {
	conn   *Conn
	events <-chan Event
	active int32
	tree   cachedLeavesTreeNode
	lock   sync.RWMutex
}

func NewCachedLeavesWalker(conn *Conn, path string) (*CachedLeavesWalker, error) {
	events, err := conn.AddWatch(path, true)
	if err != nil {
		return nil, err
	}

	w := &CachedLeavesWalker{
		conn:   conn,
		events: events,
		tree:   make(cachedLeavesTreeNode),
		lock:   sync.RWMutex{},
	}

	go w.eventLoop()
	if err = w.conn.WalkLeaves(path, func(p string, stat *Stat) error {
		w.lock.Lock()
		w.tree.ensurePathPresent(p)
		w.lock.Unlock()
		return nil
	}); err != nil {
		if cerr := w.Close(); cerr != nil {
			w.conn.logger.Printf("failed to close CachedLeavesWalker: %v", cerr)
		}
		return nil, err
	}
	return w, nil
}

func (w *CachedLeavesWalker) Close() error {
	if atomic.LoadInt32(&w.active) != 1 {
		if err := w.conn.RemoveWatch(w.events); err != nil {
			return err
		}
	}
	return nil
}

func (w *CachedLeavesWalker) eventLoop() {
	atomic.StoreInt32(&w.active, 1)
	defer atomic.StoreInt32(&w.active, 0)
	for e := range w.events {
		w.lock.Lock()
		switch e.Type {
		case EventNodeCreated, EventNodeDataChanged:
			w.tree.ensurePathPresent(e.Path)
		case EventNodeDeleted:
			w.tree.ensurePathAbsent(e.Path)
		}
		w.lock.Unlock()
	}
}

func (w *CachedLeavesWalker) WalkLeaves(visitor func(p string) error) error {
	if atomic.LoadInt32(&w.active) != 1 {
		return fmt.Errorf("CachedLeavesWalker not ready yet")
	}
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.walkLeaves("/", w.tree, visitor)
}

func (w *CachedLeavesWalker) walkLeaves(p string, tree cachedLeavesTreeNode, visitor func(p string) error) error {
	if len(tree) > 0 {
		for l := range tree {
			if err := w.walkLeaves(gopath.Join(p, l), tree[l], visitor); err != nil {
				return err
			}
		}
		return nil
	}
	return visitor(p)
}

type cachedLeavesTreeNode map[string]cachedLeavesTreeNode

func (n cachedLeavesTreeNode) ensurePathPresent(path string) {
	nodes := strings.Split(path[1:], "/")
	n.setNodes(nodes)
}

func (n cachedLeavesTreeNode) ensurePathAbsent(path string) {
	nodes := strings.Split(path[1:], "/")
	n.deleteNodes(nodes)
}

func (n cachedLeavesTreeNode) setNodes(nodes []string) {
	if len(nodes) == 0 {
		return
	}

	if _, ok := n[nodes[0]]; !ok {
		n[nodes[0]] = make(cachedLeavesTreeNode)
	}
	n[nodes[0]].setNodes(nodes[1:])
}

func (n cachedLeavesTreeNode) deleteNodes(nodes []string) {
	l := len(nodes)
	switch l {
	case 0:
		return
	case 1:
		if nodes[0] == "" {
			return // Don't delete root
		}
		delete(n, nodes[0])
	default:
		n[nodes[0]].deleteNodes(nodes[1:])
	}
}
