package zk

import (
	"reflect"
	"testing"
)

func TestTreeWalker(t *testing.T) {
	paths := []string{
		"/gozk-test-walker",
		"/gozk-test-walker/a",
		"/gozk-test-walker/a/b",
		"/gozk-test-walker/a/c",
		"/gozk-test-walker/a/c/d",
	}

	expectVisitedExact := func(t *testing.T, expected []string, visited []string) {
		if !reflect.DeepEqual(expected, visited) {
			t.Fatalf("%s saw unexpected paths:\n Expected: %+v\n Got:      %+v",
				t.Name(), expected, visited)
		}
	}

	WithTestCluster(t, 1, nil, logWriter{t: t, p: "[ZKERR] "}, func(t *testing.T, tc *TestCluster) {
		WithConnectAll(t, tc, func(t *testing.T, c *Conn, _ <-chan Event) {
			for _, p := range paths {
				if path, err := c.Create(p, []byte{1, 2, 3, 4}, 0, WorldACL(PermAll)); err != nil {
					t.Fatalf("Create returned error: %+v", err)
				} else if path != p {
					t.Fatalf("Create returned different path '%s' != '%s'", path, p)
				}
			}

			// Test Walk with visitor.
			t.Run("Walk", func(t *testing.T) {
				w := NewTreeWalker(c.ChildrenCtx, "/gozk-test-walker")

				var visited []string
				err := w.Walk(func(path string, _ *Stat) error {
					visited = append(visited, path)
					return nil
				})
				if err != nil {
					t.Fatalf("Walk returned an error: %+v", err)
				}

				expectVisitedExact(t, paths, visited)
			})

			// Test the walk with channel events.
			t.Run("WalkChan", func(t *testing.T) {
				w := NewTreeWalker(c.ChildrenCtx, "/gozk-test-walker")

				var visited []string
				ch := w.WalkChan(1)
				for e := range ch {
					if e.Err != nil {
						t.Fatalf("WalkChan returned an error: %+v", e.Err)
					}
					visited = append(visited, e.Path)
				}

				expectVisitedExact(t, paths, visited)
			})
		})
	})
}
