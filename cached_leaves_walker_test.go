package zk

import (
	"reflect"
	"testing"
)

func TestCachedLeavesTreeNode(t *testing.T) {
	testCases := []struct {
		setPaths             []string
		expectedLeavesSet    []string
		deletePaths          []string
		expectedLeavesDelete []string
	}{
		{
			setPaths:             []string{"/"},
			expectedLeavesSet:    []string{"/"},
			deletePaths:          []string{"/"},
			expectedLeavesDelete: []string{"/"},
		},
		{
			setPaths: []string{
				"/a/b/c/d",
				"/a/b",
				"/a/c",
				"/a/c/d",
				"/a/e",
			},
			expectedLeavesSet: []string{
				"/a/b/c/d",
				"/a/c/d",
				"/a/e",
			},
			deletePaths: []string{
				"/a/b",
				"/a/e",
				"/unknown",
			},
			expectedLeavesDelete: []string{
				"/a/c/d",
			},
		},
	}
	for _, tc := range testCases {
		w := &CachedLeavesWalker{
			tree:   make(cachedLeavesTreeNode),
			active: 1,
		}

		for _, p := range tc.setPaths {
			w.tree.ensurePathPresent(p)
		}

		visisted := []string{}
		w.WalkLeaves(func(p string) error {
			visisted = append(visisted, p)
			return nil
		})

		if !reflect.DeepEqual(visisted, tc.expectedLeavesSet) {
			t.Errorf("setTree: Expected visisted=%+v, got %+v", tc.expectedLeavesSet, visisted)
		}

		if len(tc.deletePaths) > 0 {
			for _, p := range tc.deletePaths {
				w.tree.ensurePathAbsent(p)
			}

			visisted := []string{}
			w.WalkLeaves(func(p string) error {
				visisted = append(visisted, p)
				return nil
			})

			if !reflect.DeepEqual(visisted, tc.expectedLeavesDelete) {
				t.Errorf("deleteTree: Expected visisted=%+v, got %+v", tc.expectedLeavesDelete, visisted)
			}

		}
	}

}
