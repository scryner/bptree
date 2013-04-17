package bptree

import (
	"sort"
)

type Key interface {
	CompareTo(k Key) (int, error)
}

type Elem interface {
	Key() Key
}

type Bptree struct {
	root *node

	maxDegree int

	currDepth int
	maxDepth  int
}

type indexNode struct {
	children    []Elem
	depthToLeaf int
}

// return smallest key in sub-tree
func (node *indexNode) Key() Key {
	if len(node.children) < 1 {
		panic("must having children")
	}

	var elem Elem
	var n *indexNode
	var ok bool

	for {
		elem = n.children[0]
		n, ok = elem.(*indexNode)

		if !ok {
			return elem.Key()
		}
	}
}

func NewBptree(maxDegree, maxDepth int) *Bptree {
	return &Bptree{
		maxDegree: maxDegree,
		maxDepth:  maxDepth,
	}
}

func (tree *Bptree) Insert(elem Elem) error {

}

func (tree *Bptree) Search(key Key) (Elem, bool, error) {

}

func (tree *Bptree) Remove(key Key) error {

}

func (tree *Bptree) find(key Key) (path []*indexNode, err error) {
	path = make([]*indexNode, 0, tree.maxDepth)

	return
}

func (tree *Bptree) balance(path []*indexNode) error {

}
