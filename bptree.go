package bptree

import (
	"errors"
	"sort"
	"sync"
)

type Cond int

const (
	Less    Cond = -1
	Equal        = 0
	Greater      = 1
)

type Key interface {
	CompareTo(k Key) Cond
}

type Elem interface {
	Key() Key
}

type Elems []Elem

func (elems Elems) Len() int { return len(elems) }
func (elems Elems) Less(i, j int) bool {
	cond := elems[i].Key().CompareTo(elems[j].Key())
	return cond == Less
}

func (elems Elems) Swap(i, j int) { elems[i], elems[j] = elems[j], elems[i] }

func (elems Elems) insert(elem Elem, maxDegree int, allowOverlap bool) (Elems, error) {
	var equal bool

	idx := sort.Search(len(elems), func(i int) bool {
		cond := elems[i].Key().CompareTo(elem.Key())
		if cond == Equal {
			equal = true
		}

		return cond == Equal || cond == Greater
	})

	if idx >= len(elems) {
		elems = append(elems, elem)
		return elems, nil
	}

	if equal && !allowOverlap {
		return nil, errors.New("element overlapped")
	}

	newElems := make(Elems, len(elems)+1, maxDegree+1)

	copy(newElems, elems[:idx])
	newElems[idx] = elem
	copy(newElems[i+1:], elems[i:])

	return newElems, nil
}

type Bptree struct {
	root *node

	maxDegree int

	currDepth int
	maxDepth  int

	allowOverlap bool

	lock *sync.Mutex

	initialized bool
}

type indexNode struct {
	children Elems
	next     *indexNode

	isInternal bool

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

func NewBptree(maxDegree, maxDepth int, allowOverlap bool) (*Bptree, error) {
	if maxDegree < 3 {
		return errors.New("max degree must to have more than 3")
	}

	if maxDepth < 0 {
		return errors.New("max depth must to have zero or a positive value")
	}

	return &Bptree{
		maxDegree:   maxDegree,
		maxDepth:    maxDepth,
		lock:        new(sync.Mutex),
		initialized: true,
	}, nil
}

func (tree *Bptree) Insert(elem Elem) error {
	if !tree.initialized {
		return errors.New("Bptree is not initialized")
	}

	// create root node if it is not exist
	if tree.root == nil {
		rnode := &indexNode{
			children:    make([]Elem, 0, tree.maxDegree+1),
			depthToLeaf: 0,
			isInternal:  false,
			next:        nil,
		}

		rnode.children = append(rnode.children, elem)

		tree.root = rnode
		return nil
	}

	// find paths pass by
	paths, err := tree.find(elem.Key())
	if err != nil {
		return err
	}

	// insert element into last index node
	lastPath := paths[len(paths)-1]

	lastPath.children, err = lastPath.children.insert(elem, tree.maxDegree, tree.allowOverlap)
	if err != nil {
		return err
	}

	// do balancing if index node has children more than tree.maxDegree
	for i := len(paths) - 1; i >= 0; i-- {
		path := paths[i]

		var allowedDegree int

		if path.isInternal {
			allowedDegree = tree.maxDegree
		} else {
			allowedDegree = tree.maxDegree - 1
		}

		if len(path.children) > allowedDegree {
			err = tree.balance(paths[:i+1])
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (tree *Bptree) Search(key Key) (Elem, bool, error) {

}

func (tree *Bptree) Remove(key Key) error {

}

func (tree *Bptree) find(key Key) (path []*indexNode, err error) {
	path = make([]*indexNode, 0, tree.maxDepth)

	return
}

func (tree *Bptree) balance(paths []*indexNode) error {
	lenPaths := len(paths)

	switch {
	case lenPaths == 0:
		return errors.New("paths are empty")

	case lenPaths == 1:
		// given paths have only root
		oldRoot := paths[0]

		newRoot := &indexNode{
			children:    make([]Elem, 0, tree.maxDegree+1),
			depthToLeaf: oldRoot.depthToLeaf + 1,
			isInternal:  false,
			next:        nil,
		}

		oldChildren := oldRoot.children
		mid := len(oldChildren) / 2

		next = &indexNode{
			children:    make([]ELem, len(oldChildren)-mid, tree.maxDegree+1),
			depthToLeaf: oldRoot.depthToLeaf,
			isInternal:  oldRoot.isInternal,
			next:        nil,
		}

		oldRoot.children = oldChildren[:mid]
		copy(next.children, oldChildren[mid:])
		oldRoot.next = next

		newRoot.children = append(newRoot.children, oldRoot, next)

	case lenPaths > 1:
		prev := paths[lenPaths-2]
		curr := paths[lenPaths-1]

		if len(curr.children) <= tree.maxDegree {
			// actually, never reached
			return nil
		}

	}

	return nil
}
