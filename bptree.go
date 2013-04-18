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

type Bptree struct {
	root *indexNode

	maxDegree int

	currDepth int
	maxDepth  int

	allowOverlap bool

	lock *sync.RWMutex

	initialized bool
}

func NewBptree(maxDegree, maxDepth int, allowOverlap bool) (*Bptree, error) {
	if maxDegree < 3 {
		return nil, errors.New("max degree must to have more than 3")
	}

	if maxDepth < 0 {
		return nil, errors.New("max depth must to have zero or a positive value")
	}

	return &Bptree{
		maxDegree:   maxDegree,
		maxDepth:    maxDepth,
		lock:        new(sync.RWMutex),
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
			prev:        nil,
		}

		rnode.children = append(rnode.children, elem)

		tree.root = rnode
		return nil
	}

	// find paths pass by
	paths, err := tree.findToInsert(elem.Key())
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
			// fmt.Println("rebalancing:", paths[:i+1])
			err = tree.balance(paths[:i+1])
			if err != nil {
				return err
			}
		}
	}

	return nil
}

/*
func (tree *Bptree) Remove(key Key) error {
	if !tree.initialized {
		return errors.New("Bptree is not initialized")
	}

	// find paths
	paths, err := tree.findToPlace(key)
	if err != nil {
		return err
	}

}

func (tree *Bptree) Search(key Key) (Elem, bool, error) {

}
*/

func (tree *Bptree) find(key Key, idxAdjust func(*indexNode, int, bool) (int, error)) (paths []*indexNode, err error) {
	paths = make([]*indexNode, 0, tree.maxDepth)

	node := tree.root
	if node == nil {
		panic("yet initialized") // must be never reached
	}

	for node != nil {
		paths = append(paths, node)

		if !node.isInternal {
			break
		}

		elems := node.children

		var isEqual bool = false

		idx := sort.Search(len(elems), func(i int) bool {
			cond := elems[i].Key().CompareTo(key)

			if cond == Equal {
				isEqual = true
			}

			return cond == Equal || cond == Greater
		})

		idx, err = idxAdjust(node, idx, isEqual)
		if err != nil {
			return
		}

		node = elems[idx].(*indexNode)
	}

	return
}

func (tree *Bptree) findToInsert(key Key) (paths []*indexNode, err error) {
	return tree.find(key, func(node *indexNode, idx int, isEqual bool) (int, error) {
		if isEqual && !tree.allowOverlap {
			return 0, errors.New("element overlapped")
		}

		idx -= 1
		if idx < 0 {
			idx = 0
		}

		return idx, nil
	})
}

func (tree *Bptree) findToRemove(key Key) (paths []*indexNode, err error) {
	return tree.find(key, func(node *indexNode, idx int, isEqual bool) (int, error) {
		if !isEqual {
			if !node.isInternal {
				return 0, errors.New("not found")
			}

			idx -= 1
			if idx < 0 {
				idx = 0
			}
		}

		return idx, nil
	})
}

func (tree *Bptree) balance(paths []*indexNode) error {
	lenPaths := len(paths)

	if lenPaths == 0 {
		return errors.New("paths are empty")
	}

	var parent, curr, next *indexNode

	switch {
	case lenPaths == 1: // at root node
		// creating a new root node
		curr = paths[0]

		parent = &indexNode{
			children:    make([]Elem, 0, tree.maxDegree+1),
			depthToLeaf: curr.depthToLeaf + 1,
			isInternal:  true,
			next:        nil,
			prev:        nil,
		}

		parent.children = append(parent.children, curr)
		tree.root = parent

	default:
		parent = paths[lenPaths-2]
		curr = paths[lenPaths-1]
	}

	currChildren := curr.children
	mid := len(currChildren) / 2

	next = &indexNode{
		children:    make([]Elem, len(currChildren)-mid, tree.maxDegree+1),
		depthToLeaf: curr.depthToLeaf,
		isInternal:  curr.isInternal,
		next:        nil,
		prev:        curr,
	}

	curr.children = currChildren[:mid]
	copy(next.children, currChildren[mid:])
	curr.next = next

	newParentChildren, err := parent.children.insert(next, tree.maxDegree, tree.allowOverlap)
	if err != nil {
		return err
	}

	parent.children = newParentChildren

	/*
		fmt.Println("curr:", curr)
		fmt.Println("next:", next)
		fmt.Println("parent:", parent)
	*/

	return nil
}
