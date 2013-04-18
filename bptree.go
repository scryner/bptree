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

func (tree *Bptree) Remove(key Key) error {
	if !tree.initialized {
		return errors.New("Bptree is not initialized")
	}

	// find paths
	paths, err := tree.findToRemove(key)
	if err != nil {
		return err
	}

	// delete the element at belong node
	lastPath := paths[len(paths)-1]

	lastPath.children, err = lastPath.children.delete(key, maxDegree)
	if err != nil {
		return err
	}

	// do balancing if index node has children less than tree.maxDegree / 2
	for i := len(paths) - 1; i >= 0; i-- {
		path := paths[i]

		var allowedDegree int

		if path.isInternal {
			allowedDegree = tree.maxDegree / 2
		} else {
			allowedDegree = (tree.maxDegree - 1) / 2
		}

		if i == 0 {
			if tree.root != path {
				panic("must should be root")
			}

			if len(path.children) == 0 {
				if len(paths) > 1 {
					tree.root = paths[1]
				} else {
					tree.root = nil
				}
			}

			break
		} else {
			if len(path.children) <= allowedDegree {
				ok := tree.redistribution(paths[:i+1], allowedDegree)

				if !ok {
					err = tree.merge(paths[:i+1])
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return
}

/*
func (tree *Bptree) Search(key Key) (Elem, bool, error) {

}
*/

func (tree *Bptree) find(key Key, idxAdjust func(*indexNode, int, bool) (int, error)) (paths []*indexNode, err error) {
	paths = make([]*indexNode, 0, tree.maxDepth)

	node := tree.root
	if node == nil {
		return -1, errors.New("empty tree")
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
			return -1, errors.New("element overlapped")
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
				return -1, errors.New("not found")
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

func (tree *Bptree) redistribution(paths []*indexNode, allowedDegree int) bool {
	lenPaths := len(paths)

	if lenPaths < 1 {
		panic("redistribution must not be in root")
	}

	var parent, curr *indexNode

	parent = paths[lenPaths-2]
	curr = paths[lenPaths-1]

	// get siblings
	lSibling, rSibling := tree.findSiblings(parent, curr)

	var withLeft bool

	switch {
	case lSibling == nil && rSibling == nil:
		panic("no such case")
	case lSibling != nil && rSibling == nil:
		withLeft = true
	case lSibling == nil && rSibling != nil:
		withLeft = false
	default:
		if len(lSibling.children) > len(rSibling.children) {
			withLeft = true
		} else {
			withLeft = false
		}
	}

	if withLeft {
		// redistribution with left sibling
		lsChildrenLen := len(lSibling.children)

		if lsChildrenLen-1 <= allowedDegree {
			return false
		}

		borrow := lSibling.children[lsChildrenLen-1]
		lSibling.children = lSibling.children[:lsChildrenLen-1]

		newChildren := make([]Elem, len(curr.children)+1, tree.maxDegree+1)
		newChildren[0] = borrow
		copy(newChildren[1:], curr.children)

		curr.children = newChildren
	} else {
		// redistribution with right sibling
		rsChildrenLen := len(rSibling.children)

		if rsChildrenLen-1 <= allowedDegree {
			return false
		}

		borrow := rSibling.children[0]
		rSibling.children = rSibling.children[1:]

		curr.children = append(curr.children, borrow)
	}

	return true
}

func (tree *Bptree) merge(paths []*indexNode) error {
	lenPaths := len(paths)

	if lenPaths < 1 {
		panic("merge must not be in root")
	}

	var parent, curr *indexNode

	parent = paths[lenPaths-2]
	curr = paths[lenPaths-1]

	// calculate max children
	var allowedDegree int
	if curr.isInternal {
		allowedDegree = tree.maxDegree
	} else {
		allowedDegree = tree.maxDegree - 1
	}

	// get siblings
	lSibling, rSibling := tree.findSiblings(parent, curr)

	var withLeft bool

	switch {
	case lSibling == nil && rSibling == nil:
		panic("no such case")
	case lSibling != nil && rSibling == nil:
		withLeft = true
	case lSibling == nil && rSibling != nil:
		withLeft = false
	default:
		if len(lSibling.children) <= len(rSibling.children) {
			withLeft = true
		} else {
			withLeft = false
		}
	}

	if withLeft {
		// merging with left sibling
		if len(lSibling.children)+len(curr.children) > allowedDegree {
			panic("number of children must be after merging")
		}

		lSibling.children = append(lSibling.children, curr.children...)
		lSibling.next = curr.next
		curr.next.prev = lSibling

		parent.children, _ := parent.children.delete(curr.Key(), tree.maxDegree)
	} else {
		// merging with right sibling
		if len(rSibling.children)+len(curr.children) > allowedDegree {
			panic("number of children must be after merging")
		}

		rSibling.children = append(curr.children, rSibling.children...)
		rSibling.prev = curr.prev
		curr.prev.next = rSibling

		parent.children, _ := parent.children.delete(curr.Key(), tree.maxDegree)
	}

	return true
}

func (tree *Bptree) findSiblings(parent, curr *indexNode) (left, right *indexNode) {
	pChildrenLen := len(parent.children)

	i, equal := parent.children.find(curr.Key())
	if !equal {
		panic("parent must have the duty of supporting")
	}

	if i != 0 {
		left = parent.children[i-1]
	}

	if i != pChildrenLen-1 {
		right = parent.children(pChildrenLen - 1)
	}

	return
}
