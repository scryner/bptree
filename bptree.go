package bptree

import (
	"errors"
	"fmt"
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
	CompareTo(key Key) Cond
}

type Elem interface {
	Key() Key
}

var (
	// errors
	ERR_NOT_INITIALIZED    = errors.New("Bptree is not initialized")
	ERR_EMPTY              = errors.New("empty tree")
	ERR_NOT_FOUND          = errors.New("not found")
	ERR_OVERLAPPED         = errors.New("element overlapped")
	ERR_EXCEED_MAX_DEPTH   = errors.New("tree reached to max depth")
	ERR_SEARCH_OVERFLOWED  = errors.New("search overflowed")
	ERR_SEARCH_UNDERFLOWED = errors.New("search underflowed")
)

type Bptree struct {
	root *indexNode

	maxDegree int
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
		maxDegree:    maxDegree,
		maxDepth:     maxDepth,
		allowOverlap: allowOverlap,
		lock:         new(sync.RWMutex),
		initialized:  true,
	}, nil
}

func (tree *Bptree) Insert(elem Elem) error {
	if !tree.initialized {
		return ERR_NOT_INITIALIZED
	}

	// write lock
	tree.lock.Lock()
	defer tree.lock.Unlock()

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

	// check current node depth, actually tree could have tree.maxDepth + 1
	if tree.root.depthToLeaf > tree.maxDepth {
		return ERR_EXCEED_MAX_DEPTH
	}

	// find paths pass by
	paths, err := tree.findToInsert(elem.Key())
	if err != nil {
		return err
	}

	// insert element into last index node
	lastPath := paths[len(paths)-1]

	err = lastPath.insertElem(elem, tree.maxDegree, tree.allowOverlap)
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

func (tree *Bptree) Remove(key Key) error {
	if !tree.initialized {
		return ERR_NOT_INITIALIZED
	}

	// lock
	tree.lock.Lock()
	defer tree.lock.Unlock()

	// find paths
	paths, err := tree.findToExactElem(key)
	if err != nil {
		return err
	}

	lenPaths := len(paths)

	// if only root is existed
	if lenPaths == 1 {
		root := paths[0]

		ok := root.deleteElem(key, tree.maxDegree)
		if !ok {
			treePrinted, _ := printTreeToString(tree)
			panic(fmt.Sprintf("element must be existed in root\n%s\n", treePrinted))
		}

		return nil
	}

	var allowedDegree int
	var curr *indexNode

	// do balancing if index node has children less than tree.maxDegree / 2
	for i := lenPaths - 1; i >= 0; i-- {
		curr = paths[i]

		if i == 0 { // at root
			if tree.root != curr {
				panic("must should be root")
			}

			if len(curr.children) == 0 {
				if len(paths) > 1 {
					tree.root = paths[1]
				} else {
					tree.root = nil
				}
			}

			return nil
		}

		if curr.isInternal {
			allowedDegree = tree.maxDegree / 2
		} else {
			allowedDegree = (tree.maxDegree - 1) / 2
		}

		if i == lenPaths-1 { // at first loop (last node in paths)
			// delete the element at belong node
			var ok bool
			ok = curr.deleteElem(key, tree.maxDegree)
			if !ok {
				treePrinted, _ := printTreeToString(tree)
				panic(fmt.Sprintf("element must be existed\n%s\n", treePrinted))
			}
		}

		if len(curr.children) < allowedDegree {
			ok := tree.redistribution(paths[:i+1], allowedDegree)

			if !ok {
				err = tree.merge(paths[:i+1])
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (tree *Bptree) SearchElem(key Key) (elem Elem, ok bool, err error) {
	var res *SearchResult

	res, ok, err = tree.Search(key)
	if err != nil || !ok {
		return
	}

	elem = res.Elem()

	return
}

func (tree *Bptree) SearchElemNearby(key Key, direction Direction) (elem Elem, equal bool, err error) {
	var res *SearchResult

	res, equal, err = tree.SearchNearby(key, direction)
	if err != nil {
		return
	}

	elem = res.Elem()

	return
}

func (tree *Bptree) SearchNearby(key Key, direction Direction) (res *SearchResult, equal bool, err error) {
	if !tree.initialized {
		err = ERR_NOT_INITIALIZED
		return
	}

	// read lock
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	var elem Elem

	// find paths
	paths, _ := tree.findToExactElem(key)

	if len(paths) == 0 {
		err = ERR_EMPTY
		return
	}

	node := paths[len(paths)-1]

	i, equal := node.children.find(key)
	if equal {
		elem = node.children[i]
	} else {
		switch direction {
		case ToRight:
			if i == len(node.children) {
				if node.next == nil {
					err = ERR_SEARCH_OVERFLOWED
					return
				}

				elem = node.next.children[0]
			} else {
				elem = node.children[i]
			}

		case ToLeft:
			if i == 0 {
				if node.prev == nil {
					err = ERR_SEARCH_UNDERFLOWED
					return
				}

				node = node.prev
				i = len(node.children) - 1
				elem = node.prev.children[i]
			} else {
				i -= 1
				elem = node.children[i]
			}
		}
	}

	res = &SearchResult{
		node:      node,
		i:         i,
		matchElem: elem,
		treeLock:  tree.lock,
	}

	return
}

func (tree *Bptree) Search(key Key) (res *SearchResult, ok bool, err error) {
	if !tree.initialized {
		err = ERR_NOT_INITIALIZED
		return
	}

	// read lock
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	// find paths
	paths, e := tree.findToExactElem(key)
	if e != nil {
		if e != ERR_NOT_FOUND {
			err = e
			return
		} else {
			return
		}
	}

	if len(paths) == 0 {
		err = ERR_EMPTY
		return
	}

	node := paths[len(paths)-1]

	i, equal := node.children.find(key)
	if !equal {
		return
	}

	res = &SearchResult{
		node:      node,
		i:         i,
		matchElem: node.children[i],
		treeLock:  tree.lock,
	}

	ok = true

	return
}

func (tree *Bptree) find(key Key, idxAdjust func(*indexNode, int, bool) (int, error)) (paths []*indexNode, err error) {
	paths = make([]*indexNode, 0, tree.maxDepth)

	node := tree.root
	if node == nil {
		return nil, ERR_EMPTY
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
			return -1, ERR_OVERLAPPED
		}

		idx -= 1
		if idx < 0 {
			idx = 0
		}

		return idx, nil
	})
}

func (tree *Bptree) findToExactElem(key Key) (paths []*indexNode, err error) {
	return tree.find(key, func(node *indexNode, idx int, isEqual bool) (int, error) {
		if !isEqual {
			if !node.isInternal {
				return -1, ERR_NOT_FOUND
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
		return ERR_EMPTY
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
		next:        curr.next,
		prev:        curr,
	}

	curr.children = currChildren[:mid]
	copy(next.children, currChildren[mid:])
	curr.next = next

	if next.next != nil {
		next.next.prev = next
	}

	err := parent.insertElem(next, tree.maxDegree, tree.allowOverlap)
	if err != nil {
		return err
	}

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
	lSibling, rSibling := tree.findSiblings(parent, curr.Key())

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
	lSibling, rSibling := tree.findSiblings(parent, curr.Key())

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

		if curr.next != nil {
			curr.next.prev = lSibling
		}

		parent.deleteElem(curr.Key(), tree.maxDegree)
	} else {
		// merging with right sibling
		if len(rSibling.children)+len(curr.children) > allowedDegree {
			panic("number of children must be after merging")
		}

		rSibling.children = append(curr.children, rSibling.children...)
		rSibling.prev = curr.prev

		if curr.prev != nil {
			curr.prev.next = rSibling
		}

		parent.deleteElem(curr.Key(), tree.maxDegree)
	}

	return nil
}

func (tree *Bptree) findSiblings(parent *indexNode, key Key) (left, right *indexNode) {
	pChildrenLen := len(parent.children)

	i, equal := parent.children.find(key)
	if !equal {
		panic("parent must have the duty of supporting")
	}

	if i != 0 {
		left = parent.children[i-1].(*indexNode)
	}

	if i != pChildrenLen-1 {
		right = parent.children[i+1].(*indexNode)
	}

	return
}
