package bptree

import (
	"sync"
)

type Direction int

const (
	ToRight Direction = 1
	ToLeft            = -1
)

type SearchResult struct {
	node *indexNode
	i    int

	matchElem Elem

	treeLock *sync.RWMutex
}

func (res *SearchResult) Elem() Elem {
	return res.matchElem
}

func (res *SearchResult) ElemAt(offset int) (elem Elem, ok bool) {
	var direction Direction
	var totalRemained, remained int
	var node *indexNode
	var children Elems

	// tree read lock
	res.treeLock.RLock()
	defer res.treeLock.RUnlock()

	switch {
	case offset == 0:
		elem = res.Elem()
		ok = true
		return

	case offset > 0:
		direction = ToRight
		totalRemained = offset

		if res.i+1 == len(res.node.children) {
			node = res.node.next
			if node == nil {
				return
			}

			children = node.children
			remained = len(children)
		} else {
			node = res.node
			children = node.children[res.i+1:]
			remained = len(children)
		}

	case offset < 0:
		direction = ToLeft
		totalRemained = -offset

		if res.i == 0 {
			node = res.node.prev
			if node == nil {
				return
			}

			children = node.children
			remained = len(children)
		} else {
			node = res.node
			children = node.children[:res.i]
			remained = len(children)
		}
	}

	for totalRemained > remained {
		totalRemained -= remained

		switch direction {
		case ToRight:
			node = node.next
		case ToLeft:
			node = node.prev
		}

		if node == nil {
			return
		}

		children = node.children
		remained = len(children)
	}

	switch direction {
	case ToRight:
		elem = children[totalRemained-1]
	case ToLeft:
		elem = children[len(children)-totalRemained]
	}

	ok = true

	return
}

func (res *SearchResult) ElemRange(offset int) (elems Elems, n int) {
	var direction Direction
	var totalRemained, remained int
	var node *indexNode
	var children Elems

	// tree read lock
	res.treeLock.RLock()
	defer res.treeLock.RUnlock()

	elems = append(elems, res.node.children[res.i]) // including at least search result
	n = 1

	switch {
	case offset == 0:
		return

	case offset > 0:
		direction = ToRight
		totalRemained = offset

		if res.i+1 == len(res.node.children) {
			node = res.node.next
			if node == nil {
				return
			}

			children = node.children
			remained = len(children)
		} else {
			node = res.node
			children = node.children[res.i+1:]
			remained = len(children)
		}

	case offset < 0:
		direction = ToLeft
		totalRemained = -offset

		if res.i == 0 {
			node = res.node.prev
			if node == nil {
				return
			}

			children = node.children
			remained = len(children)
		} else {
			node = res.node
			children = node.children[:res.i]
			remained = len(children)
		}
	}

	for totalRemained > remained {
		totalRemained -= remained

		switch direction {
		case ToRight:
			elems = append(elems, children...)
			n += len(children)

			node = node.next

		case ToLeft:
			elems = append(children, elems...)
			n += len(children)

			node = node.prev
		}

		if node == nil {
			return
		}

		children = node.children
		remained = len(children)
	}

	switch direction {
	case ToRight:
		remainder := children[:totalRemained]
		elems = append(elems, remainder...)
		n += len(remainder)

	case ToLeft:
		remainder := children[len(children)-totalRemained:]

		elems = append(remainder, elems...)
		n += len(remainder)
	}

	return
}

func (res *SearchResult) ElemRangeTo(key Key, direction Direction, maxN int) (elems Elems, n int) {
	var node *indexNode
	var children Elems

	// tree read lock
	res.treeLock.RLock()
	defer res.treeLock.RUnlock()

	elems = append(elems, res.node.children[res.i]) // including at least search result
	n = 1

	if n > maxN {
		return
	}

	switch direction {
	case ToRight:
		if res.i+1 == len(res.node.children) {
			node = res.node.next
			if node == nil {
				return
			}

			children = node.children
		} else {
			node = res.node
			children = node.children[res.i+1:]
		}

	case ToLeft:
		if res.i == 0 {
			node = res.node.prev
			if node == nil {
				return
			}

			children = node.children
		} else {
			node = res.node
			children = node.children[:res.i]
		}
	}

	var copyN int
	var exit bool

	for {
		copyN = 0

		switch direction {
		case ToRight:
			i, equal := children.find(key)

			if i == len(children) {
				copyN = len(children)
			} else {
				if equal {
					copyN = i + 1
				} else {
					copyN = i
				}

				exit = true
			}

			if n+copyN > maxN {
				copyN = maxN - n
				exit = true
			}

			elems = append(elems, children[:copyN]...)
			n += copyN

			if exit {
				return
			}

			node = node.next

		case ToLeft:
			i, equal := children.find(key)

			if i == 0 && !equal {
				copyN = len(children)
			} else {
				copyN = len(children) - i
				exit = true
			}

			if n+copyN > maxN {
				copyN = maxN - n
				exit = true
			}

			elems = append(children[len(children)-copyN:], elems...)
			n += copyN

			if exit {
				return
			}

			node = node.prev
		}

		if node == nil {
			return
		}

		children = node.children
	}

	panic("never reached")
}
