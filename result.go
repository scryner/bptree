package bptree

type Direction int

const (
	ToRight Direction = 1
	ToLeft            = -1
)

type SearchResult struct {
	node *indexNode
	i    int
}

func (res *SearchResult) Elem() Elem {
	elem, ok := res.ElemAt(0)
	if !ok {
		panic("must to be searched")
	}

	return elem
}

func (res *SearchResult) ElemAt(offset int) (elem Elem, ok bool) {
	switch {
	case offset == 0:
		elem = res.node.children[res.i]
		ok = true

	case offset > 0:
		totalRemained := offset

		var node *indexNode
		var children Elems
		var remained int

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

		for totalRemained > remained {
			totalRemained -= remained

			node = node.next
			if node == nil {
				return
			}

			children = node.children
			remained = len(children)
		}

		elem = children[totalRemained-1]
		ok = true

	case offset < 0:
		// todo from here
	}

	return
}

func (res *SearchResult) ElemRange(offset int) (elems Elems, n int) {
	/*
		switch {
		case offset == 0:
			elems = append(elems, res.Elem())
			n = 1

		case offset > 0:
			node := res.node[res.i+1:]

			for i := 0; i < offset; i++ {

			}

		case offset < 0:

		}
	*/

	return
}

func (res *SearchResult) ElemRangeTo(key Key, direction Direction, maxN int) (elems Elems, n int) {

	return
}
