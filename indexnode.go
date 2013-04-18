package bptree

import (
	"fmt"
	"unsafe"
)

type indexNode struct {
	children Elems

	prev *indexNode
	next *indexNode

	isInternal bool

	depthToLeaf int
}

// return smallest key in sub-tree
func (node *indexNode) Key() Key {
	var n *indexNode = node

	for n.isInternal {
		n = n.children[0].(*indexNode)
	}

	return n.children[0].Key()
}

func (node *indexNode) String() string {
	var pKey, nKey string
	if node.prev != nil {
		pKey = fmt.Sprintf("%v", node.prev.Key())
	} else {
		pKey = "nil"
	}

	if node.next != nil {
		nKey = fmt.Sprintf("%v", node.next.Key())
	} else {
		nKey = "nil"
	}

	return fmt.Sprintf("%p{c:%v, p:%v, n:%v, i:%v, d:%d}", unsafe.Pointer(node), node.children, pKey, nKey, node.isInternal, node.depthToLeaf)
}
