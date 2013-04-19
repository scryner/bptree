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

	_tmpKey Key // for temporary empty node (to be deleted soon)
}

// return smallest key in sub-tree
func (node *indexNode) Key() Key {
	if len(node.children) < 1 {
		return node._tmpKey
	}

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

func (node *indexNode) insertElem(elem Elem, maxDegree int, allowOverlap bool) error {
	newChildren, err := node.children.insert(elem, maxDegree, allowOverlap)
	if err != nil {
		return err
	}

	node.children = newChildren
	return nil
}

func (node *indexNode) deleteElem(key Key, maxDegree int) bool {
	newChildren, ok := node.children.delete(key, maxDegree)
	if !ok {
		return false
	}

	if len(newChildren) < 1 {
		node._tmpKey = key
	}

	node.children = newChildren
	return true
}
