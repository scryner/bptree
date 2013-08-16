package bptree

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

func PrintTreeToWriter(tree *Bptree, w io.Writer) error {
	node := tree.root

	for node != nil {
		if !node.isInternal {
			break
		}

		node = node.children[0].(*indexNode)
	}

	if node == nil {
		return fmt.Errorf("tree is nil")
	}

	i := 0

	for node != nil {
		fmt.Fprintln(w, "leaf ---", i)

		for _, child := range node.children {
			fmt.Fprintf(w, "\t%v\n", child)
		}

		node = node.next
		i += 1
	}

	return nil
}

// for debug
func PrintTree(tree *Bptree) error {
	return PrintTreeToWriter(tree, os.Stdout)
}

func printTreeToString(tree *Bptree) (s string, err error) {
	buf := new(bytes.Buffer)

	err = PrintTreeToWriter(tree, buf)
	if err != nil {
		return
	}

	s = buf.String()

	return
}
