package bptree

import (
	"fmt"
	"testing"
)

var _tree *Bptree

const (
	_maxDegree    = 3
	_maxDepth     = 5
	_allowOverlap = false
)

type testKey int

func (k testKey) CompareTo(key Key) Cond {
	k2 := key.(testKey)

	k3 := k - k2

	switch {
	case k3 == 0:
		return Equal
	case k3 > 0:
		return Greater
	case k3 < 0:
		return Less
	}

	panic(`never reached`)
}

type testElem struct {
	val int
}

func (ele *testElem) Key() Key {
	return testKey(ele.val)
}

func TestInit(t *testing.T) {
	var err error

	_tree, err = NewBptree(_maxDegree, _maxDepth, _allowOverlap)
	if err != nil {
		t.Errorf("while creating bptree: %v", err)
		t.FailNow()
	}
}

func TestInsert(t *testing.T) {
	iMax := 10

	t.Logf("inserting 0 to %d (step by %d)", iMax, 2)

	for i := 0; i <= iMax; i += 2 {
		err := _tree.Insert(&testElem{i})
		if err != nil {
			t.Errorf("while inserting to bptree(%d): %v", i, err)
			t.Fail()
		}
	}
}

func TestInternalFindToInsert(t *testing.T) {
	k := testKey(100)

	_, err := _tree.findToInsert(k)
	if err != nil {
		t.Errorf("while find a key(%v): %v", k, err)
		t.Fail()

	}

	/*
		for _, path := range paths {
			fmt.Println("path:", path)

				if !path.isInternal {
					for _, child := range path.children {
						fmt.Println("--", child)
					}
				}
		}
	*/

	// printTree(_tree)

}

/*
func TestInternalFindToRemove(t *testing.T) {
	k := testKey(2)

	paths, err := _tree.findToRemove(k)
	if err != nil {
		t.Errorf("while find a key(%v): %v", k, err)
		t.Fail()
	}

	fmt.Println("root:", _tree.root)

	fmt.Println("paths:", paths)
	printTree(_tree)
}
*/

func TestRemove(t *testing.T) {
	printTree(_tree)
	fmt.Println("root:", _tree.root)

	k := testKey(2)

	err := _tree.Remove(k)
	if err != nil {
		t.Errorf("while removing:", err)
		t.Fail()
	}

	printTree(_tree)
}

func printTree(tree *Bptree) error {
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
		fmt.Println("---", i)

		for _, child := range node.children {
			fmt.Printf("\t%v\n", child)
		}

		node = node.next
		i += 1
	}

	return nil
}

/*
func TestOverlappedinserting(t *testing.T) {
	err := _tree.Insert(&testElem{2})
	if err != nil {
		fmt.Println(err)
	}

	printTree(_tree)
}

*/
