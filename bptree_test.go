package bptree

import (
	"fmt"
	"testing"
)

var _tree *Bptree

const (
	_maxDegree    = 5
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

func TestInit(t *testing.T) {
	var err error

	_tree, err = NewBptree(_maxDegree, _maxDepth, _allowOverlap)
	if err != nil {
		t.Errorf("while creating bptree: %v", err)
		t.FailNow()
	}
}

func TestInsert(t *testing.T) {
	iMax := 100

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
	// printTree(_tree)
	// fmt.Println("root:", _tree.root)

	k := testKey(2)

	err := _tree.Remove(k)
	if err != nil {
		t.Errorf("while removing:", err)
		t.Fail()
	}

	// printTree(_tree)
}

func TestSearchElem(t *testing.T) {
	k1 := testKey(2)
	k2 := testKey(50)

	_, ok, err := _tree.SearchElem(k1)
	if err != nil {
		t.Errorf("while searching elem:", err)
		t.Fail()
	}

	if ok {
		t.Errorf("element must be not found")
		t.Fail()
	}

	elem, ok, err := _tree.SearchElem(k2)
	if err != nil {
		t.Errorf("while searching:", err)
		t.Fail()
	}

	if !ok {
		t.Errorf("element must be found")
		t.Fail()
	}

	cond := k2.CompareTo(elem.Key())
	if cond != Equal {
		t.Errorf("element must be same")
		t.Fail()
	}
}

func TestSearch(t *testing.T) {
	k := testKey(50)

	res, ok, err := _tree.Search(k)
	if err != nil {
		t.Errorf("while searching:", err)
		t.Fail()
	}

	if !ok {
		t.Errorf("element must be found")
		t.Fail()
	}

	elem, ok := res.ElemAt(-24)
	if !ok {
		t.Errorf("element must be found")
		t.Fail()
	}

	fmt.Println("---------", elem)

	elems, n := res.ElemRange(100)
	/*
		if n != 11 {
			t.Errorf("elemnts length is not matched")
			t.Fail()
		}
	*/

	fmt.Println(elems, n)
	fmt.Println(_tree.root.depthToLeaf)

	elems, n = res.ElemRangeTo(testKey(71), ToRight, 100)
	fmt.Println("!!!", elems, n)

	elems, n = res.ElemRangeTo(testKey(21), ToLeft, 100)
	fmt.Println("!!!", elems, n)
}

func TestSearchNearby(t *testing.T) {
	k := testKey(31)

	elem, equal, err := _tree.SearchElemNearby(k, ToLeft)
	if err != nil {
		t.Errorf("searching nearby err:", err)
		t.Fail()
	}

	fmt.Println("%%%%", elem, equal)
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
