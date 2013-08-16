package bptree

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)

var _tree *Bptree
var _array []int
var _array2 []int

const (
	_maxDegree    = 32
	_maxDepth     = 16
	_allowOverlap = true

	_n_elems = 100000
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

func (ele *testElem) String() string {
	return fmt.Sprintf("%d", ele.val)
}

func TestInit(t *testing.T) {
	// creating test workload randomly arranged array
	_array = make([]int, 0, _n_elems)

	for i := 0; i < _n_elems; i++ {
		_array = append(_array, rand.Int())
	}

	var err error

	_tree, err = NewBptree(_maxDegree, _maxDepth, _allowOverlap)
	if err != nil {
		t.Errorf("while creating bptree: %v", err)
		t.FailNow()
	}
}

func TestInsert(t *testing.T) {
	t.Logf("inserting 0 to %d", _n_elems)

	for i := 0; i < _n_elems; i++ {
		err := _tree.Insert(&testElem{_array[i]})
		if err != nil {
			t.Errorf("while inserting to bptree(%d): %v", i, err)
			t.FailNow()
		}
	}

	// PrintTree(_tree)
}

func TestSearchElem(t *testing.T) {
	// sort original array
	sort.Ints(_array)

	for i := 0; i < len(_array); i++ {
		k := testKey(_array[i])

		_, ok, err := _tree.SearchElem(k)
		if !ok {
			t.Errorf("not found")
			t.Fail()
		}
		if err != nil {
			t.Errorf("searching err: %v", err)
			t.Fail()
		}
	}
}

func TestRemove(t *testing.T) {
	removeN := 9270

	// selecting remove index
	m := make(map[int]bool)
	lArray := len(_array)

	for i := 0; i < removeN; i++ {
		idx := rand.Intn(lArray)

		m[idx] = true
	}

	mVals := make(map[int]bool)

	for idx, _ := range m {
		val := _array[idx]
		mVals[val] = true
	}

	// removing at original sorted array

	for _, val := range _array {
		if !mVals[val] {
			_array2 = append(_array2, val)
		}
	}

	// removing at tree
	for val, _ := range mVals {
		k := testKey(val)

		err := _tree.Remove(k)
		if err != nil {
			t.Errorf("removing err: %v", err)
			t.Fail()
		}
	}

	// checking by SearchElem
	for i := 0; i < len(_array2); i++ {
		k := testKey(_array2[i])

		_, ok, err := _tree.SearchElem(k)
		if !ok {
			t.Errorf("not found")
			t.Fail()
		}
		if err != nil {
			t.Errorf("searching err: %v", err)
			t.Fail()
		}
	}
}

func TestSearch(t *testing.T) {
	kVal := _array2[len(_array2)/2]

	// find elem at original array
	idx := -1

	for i, val := range _array2 {
		if val == kVal {
			idx = i
			break
		}
	}

	if idx == -1 {
		t.Errorf("not found in original array")
		t.Fail()
	}

	// getting target value in original array
	offset := idx / 2

	// find in tree
	key := testKey(kVal)

	res, ok, err := _tree.Search(key)
	if !ok {
		t.Errorf("element must be found: %v", key)
		t.Fail()
	}

	if err != nil {
		t.Errorf("while searching: %v", err)
		t.Fail()
	}

	// testing SearchElemAt
	err = testSearchElemAt(idx, offset, res)
	if err != nil {
		t.Errorf("while SearchElemAt: %v", err)
		t.Fail()
	}

	// testing SearchElemRange
	err = testSearchElemRange(idx, offset, res)
	if err != nil {
		t.Errorf("while SearchElemRange: %v", err)
		t.Fail()
	}

	// testing SearchElemTo
	err = testSearchElemRangeTo(idx, offset, res)
	if err != nil {
		t.Errorf("while SearchElemTo: %v", err)
		t.Fail()
	}
}

func testSearchElemAt(idx, offset int, res *SearchResult) error {
	before := _array2[idx-offset]
	after := _array2[idx+offset]

	// checking left direction
	elem, ok := res.ElemAt(-offset)
	if !ok {
		return fmt.Errorf("element must be found")
	}

	if elem.Key().CompareTo(testKey(before)) != Equal {
		return fmt.Errorf("element is not matched")
	}

	// checking right direction
	elem, ok = res.ElemAt(offset)
	if !ok {
		return fmt.Errorf("element must be found")
	}

	if elem.Key().CompareTo(testKey(after)) != Equal {
		return fmt.Errorf("element is not matched")
	}

	return nil
}

func testSearchElemRange(idx, offset int, res *SearchResult) error {
	before := _array2[idx-offset : idx+1]
	after := _array2[idx : idx+offset+1]

	// checking left direction
	elems, n := res.ElemRange(-offset)
	if n != len(before) {
		return fmt.Errorf("element length not matched: left")
	}

	for i, v := range before {
		if elems[i].Key().CompareTo(testKey(v)) != Equal {
			return fmt.Errorf("element is not matched: left")
		}
	}

	// checking right direction
	elems, n = res.ElemRange(offset)
	if n != len(after) {
		return fmt.Errorf("element length not matched: right")
	}

	for i, v := range after {
		if elems[i].Key().CompareTo(testKey(v)) != Equal {
			return fmt.Errorf("element is not matched: right")
		}
	}

	return nil
}

func testSearchElemRangeTo(idx, offset int, res *SearchResult) error {
	var i int

	// checking left direction
	for i = idx - offset; i >= 1; i-- {
		prev := _array2[i-1]
		curr := _array2[i]

		if curr-prev > 1 {
			break
		}
	}

	before := _array2[i : idx+1]
	lKey := testKey(_array2[i] - 1)

	elems, n := res.ElemRangeTo(lKey, ToLeft, len(before)+1)
	if n != len(before) {
		return fmt.Errorf("element length not matched: left")
	}

	for j, v := range before {
		if elems[j].Key().CompareTo(testKey(v)) != Equal {
			return fmt.Errorf("elemnt is not matched: left")
		}
	}

	// checking right direction
	for i = idx + offset; i < len(_array2)-1; i++ {
		curr := _array2[i]
		next := _array2[i+1]

		if next-curr > 1 {
			break
		}
	}

	after := _array2[idx : i+1]
	rKey := testKey(_array2[i] + 1)

	elems, n = res.ElemRangeTo(rKey, ToRight, len(after)+1)
	if n != len(after) {
		return fmt.Errorf("element length not matched: right")
	}

	for j, v := range after {
		if elems[j].Key().CompareTo(testKey(v)) != Equal {
			return fmt.Errorf("element is not matched: right")
		}
	}

	return nil
}

func TestSearchElemNearby(t *testing.T) {
	idx := len(_array2) / 2

	var i int

	for i = idx; i < len(_array2)-1; i++ {
		curr := _array2[i]
		next := _array2[i+1]

		if next-curr > 1 {
			break
		}
	}

	// if exact key
	exactKey := testKey(_array2[i])

	elem, equal, err := _tree.SearchElemNearby(exactKey, ToLeft)
	if err != nil {
		t.Errorf("while SearchElemNearby when exact key: %v", err)
		t.Fail()
	}

	if !equal || elem.Key().CompareTo(exactKey) != Equal {
		t.Errorf("element must be same")
		t.Fail()
	}

	// left, right nearby
	key := testKey(_array2[i] + 1)

	// left nearby
	lKey := exactKey
	elem, equal, err = _tree.SearchElemNearby(key, ToLeft)
	if err != nil {
		t.Errorf("while SearchElemNearby when to left: %v", err)
		t.Fail()
	}

	if equal {
		t.Errorf("element must not same")
		t.Fail()
	}

	if elem.Key().CompareTo(lKey) != Equal {
		t.Errorf("SearchElemNearby to left result is invalid")
		t.Fail()
	}

	// right nearby
	rKey := testKey(_array2[i+1])
	elem, equal, err = _tree.SearchElemNearby(key, ToRight)
	if err != nil {
		t.Errorf("while SearchElemNearby when to right: %v", err)
		t.Fail()
	}

	if equal {
		t.Errorf("element must not same")
		t.Fail()
	}

	if elem.Key().CompareTo(rKey) != Equal {
		t.Errorf("SearchElemNearby to right result is invalid")
		t.Fail()
	}
}

func TestRecognizableBptree(t *testing.T) {
	rbptree, err := NewRecognizableBptree(_maxDegree, _maxDepth, _allowOverlap)
	if err != nil {
		t.Errorf("while creating recognizable bptree: %v", err)
		t.FailNow()
	}

	notify := rbptree.AddWatch()

	rbptree.Insert(&testElem{0})

	timeout := time.After(time.Second)
	select {
	case <-notify:
		return
	case <-timeout:
		t.Errorf("timeouted")
		t.FailNow()
	}

	panic(`never reached`)
}

func TestElemRangeToIfThereIsOneElementInTree(t *testing.T) {
	_tree2, err := NewBptree(_maxDegree, _maxDepth, true)
	if err != nil {
		t.Errorf("while initializing bptree: %v", err)
		t.FailNow()
	}

	err = _tree2.Insert(&testElem{10})
	if err != nil {
		t.Errorf("while inserting element: %v", err)
		t.FailNow()
	}

	res, _, err := _tree2.SearchNearby(testKey(12), ToLeft)
	if err != nil {
		t.Errorf("while searching nearby: %v", err)
		t.FailNow()
	}

	elems, n := res.ElemRangeTo(testKey(0), ToLeft, math.MaxUint32)
	if n != 1 {
		t.Errorf("elems length must be 1, but %d", n)
		t.FailNow()
	}
	if elems == nil {
		t.Errorf("elems must be existed")
		t.FailNow()
	}
}
