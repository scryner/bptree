package bptree

import (
	"fmt"
	"sort"
)

type Elems []Elem

func (elems Elems) Len() int { return len(elems) }
func (elems Elems) Less(i, j int) bool {
	cond := elems[i].Key().CompareTo(elems[j].Key())
	return cond == Less
}

func (elems Elems) Swap(i, j int) { elems[i], elems[j] = elems[j], elems[i] }

func (elems Elems) find(key Key) (idx int, isEqual bool) {
	idx = sort.Search(len(elems), func(i int) bool {
		cond := elems[i].Key().CompareTo(key)
		if cond == Equal {
			isEqual = true
		}

		return cond == Equal || cond == Greater
	})

	return
}

func (elems Elems) insert(elem Elem, maxDegree int, allowOverlap bool) (Elems, error) {
	idx, equal := elems.find(elem.Key())

	if idx >= len(elems) {
		elems = append(elems, elem)
		return elems, nil
	}

	if equal && !allowOverlap {
		return nil, ERR_OVERLAPPED
	}

	newElems := make(Elems, len(elems)+1, maxDegree+1)

	copy(newElems, elems[:idx])
	newElems[idx] = elem
	copy(newElems[idx+1:], elems[idx:])

	return newElems, nil
}

func (elems Elems) delete(key Key, maxDegree int) (Elems, bool) {
	idx, equal := elems.find(key)

	if equal {
		// found
		newElems := make(Elems, len(elems)-1, maxDegree+1)

		copy(newElems, elems[:idx])
		copy(newElems[idx:], elems[idx+1:])

		return newElems, true
	}

	return elems, false
}

func (elems Elems) String() string {
	var elemsStr []string

	for _, _elem := range elems {
		switch elem := _elem.(type) {
		case *indexNode:
			elemsStr = append(elemsStr, fmt.Sprintf("i<%v>", elem.Key()))
		default:
			elemsStr = append(elemsStr, fmt.Sprintf("e<%v>", elem.Key()))
		}
	}

	return fmt.Sprintf("%v", elemsStr)
}
