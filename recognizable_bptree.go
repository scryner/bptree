package bptree

import (
	"github.com/scryner/lfreequeue"
	"sync"
	"time"
)

// Instantly recognizable when tree changed
type RecognizableBptree struct {
	*Bptree

	lastModified     int64
	lastModifiedLock *sync.RWMutex

	notifyQueue *lfreequeue.Queue
}

func NewRecognizableBptree(maxDegree, maxDepth int, allowOverlap bool) (*RecognizableBptree, error) {
	bptree, err := NewBptree(maxDegree, maxDepth, allowOverlap)
	if err != nil {
		return nil, err
	}

	return &RecognizableBptree{
		Bptree:           bptree,
		lastModified:     -1,
		lastModifiedLock: new(sync.RWMutex),
		notifyQueue:      lfreequeue.NewQueue(),
	}, nil
}

func (tree *RecognizableBptree) GetLastModified() int64 {
	tree.lastModifiedLock.RLock()
	defer tree.lastModifiedLock.RUnlock()

	return tree.lastModified
}

func (tree *RecognizableBptree) AddWatch() <-chan int {
	ch := make(chan int)

	tree.notifyQueue.Enqueue(ch)
	return ch
}

func (tree *RecognizableBptree) notify() {
	for v := range tree.notifyQueue.Iter() {
		ch := v.(chan int)

		go func() {
			ch <- 1
		}()
	}
}

func (tree *RecognizableBptree) Insert(elem Elem) error {
	tree.lastModifiedLock.Lock()
	defer tree.lastModifiedLock.Unlock()

	tree.lastModified = time.Now().UnixNano()

	err := tree.Bptree.Insert(elem)
	if err != nil {
		return err
	}

	tree.notify()

	return nil
}

func (tree *RecognizableBptree) Remove(key Key) error {
	tree.lastModifiedLock.Lock()
	defer tree.lastModifiedLock.Unlock()

	tree.lastModified = time.Now().UnixNano()

	err := tree.Bptree.Remove(key)
	if err != nil {
		return err
	}

	tree.notify()

	return nil
}
