package DS

// Array/Linked List/ Queue
type SequenceStorage interface {
	AddFirst(value any) // add @ head
	AddLast(value any)  // add @ tail
	PopHead() any       // remove head and return it
	PopTail() any       // remove tail and return it
	Peak() any          // copie of head
	PeakTail() any      // copie of tail
	Range(start, end int) []interface{}
	Size() uint
}
type Node struct {
	Value interface{}
	next  *Node
	prev  *Node
}

func NewNode(value any, pointers ...*Node) *Node {
	var next *Node = nil
	var prev *Node = nil
	if len(pointers) > 0 {
		next = pointers[0]
	}
	if len(pointers) > 1 {
		prev = pointers[1]
	}
	return &Node{
		Value: value,
		next:  next,
		prev:  prev,
	}
}

type LazyList struct {
	head *Node
	tail *Node
}

func NewLazyList() *LazyList {

	return &LazyList{
		head: nil,
		tail: nil,
	}
}
func (l *LazyList) AddFirst(value any) {
	nn := NewNode(value)
	if l.head == nil {
		l.head = nn
		l.tail = nn
	} else {
		l.head.prev = nn
		nn.next = l.head
		l.head = nn
	}
}
func (l *LazyList) AddLast(value any) {
	nn := NewNode(value)
	if l.tail == nil {
		l.head = nn
		l.tail = nn
	} else {
		l.tail.next = nn // old tail points forward to new node
		nn.prev = l.tail // new node points back to old tail
		l.tail = nn      // update tail to new node
	}
}
func (l *LazyList) PopHead() any {
	if l.head == nil {
		return nil
	}
	oldhead := l.head

	l.head = oldhead.next
	if l.head != nil {
		l.head.prev = nil
	} else {
		l.tail = nil
	}
	return oldhead

}
func (l *LazyList) PopTail() any {
	if l.tail == nil {
		return nil
	}
	oldTail := l.tail
	l.tail = oldTail.prev

	if l.tail != nil {
		l.tail.next = nil
	} else {
		// List became empty
		l.head = nil
	}

	return oldTail

}
func (l *LazyList) Peak() any {
	return *l.head
}
func (l *LazyList) PeakTail() any {
	return *l.tail
}
func (l *LazyList) Size() uint {
	var count uint = 0
	current := l.head
	for current != nil {
		count++
		current = current.next
	}
	return count
}

func (l *LazyList) Range(start, end int) []interface{} {
	if start < 0 || end < start || l.head == nil {
		return []interface{}{}
	}

	var result []interface{}
	current := l.head
	index := 0

	for current != nil && index <= end {
		if index >= start {
			result = append(result, current) // collect value
		}
		current = current.next
		index++
	}

	return result
}

func NewSequenceStorage() SequenceStorage {
	return NewLazyList()
}
