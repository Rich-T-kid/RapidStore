package DS

// Array/Linked List/ Queue
type SequenceStorage interface {
	AddFirst(*Node) // add @ head
	AddLast(*Node)  // add @ tail
	PopHead() *Node // remove head and return it
	PopTail() *Node // remove tail and return it
	Peak() Node     // copie of head
	PeakTail() Node // copie of tail
	Size() uint
}
type Node struct {
	value interface{}
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
		value: value,
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
func (L *LazyList) AddFirst(*Node) {}
func (L *LazyList) AddLast(*Node)  {}
func (L *LazyList) PopHead() *Node { return nil }
func (L *LazyList) PopTail() *Node { return nil }
func (L *LazyList) Peak() Node     { return Node{} }
func (L *LazyList) PeakTail() Node { return Node{} }
func (L *LazyList) Size() uint     { return 0 }

func NewSequenceStorage() SequenceStorage {
	return NewLazyList()
}
