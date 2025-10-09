package DS

import (
	"testing"
)

// --------------------
// LazyList (SequenceStorage) Tests
// --------------------

func TestAddFirstAndAddLast(t *testing.T) {
	list := NewSequenceStorage()

	list.AddFirst(2)
	list.AddFirst(1)
	list.AddLast(3)

	if list.Size() != 3 {
		t.Errorf("expected size 3, got %d", list.Size())
	}

	head := list.Peak().(Node)
	tail := list.PeakTail().(Node)

	if head.Value != 1 {
		t.Errorf("expected head = 1, got %v", head.Value)
	}
	if tail.Value != 3 {
		t.Errorf("expected tail = 3, got %v", tail.Value)
	}
}

func TestPopHeadAndPopTail(t *testing.T) {
	list := NewSequenceStorage()

	list.AddLast(1)
	list.AddLast(2)
	list.AddLast(3)

	head := list.PopHead().(*Node)
	if head.Value != 1 {
		t.Errorf("expected pop head = 1, got %v", head.Value)
	}

	tail := list.PopTail().(*Node)
	if tail.Value != 3 {
		t.Errorf("expected pop tail = 3, got %v", tail.Value)
	}

	if list.Size() != 1 {
		t.Errorf("expected size = 1 after pops, got %d", list.Size())
	}
}

func TestPeakFunctions(t *testing.T) {
	list := NewSequenceStorage()

	list.AddLast("a")
	list.AddLast("b")

	if list.Peak().(Node).Value != "a" {
		t.Errorf("expected peak head = a, got %v", list.Peak().(Node).Value)
	}
	if list.PeakTail().(Node).Value != "b" {
		t.Errorf("expected peak tail = b, got %v", list.PeakTail().(Node).Value)
	}
}

func TestRangeFunction(t *testing.T) {
	list := NewSequenceStorage()
	list.AddLast("x")
	list.AddLast("y")
	list.AddLast("z")

	result := list.Range(0, 1)
	if len(result) != 2 {
		t.Errorf("expected 2 elements in range, got %d", len(result))
	}
	if result[0].(*Node).Value != "x" || result[1].(*Node).Value != "y" {
		t.Errorf("expected [x,y], got [%v,%v]", result[0].(*Node).Value, result[1].(*Node).Value)
	}

	// edge case: invalid range
	empty := list.Range(2, 1)
	if len(empty) != 0 {
		t.Errorf("expected empty slice for invalid range, got %v", empty)
	}
}

func TestPopOnEmptyList(t *testing.T) {
	list := NewSequenceStorage()

	if list.PopHead() != nil {
		t.Errorf("expected nil when popping head from empty list")
	}
	if list.PopTail() != nil {
		t.Errorf("expected nil when popping tail from empty list")
	}
}

// --------------------
// Queue Tests
// --------------------

func TestQueueEnqueueDequeue(t *testing.T) {
	var q Queue[int]

	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)

	if q.Size() != 3 {
		t.Errorf("expected queue size 3, got %d", q.Size())
	}

	val, ok := q.Dequeue()
	if !ok || val != 1 {
		t.Errorf("expected dequeue 1, got %v (ok=%v)", val, ok)
	}

	if q.Size() != 2 {
		t.Errorf("expected size 2 after dequeue, got %d", q.Size())
	}
}

func TestQueueIsEmpty(t *testing.T) {
	var q Queue[string]

	if !q.IsEmpty() {
		t.Errorf("expected new queue to be empty")
	}

	q.Enqueue("hello")
	if q.IsEmpty() {
		t.Errorf("expected non-empty queue after enqueue")
	}

	q.Dequeue()
	if !q.IsEmpty() {
		t.Errorf("expected queue to be empty after dequeue all")
	}
}

func TestQueueOrder(t *testing.T) {
	var q Queue[int]

	q.Enqueue(10)
	q.Enqueue(20)
	q.Enqueue(30)

	val1, _ := q.Dequeue()
	val2, _ := q.Dequeue()
	val3, _ := q.Dequeue()

	if val1 != 10 || val2 != 20 || val3 != 30 {
		t.Errorf("expected [10,20,30], got [%d,%d,%d]", val1, val2, val3)
	}
}
