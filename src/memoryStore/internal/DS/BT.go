package DS

import "golang.org/x/exp/constraints"

// import "golang.org/x/exp/constraints"
// Ordered represents types that support ordering operations
type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 |
		~string
}

type BTNode[T constraints.Ordered] struct {
	Left  *BTNode[T]
	Right *BTNode[T]
	Key   T
	Value interface{}
}
type BinaryTree[T constraints.Ordered] interface {
	Insert(key T, value interface{})
	Delete(key T) bool
	Search(key T) (*BTNode[T], bool)
	Rank(key T) (int, bool)
	RevRank(key T) (int, bool)
	InOrder() []*BTNode[T]
	PreOrder() []*BTNode[T]
	PostOrder() []*BTNode[T]
	LevelOrder() [][]*BTNode[T]
	RangeSearch(minVal, maxVal T) []*BTNode[T]
	Size() uint
	Min() (*BTNode[T], bool)
	Max() (*BTNode[T], bool)
}

type BTree[T constraints.Ordered] struct {
	Root *BTNode[T]
	size uint
}

func NewBTree[T constraints.Ordered]() *BTree[T] {
	return &BTree[T]{
		Root: nil,
		size: 0,
	}
}
func (t *BTree[T]) incrementSize() {
	t.size++
}
func (t *BTree[T]) decrementSize() {
	t.size--
}

// keys must be unqiue
func (t *BTree[T]) insertNode(node *BTNode[T], key T, value interface{}) {
	// if the current node is more than the key this needs to go left
	if key < node.Key {
		// if the a spot is open place it there
		if node.Left == nil {
			t.incrementSize()
			node.Left = &BTNode[T]{
				Left:  nil,
				Right: nil,
				Key:   key,
				Value: value,
			}
			return
		}
		// otherwise keep going down the left subtree
		t.insertNode(node.Left, key, value)
	} else if key > node.Key {
		// if the a spot is open place it there
		if node.Right == nil {
			t.incrementSize()
			node.Right = &BTNode[T]{
				Left:  nil,
				Right: nil,
				Key:   key,
				Value: value,
			}
			return
		}
		// otherwise keep going down the right subtree
		t.insertNode(node.Right, key, value)
	}

}
func (t *BTree[T]) Insert(key T, value interface{}) {
	if t.Root == nil {
		t.incrementSize()
		t.Root = &BTNode[T]{
			Key:   key,
			Value: value,
			Left:  nil,
			Right: nil,
		}
		return
		// set root and return
	}
	// otherwise insert normally
	t.insertNode(t.Root, key, value)
}
func (t *BTree[T]) Delete(key T) bool {
	if t.Root == nil {
		return false
	}
	var del bool
	t.Root, del = t.internalDelete(t.Root, key)
	return del
}
func (t *BTree[T]) internalDelete(node *BTNode[T], key T) (*BTNode[T], bool) {
	// if node wasnt found return false
	if node == nil {
		return nil, false
	}
	// gotta find the node first, so perform standard searc through bst
	var delete bool
	if key > node.Key {
		node.Right, delete = t.internalDelete(node.Right, key)

	} else if key < node.Key {
		node.Left, delete = t.internalDelete(node.Left, key)
	} else {
		// this is the node we want to delelete
		delete = true
		t.decrementSize()
		// case 1 no children
		if node.Left == nil && node.Right == nil {
			return nil, delete
		}
		// case 2 one child
		if node.Left == nil {
			return node.Right, delete
		}
		if node.Right == nil {
			return node.Left, delete
		}
		// case 3 two children
		// find the min of the right subtree to replace this node with
		minRight := minNode(node.Right)
		node.Key = minRight.Key
		node.Value = minRight.Value
		// now delete the min from the right subtree since its now here
		node.Right, _ = t.internalDelete(node.Right, minRight.Key)

	}
	return node, delete
}
func (t *BTree[T]) internalSearch(node *BTNode[T], key T) (*BTNode[T], bool) {
	// handle nil nodes , not found return
	if node == nil {
		return nil, false
	}
	if node.Key == key {
		return node, true
	} else if key < node.Key {
		// smaller -> go left
		return t.internalSearch(node.Left, key)
	} else {
		// larger -> go right
		return t.internalSearch(node.Right, key)
	}

}
func (t *BTree[T]) Search(key T) (*BTNode[T], bool) {
	if t.Root == nil {
		return nil, false
	}
	// if this is the value return it
	if key == t.Root.Key {
		return t.Root, true
	} else if key < t.Root.Key { // smaller -> left
		return t.internalSearch(t.Root.Left, key)
	} else { // larger -> right
		return t.internalSearch(t.Root.Right, key)
	}
}
func (t *BTree[T]) InOrder() []*BTNode[T] {
	var result []*BTNode[T]
	var innerRec func(node *BTNode[T])
	innerRec = func(node *BTNode[T]) {
		if node == nil {
			return
		}
		innerRec(node.Left)
		result = append(result, node)
		innerRec(node.Right)
	}
	innerRec(t.Root)
	return result
}
func (t *BTree[T]) PreOrder() []*BTNode[T] {

	var result []*BTNode[T]
	var innerRec func(node *BTNode[T])
	innerRec = func(node *BTNode[T]) {
		if node == nil {
			return
		}
		result = append(result, node)
		innerRec(node.Left)
		innerRec(node.Right)
	}
	innerRec(t.Root)
	return result
}
func (t *BTree[T]) PostOrder() []*BTNode[T] {
	var result []*BTNode[T]
	var innerRec func(node *BTNode[T])
	innerRec = func(node *BTNode[T]) {
		if node == nil {
			return
		}
		innerRec(node.Left)
		innerRec(node.Right)
		result = append(result, node)
	}
	innerRec(t.Root)
	return result
}
func (t *BTree[T]) LevelOrder() [][]*BTNode[T] {
	var result [][]*BTNode[T]
	if t.Root == nil {
		return result
	}
	currentQueue := Queue[*BTNode[T]]{t.Root}
	for !currentQueue.IsEmpty() {
		var curLevel []*BTNode[T]
		cursize := currentQueue.Size()
		for i := 0; i < cursize; i++ {
			node, _ := currentQueue.Dequeue()
			if node.Left != nil {
				currentQueue.Enqueue(node.Left)
			}
			if node.Right != nil {
				currentQueue.Enqueue(node.Right)
			}
			curLevel = append(curLevel, node)
		}
		result = append(result, curLevel)

	}
	return result
}

// range is inclusive on both ends so (minVal <= key <= maxVal) || if minval > maxval return empty
func (t *BTree[T]) RangeSearch(minVal, maxVal T) []*BTNode[T] {
	if t.Root == nil || minVal > maxVal {
		return []*BTNode[T]{}
	}
	if minVal == maxVal {
		if node, found := t.Search(minVal); found {
			return []*BTNode[T]{node}
		}
		return []*BTNode[T]{}
	}
	var result []*BTNode[T]
	var innerRec func(node *BTNode[T])
	innerRec = func(node *BTNode[T]) {
		if node == nil {
			return
		}
		if node.Key > minVal {
			innerRec(node.Left)
		}
		if node.Key >= minVal && node.Key <= maxVal {
			result = append(result, node)
		}
		if node.Key < maxVal {
			innerRec(node.Right)
		}

	}
	innerRec(t.Root)
	return result
}
func (t *BTree[T]) Rank(key T) (int, bool) {
	index := 0
	var found bool

	var inorder func(node *BTNode[T])
	inorder = func(node *BTNode[T]) {
		if node == nil || found {
			return
		}
		inorder(node.Left)

		if !found {
			if node.Key == key {
				found = true
				return
			}
			index++
		}

		inorder(node.Right)
	}

	inorder(t.Root)
	return index, found
}

func (t *BTree[T]) RevRank(key T) (int, bool) {
	index := 0
	var found bool

	var reverseInorder func(node *BTNode[T])
	reverseInorder = func(node *BTNode[T]) {
		if node == nil || found {
			return
		}
		reverseInorder(node.Right)

		if !found {
			if node.Key == key {
				found = true
				return
			}
			index++
		}

		reverseInorder(node.Left)
	}

	reverseInorder(t.Root)
	return index, found
}

func (t *BTree[T]) Size() uint {
	return t.size
}
func (t *BTree[T]) Min() (lowest *BTNode[T], exist bool) {
	if t.Root == nil {
		return nil, false
	}
	var innerFunc func(node *BTNode[T]) *BTNode[T]
	innerFunc = func(node *BTNode[T]) *BTNode[T] {
		if node.Left == nil {
			return node
		}
		return innerFunc(node.Left)
	}
	return innerFunc(t.Root), true
}
func (t *BTree[T]) Max() (highest *BTNode[T], exist bool) {
	if t.Root == nil {
		return nil, false
	}
	var innerFunc func(node *BTNode[T]) *BTNode[T]
	innerFunc = func(node *BTNode[T]) *BTNode[T] {
		if node.Right == nil {
			return node
		}
		return innerFunc(node.Right)
	}
	return innerFunc(t.Root), true
}
func minNode[T constraints.Ordered](node *BTNode[T]) *BTNode[T] {
	current := node
	for current.Left != nil {
		current = current.Left
	}
	return current
}

func NewBinaryTree[T constraints.Ordered]() BinaryTree[T] {
	return NewBTree[T]()
}
