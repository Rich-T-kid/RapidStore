package DS

import (
	"testing"

	"golang.org/x/exp/constraints"
)

// helper to extract keys from []*BTNode[T]
func keys[T constraints.Ordered](nodes []*BTNode[T]) []T {
	out := make([]T, len(nodes))
	for i, n := range nodes {
		out[i] = n.Key
	}
	return out
}
func equal[T constraints.Ordered](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func TestInsert_EmptyTree(t *testing.T) {
	tree := &BTree[int]{}

	tree.Insert(10, "ten")

	if tree.Root == nil {
		t.Fatal("Root should not be nil after insert")
	}
	if tree.Root.Key != 10 {
		t.Errorf("Expected root key to be 10, got %d", tree.Root.Key)
	}
	if tree.Root.Value != "ten" {
		t.Errorf("Expected root value to be 'ten', got %v", tree.Root.Value)
	}
	if tree.Size() != 1 {
		t.Errorf("Expected size to be 1, got %d", tree.Size())
	}
}

func TestInsert_MultipleNodes(t *testing.T) {
	tree := &BTree[int]{}

	// Insert in order: 50, 25, 75, 10, 30, 60, 90
	tree.Insert(50, "fifty")
	tree.Insert(25, "twenty-five")
	tree.Insert(75, "seventy-five")
	tree.Insert(10, "ten")
	tree.Insert(30, "thirty")
	tree.Insert(60, "sixty")
	tree.Insert(90, "ninety")

	// Verify tree structure
	if tree.Size() != 7 {
		t.Fatalf("Expected size to be 7, got %d", tree.Size())
	}

	// Check root
	if tree.Root.Key != 50 {
		t.Errorf("Expected root key to be 50, got %d", tree.Root.Key)
	}

	// Check left subtree
	if tree.Root.Left.Key != 25 {
		t.Errorf("Expected left child of root to be 25, got %d", tree.Root.Left.Key)
	}
	if tree.Root.Left.Left.Key != 10 {
		t.Errorf("Expected left-left child to be 10, got %d", tree.Root.Left.Left.Key)
	}
	if tree.Root.Left.Right.Key != 30 {
		t.Errorf("Expected left-right child to be 30, got %d", tree.Root.Left.Right.Key)
	}

	// Check right subtree
	if tree.Root.Right.Key != 75 {
		t.Errorf("Expected right child of root to be 75, got %d", tree.Root.Right.Key)
	}
	if tree.Root.Right.Left.Key != 60 {
		t.Errorf("Expected right-left child to be 60, got %d", tree.Root.Right.Left.Key)
	}
	if tree.Root.Right.Right.Key != 90 {
		t.Errorf("Expected right-right child to be 90, got %d", tree.Root.Right.Right.Key)
	}
}

func TestInsert_DuplicateKeys(t *testing.T) {
	tree := &BTree[int]{}

	tree.Insert(20, "first")
	tree.Insert(10, "ten")
	tree.Insert(30, "thirty")

	initialSize := tree.Size()
	if initialSize != 3 {
		t.Fatalf("Expected initial size to be 3, got %d", initialSize)
	}

	// Try inserting duplicate keys
	tree.Insert(20, "second")      // duplicate root
	tree.Insert(10, "updated ten") // duplicate left child

	// Size should remain the same (duplicates ignored)
	if tree.Size() != initialSize {
		t.Errorf("Expected size to remain %d after duplicate inserts, got %d", initialSize, tree.Size())
	}

	// Original values should remain (since duplicates are ignored)
	if tree.Root.Value != "first" {
		t.Errorf("Expected root value to remain 'first', got %v", tree.Root.Value)
	}
	if tree.Root.Left.Value != "ten" {
		t.Errorf("Expected left child value to remain 'ten', got %v", tree.Root.Left.Value)
	}
}
func TestInorderBasic(t *testing.T) {
	tree := &BTree[int]{}

	tree.Insert(20, "first")
	tree.Insert(10, "ten")
	tree.Insert(30, "thirty")

	initialSize := tree.Size()
	if initialSize != 3 {
		t.Fatalf("Expected initial size to be 3, got %d", initialSize)
	}
	var expected = map[int]int{
		0: 10,
		1: 20,
		2: 30,
	}
	iter := tree.InOrder()
	for i := range iter {
		if expected[i] != iter[i].Key {
			t.Fatalf("expected %d but got %d at position %d", expected[i], iter[i].Key, i)
		}

	}
}

// ---------------- InOrder Tests ----------------

func TestInOrderSimple(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(2, "b")
	tree.Insert(1, "a")
	tree.Insert(3, "c")

	got := keys(tree.InOrder())
	want := []int{1, 2, 3}

	if !equal(got, want) {
		t.Errorf("InOrder = %v, want %v", got, want)
	}
}

func TestInOrderLeftSkewed(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(3, "c")
	tree.Insert(2, "b")
	tree.Insert(1, "a")

	got := keys(tree.InOrder())
	want := []int{1, 2, 3}

	if !equal(got, want) {
		t.Errorf("InOrder (left skewed) = %v, want %v", got, want)
	}
}

func TestInOrderRightSkewed(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(1, "a")
	tree.Insert(2, "b")
	tree.Insert(3, "c")

	got := keys(tree.InOrder())
	want := []int{1, 2, 3}

	if !equal(got, want) {
		t.Errorf("InOrder (right skewed) = %v, want %v", got, want)
	}
}

// ---------------- PreOrder Tests ----------------

func TestPreOrderSimple(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(2, "b")
	tree.Insert(1, "a")
	tree.Insert(3, "c")

	got := keys(tree.PreOrder())
	want := []int{2, 1, 3}

	if !equal(got, want) {
		t.Errorf("PreOrder = %v, want %v", got, want)
	}
}

func TestPreOrderLeftSkewed(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(3, "c")
	tree.Insert(2, "b")
	tree.Insert(1, "a")

	got := keys(tree.PreOrder())
	want := []int{3, 2, 1}

	if !equal(got, want) {
		t.Errorf("PreOrder (left skewed) = %v, want %v", got, want)
	}
}

func TestPreOrderRightSkewed(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(1, "a")
	tree.Insert(2, "b")
	tree.Insert(3, "c")

	got := keys(tree.PreOrder())
	want := []int{1, 2, 3}

	if !equal(got, want) {
		t.Errorf("PreOrder (right skewed) = %v, want %v", got, want)
	}
}

// ---------------- PostOrder Tests ----------------

func TestPostOrderSimple(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(2, "b")
	tree.Insert(1, "a")
	tree.Insert(3, "c")

	got := keys(tree.PostOrder())
	want := []int{1, 3, 2}

	if !equal(got, want) {
		t.Errorf("PostOrder = %v, want %v", got, want)
	}
}

func TestPostOrderLeftSkewed(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(3, "c")
	tree.Insert(2, "b")
	tree.Insert(1, "a")

	got := keys(tree.PostOrder())
	want := []int{1, 2, 3}

	if !equal(got, want) {
		t.Errorf("PostOrder (left skewed) = %v, want %v", got, want)
	}
}

func TestPostOrderRightSkewed(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(1, "a")
	tree.Insert(2, "b")
	tree.Insert(3, "c")

	got := keys(tree.PostOrder())
	want := []int{3, 2, 1}

	if !equal(got, want) {
		t.Errorf("PostOrder (right skewed) = %v, want %v", got, want)
	}
}

// ---------------- LevelOrder Tests ----------------
func levelKeys[T constraints.Ordered](levels [][]*BTNode[T]) [][]T {
	out := make([][]T, len(levels))
	for i, lvl := range levels {
		for _, n := range lvl {
			out[i] = append(out[i], n.Key)
		}
	}
	return out
}
func equal2D[T comparable](a, b [][]T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

func TestLevelOrderBalancedLevels(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(2, "b")
	tree.Insert(1, "a")
	tree.Insert(3, "c")

	got := levelKeys(tree.LevelOrder())

	want := [][]int{
		{2},    // root
		{1, 3}, // children
	}

	if !equal2D(got, want) {
		t.Errorf("LevelOrder (balanced) = %v, want %v", got, want)
	}
}

func TestLevelOrderUnbalancedLevels(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(10, "root")
	tree.Insert(5, "left")
	tree.Insert(15, "right")
	tree.Insert(3, "left.left")
	tree.Insert(7, "left.right")

	got := levelKeys(tree.LevelOrder())
	want := [][]int{
		{10},
		{5, 15},
		{3, 7},
	}

	if !equal2D(got, want) {
		t.Errorf("LevelOrder (unbalanced) = %v, want %v", got, want)
	}
}

func TestInOrderComplex(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(50, "a")
	tree.Insert(30, "b")
	tree.Insert(70, "c")
	tree.Insert(20, "d")
	tree.Insert(40, "e")
	tree.Insert(60, "f")
	tree.Insert(80, "g")
	tree.Insert(35, "h")
	tree.Insert(45, "i")

	got := keys(tree.InOrder())
	want := []int{20, 30, 35, 40, 45, 50, 60, 70, 80}

	if !equal(got, want) {
		t.Errorf("InOrder (complex) = %v, want %v", got, want)
	}
}

// ---------------- Min max Tests ----------------
func TestMinSimple(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(10, "root")
	tree.Insert(5, "left")
	tree.Insert(20, "right")

	minNode, ok := tree.Min()
	if !ok {
		t.Fatalf("expected Min() to succeed, got ok=false")
	}
	if minNode.Key != 5 {
		t.Errorf("expected Min() key=5, got %v", minNode.Key)
	}
	tree.Insert(1, "far-left")
	minNode, ok = tree.Min()
	if !ok {
		t.Fatalf("expected Min() to succeed, got ok=false")
	}
	if minNode.Key != 1 {
		t.Errorf("expected Min() key=1, got %v", minNode.Key)
	}
}

func TestMinEmptyTree(t *testing.T) {
	tree := NewBTree[int]()
	minNode, ok := tree.Min()
	if ok || minNode != nil {
		t.Errorf("expected Min() to fail on empty tree, got ok=%v node=%v", ok, minNode)
	}
}

func TestMaxSimple(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(10, "root")
	tree.Insert(5, "left")
	tree.Insert(20, "right")
	tree.Insert(25, "far-right")

	maxNode, ok := tree.Max()
	if !ok {
		t.Fatalf("expected Max() to succeed, got ok=false")
	}
	if maxNode.Key != 25 {
		t.Errorf("expected Max() key=25, got %v", maxNode.Key)
	}
	tree.Insert(110, "mid-right")
	maxNode, ok = tree.Max()
	if !ok {
		t.Fatalf("expected Max() to succeed, got ok=false")
	}
	if maxNode.Key != 110 {
		t.Errorf("expected Max() key=110, got %v", maxNode.Key)
	}
}

func TestMaxEmptyTree(t *testing.T) {
	tree := NewBTree[int]()
	maxNode, ok := tree.Max()
	if ok || maxNode != nil {
		t.Errorf("expected Max() to fail on empty tree, got ok=%v node=%v", ok, maxNode)
	}
}

// ------------------ BInary search ------------------

func TestSearchEmptyTree(t *testing.T) {
	tree := NewBTree[int]()
	node, ok := tree.Search(10)
	if ok || node != nil {
		t.Errorf("expected search on empty tree to return (nil,false), got (%v,%v)", node, ok)
	}
}

func TestSearchRoot(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(10, "root")

	node, ok := tree.Search(10)
	if !ok || node == nil || node.Key != 10 {
		t.Errorf("expected to find root node with key=10, got (%v,%v)", node, ok)
	}
}

func TestSearchLeftChild(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(10, "root")
	tree.Insert(5, "left")

	node, ok := tree.Search(5)
	// if BST rules are correct, should return true and key=5
	if !ok || node == nil || node.Key != 5 {
		t.Errorf("expected to find left child key=5, got (%v,%v)", node, ok)
	}
}

func TestSearchRightChild(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(10, "root")
	tree.Insert(15, "right")

	node, ok := tree.Search(15)
	// if BST rules are correct, should return true and key=15
	if !ok || node == nil || node.Key != 15 {
		t.Errorf("expected to find right child key=15, got (%v,%v)", node, ok)
	}
}

func TestSearchNonExistent(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(10, "root")
	tree.Insert(5, "left")
	tree.Insert(15, "right")

	node, ok := tree.Search(42)
	if ok || node != nil {
		t.Errorf("expected not to find 42, got (%v,%v)", node, ok)
	}
}

// ----------------- Range Search test -----------------

func nodeKeys(nodes []*BTNode[int]) []int {
	keys := make([]int, len(nodes))
	for i, n := range nodes {
		keys[i] = n.Key
	}
	return keys
}

// Case 1: Empty tree → always empty result
func TestRangeSearchEmptyTree(t *testing.T) {
	tree := NewBTree[int]()
	result := nodeKeys(tree.RangeSearch(1, 10))
	if len(result) != 0 {
		t.Errorf("expected empty result, got %v", result)
	}
}

// Case 2: Full-range query should return all nodes in sorted order
func TestRangeSearchFullRange(t *testing.T) {
	tree := NewBTree[int]()
	for _, k := range []int{50, 30, 70, 20, 40, 60, 80} {
		tree.Insert(k, nil)
	}
	result := nodeKeys(tree.RangeSearch(0, 100))
	expected := []int{20, 30, 40, 50, 60, 70, 80}
	if !equal(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// Case 3: Mid-range query should return subset
func TestRangeSearchSubset(t *testing.T) {
	tree := NewBTree[int]()
	for _, k := range []int{50, 30, 70, 20, 40, 60, 80} {
		tree.Insert(k, nil)
	}
	result := nodeKeys(tree.RangeSearch(35, 65))
	expected := []int{40, 50, 60}
	if !equal(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// Case 4: Single-value range that matches
func TestRangeSearchSingleMatch(t *testing.T) {
	tree := NewBTree[int]()
	for _, k := range []int{10, 5, 15} {
		tree.Insert(k, nil)
	}
	result := nodeKeys(tree.RangeSearch(5, 5))
	expected := []int{5}
	if !equal(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// Case 5: Range that includes no keys
func TestRangeSearchNoResults(t *testing.T) {
	tree := NewBTree[int]()
	for _, k := range []int{10, 20, 30} {
		tree.Insert(k, nil)
	}
	result := nodeKeys(tree.RangeSearch(100, 200))
	if len(result) != 0 {
		t.Errorf("expected empty result, got %v", result)
	}
}

// Case 6: Range partially overlaps (some values in range, some out)
func TestRangeSearchPartialOverlap(t *testing.T) {
	tree := NewBTree[int]()
	for _, k := range []int{10, 20, 30, 40, 50} {
		tree.Insert(k, nil)
	}
	result := nodeKeys(tree.RangeSearch(25, 45))
	expected := []int{30, 40}
	if !equal(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

// --- Edge cases ---

func TestRangeSearchEdgeCases(t *testing.T) {
	tree := NewBTree[int]()

	// Case 1: Empty tree
	if got := tree.RangeSearch(1, 10); len(got) != 0 {
		t.Errorf("expected empty result for empty tree, got %v", got)
	}

	// Prepare a tree with [10, 20, 30, 40]
	for _, k := range []int{10, 20, 30, 40} {
		tree.Insert(k, nil)
	}

	// Case 2: minVal > maxVal should return empty
	if got := tree.RangeSearch(50, 10); len(got) != 0 {
		t.Errorf("expected empty for reversed bounds, got %v", nodeKeys(got))
	}

	// Case 3: Single-value range where the key exists
	if got := nodeKeys(tree.RangeSearch(20, 20)); !equal(got, []int{20}) {
		t.Errorf("expected [20], got %v", got)
	}

	// Case 4: Single-value range where the key does not exist
	if got := tree.RangeSearch(25, 25); len(got) != 0 {
		t.Errorf("expected empty for non-existent single value, got %v", nodeKeys(got))
	}
}

func TestRangeSearchBoundaryCases(t *testing.T) {
	tree := NewBTree[int]()
	for _, k := range []int{10, 20, 30, 40} {
		tree.Insert(k, nil)
	}

	// Case 5: Range matches the smallest and largest keys
	if got := nodeKeys(tree.RangeSearch(10, 40)); !equal(got, []int{10, 20, 30, 40}) {
		t.Errorf("expected full tree [10,20,30,40], got %v", got)
	}

	// Case 6: Range smaller than all keys
	if got := tree.RangeSearch(-100, -1); len(got) != 0 {
		t.Errorf("expected empty, got %v", nodeKeys(got))
	}

	// Case 7: Range larger than all keys
	if got := tree.RangeSearch(100, 200); len(got) != 0 {
		t.Errorf("expected empty, got %v", nodeKeys(got))
	}

	// Case 8: Range overlaps only one boundary (just highest)
	if got := nodeKeys(tree.RangeSearch(35, 45)); !equal(got, []int{40}) {
		t.Errorf("expected [40], got %v", got)
	}
}

// ------------------- Delete Operations -------------------

func TestDeleteEmptyTree(t *testing.T) {
	tree := NewBTree[int]()
	ok := tree.Delete(10)
	if ok {
		t.Errorf("expected Delete on empty tree to return false, got true")
	}
}
func TestDeleteNonExistent(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(10, nil)
	tree.Insert(5, nil)
	tree.Insert(15, nil)

	ok := tree.Delete(99)
	if ok {
		t.Errorf("expected Delete(99) = false, got true")
	}
}

func TestDeleteLeafNode(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(10, nil)
	tree.Insert(5, nil)
	tree.Insert(15, nil)

	ok := tree.Delete(5) // leaf
	if !ok {
		t.Errorf("expected Delete(5) = true, got false")
	}

	// Verify 5 is gone
	if _, found := tree.Search(5); found {
		t.Errorf("expected 5 to be deleted, but found")
	}
}

func TestDeleteNodeWithOneChild(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(10, nil)
	tree.Insert(5, nil)
	tree.Insert(2, nil) // 5 has one child (2)

	ok := tree.Delete(5)
	if !ok {
		t.Errorf("expected Delete(5) = true, got false")
	}

	// Verify 5 is gone, 2 still exists
	if _, found := tree.Search(5); found {
		t.Errorf("expected 5 to be deleted, but found")
	}
	if _, found := tree.Search(2); !found {
		t.Errorf("expected 2 to remain, but not found")
	}
}

func TestDeleteNodeWithTwoChildren(t *testing.T) {
	tree := NewBTree[int]()
	tree.Insert(10, nil)
	tree.Insert(5, nil)
	tree.Insert(15, nil)
	tree.Insert(12, nil)
	tree.Insert(18, nil)

	ok := tree.Delete(15) // 15 has children 12 and 18
	if !ok {
		t.Errorf("expected Delete(15) = true, got false")
	}

	// Verify 15 is gone
	if _, found := tree.Search(15); found {
		t.Errorf("expected 15 to be deleted, but found")
	}
	levelOrder := levelKeys(tree.LevelOrder())
	expectedLevels := [][]int{
		{10},
		{5, 18}, // 18 should replace 15
		{12},
	}
	if !equal2D(levelOrder, expectedLevels) {
		t.Errorf("expected levels %v, got %v", expectedLevels, levelOrder)
	}
	// Verify subtree is intact
	if _, found := tree.Search(12); !found {
		t.Errorf("expected 12 to remain, but not found")
	}
	if _, found := tree.Search(18); !found {
		t.Errorf("expected 18 to remain, but not found")
	}
}

func TestDeleteRootEdgeCases(t *testing.T) {
	// Case 1: Root is the only node
	tree := NewBTree[int]()
	tree.Insert(10, "root")

	ok := tree.Delete(10)
	if !ok {
		t.Errorf("expected Delete(10) = true, got false")
	}
	if tree.Root != nil {
		t.Errorf("expected tree to be empty after deleting root")
	}

	// Case 2: Root has one child
	tree = NewBTree[int]()
	tree.Insert(10, "root")
	tree.Insert(5, "left")

	ok = tree.Delete(10)
	if !ok {
		t.Errorf("expected Delete(10) = true, got false")
	}
	if tree.Root == nil || tree.Root.Key != 5 {
		t.Errorf("expected new root to be 5, got %v", tree.Root)
	}

	// Case 3: Root has two children
	tree = NewBTree[int]()
	tree.Insert(10, "root")
	tree.Insert(5, "left")
	tree.Insert(15, "right")

	ok = tree.Delete(10)
	if !ok {
		t.Errorf("expected Delete(10) = true, got false")
	}
	if tree.Root == nil {
		t.Errorf("expected non-empty tree after deleting root")
	}
	// Root should now be either 15 (successor) or 5 (if you used predecessor)
	if tree.Root.Key != 15 && tree.Root.Key != 5 {
		t.Errorf("expected root to be 15 or 5 after deletion, got %v", tree.Root.Key)
	}
}
