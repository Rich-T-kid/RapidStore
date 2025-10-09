package test

import (
	memorystore "RapidStore/memoryStore"
	"reflect"
	"testing"
)

func newTestSortedSet() memorystore.SortedSetManager {
	return memorystore.NewSortedSetManager(1024, "LRU")
}

// -------------------------
// ZAdd Tests
// -------------------------

func TestZAddCreatesNewKey(t *testing.T) {
	s := newTestSortedSet()

	err := s.ZAdd("leaderboard", 100, "Alice")
	if err != nil {
		t.Errorf("unexpected error on first ZAdd: %v", err)
	}

	// Verify Alice was added by trying to remove her (indirect check)
	if err := s.ZRemove("leaderboard", "Alice"); err != nil {
		t.Errorf("expected Alice to exist, but got remove error: %v", err)
	}
}

func TestZAddMultipleAndUpdate(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("leaderboard", 100, "Alice")
	_ = s.ZAdd("leaderboard", 200, "Bob")
	_ = s.ZAdd("leaderboard", 150, "Charlie")

	// update Alice's score
	_ = s.ZAdd("leaderboard", 300, "Alice")

	// Verify update worked: removing Alice should succeed
	if err := s.ZRemove("leaderboard", "Alice"); err != nil {
		t.Errorf("expected Alice to still exist after update, got error: %v", err)
	}

	// Verify Bob still exists
	if err := s.ZRemove("leaderboard", "Bob"); err != nil {
		t.Errorf("expected Bob to exist, got error: %v", err)
	}
}

// -------------------------
// ZRemove Tests
// -------------------------

func TestZRemoveExistingMember(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("leaderboard", 100, "Alice")
	_ = s.ZAdd("leaderboard", 200, "Bob")

	// remove Alice
	err := s.ZRemove("leaderboard", "Alice")
	if err != nil {
		t.Errorf("unexpected error removing Alice: %v", err)
	}

	// try to remove Alice again → should fail
	if err := s.ZRemove("leaderboard", "Alice"); err == nil {
		t.Errorf("expected error removing already-removed member Alice")
	}
}

func TestZRemoveNonExistentMemberOrKey(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("leaderboard", 100, "Alice")

	// remove non-existent member
	if err := s.ZRemove("leaderboard", "Bob"); err == nil {
		t.Errorf("expected error removing non-existent member Bob")
	}

	// remove from non-existent key
	if err := s.ZRemove("nosuchkey", "Alice"); err == nil {
		t.Errorf("expected error removing from non-existent key")
	}
}

// -------------------------
// Edge Case Tests
// -------------------------

func TestZAddDuplicateScores(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("leaderboard", 100, "Alice")
	_ = s.ZAdd("leaderboard", 100, "Bob") // different member, same score

	// both should exist and be removable
	if err := s.ZRemove("leaderboard", "Alice"); err != nil {
		t.Errorf("expected Alice to exist, got error: %v", err)
	}
	if err := s.ZRemove("leaderboard", "Bob"); err != nil {
		t.Errorf("expected Bob to exist, got error: %v", err)
	}
}

func TestZAddExtremeScores(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("leaderboard", -1e9, "Neg")
	_ = s.ZAdd("leaderboard", 1e9, "Pos")

	// Both should exist and be removable
	if err := s.ZRemove("leaderboard", "Neg"); err != nil {
		t.Errorf("expected Neg to exist with extreme score, got error: %v", err)
	}
	if err := s.ZRemove("leaderboard", "Pos"); err != nil {
		t.Errorf("expected Pos to exist with extreme score, got error: %v", err)
	}
}

func TestZRankBasic(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("lb", 100, "Alice")
	_ = s.ZAdd("lb", 200, "Bob")
	_ = s.ZAdd("lb", 150, "Charlie")

	rank, found := s.ZRank("lb", "Alice")
	if !found || rank != 0 {
		t.Errorf("expected Alice rank=0, got %d (found=%v)", rank, found)
	}

	rank, found = s.ZRank("lb", "Bob")
	if !found || rank != 2 {
		t.Errorf("expected Bob rank=2, got %d (found=%v)", rank, found)
	}
}

func TestZRankNotFound(t *testing.T) {
	s := newTestSortedSet()
	_ = s.ZAdd("lb", 100, "Alice")

	if rank, found := s.ZRank("lb", "Ghost"); found {
		t.Errorf("expected Ghost not found, got rank=%d", rank)
	}
}

func TestZRevRankBasic(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("lb", 100, "Alice")
	_ = s.ZAdd("lb", 200, "Bob")
	_ = s.ZAdd("lb", 150, "Charlie")

	rev, found := s.ZRevRank("lb", "Bob")
	if !found || rev != 0 {
		t.Errorf("expected Bob revRank=0, got %d (found=%v)", rev, found)
	}

	rev, found = s.ZRevRank("lb", "Alice")
	if !found || rev != 2 {
		t.Errorf("expected Alice revRank=2, got %d (found=%v)", rev, found)
	}
}

func TestZRevRankNotFound(t *testing.T) {
	s := newTestSortedSet()
	_ = s.ZAdd("lb", 100, "Alice")

	if rev, found := s.ZRevRank("lb", "Ghost"); found {
		t.Errorf("expected Ghost not found, got revRank=%d", rev)
	}
}

// -------------------------
// ZRange Tests
// -------------------------

func TestZRangeBasicAscending(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("leaderboard", 100, "Alice")
	_ = s.ZAdd("leaderboard", 200, "Bob")
	_ = s.ZAdd("leaderboard", 150, "Charlie")

	// expect ascending order by score
	res := s.ZRange("leaderboard", 0, -1, false)
	expected := []any{"Alice", "Charlie", "Bob"}

	if !reflect.DeepEqual(res, expected) {
		t.Errorf("expected %v, got %v", expected, res)
	}
}

func TestZRangeNegativeIndices(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("leaderboard", 100, "Alice")
	_ = s.ZAdd("leaderboard", 200, "Bob")
	_ = s.ZAdd("leaderboard", 150, "Charlie")

	// last two members
	res := s.ZRange("leaderboard", -2, -1, false)
	expected := []any{"Charlie", "Bob"}

	if !reflect.DeepEqual(res, expected) {
		t.Errorf("expected %v, got %v", expected, res)
	}
}

func TestZRangeWithScores(t *testing.T) {

	s := newTestSortedSet()
	_ = s.ZAdd("leaderboard", 100, "Alice")
	_ = s.ZAdd("leaderboard", 200, "Bob")

	// request withScores=true
	res := s.ZRange("leaderboard", 0, -1, true)
	expected := []any{
		[]any{100.0, "Alice"},
		[]any{200.0, "Bob"},
	}

	if !reflect.DeepEqual(res, expected) {
		t.Errorf("expected %v, got %v", expected, res)
	}
}

func TestZRangeEdgeCases(t *testing.T) {
	s := newTestSortedSet()

	// Empty set
	res := s.ZRange("leaderboard", 0, -1, false)
	if len(res) != 0 {
		t.Errorf("expected empty slice, got %v", res)
	}

	// Add one member
	_ = s.ZAdd("leaderboard", 50, "Solo")

	// Out-of-bounds indices
	res = s.ZRange("leaderboard", 5, 10, false)
	if len(res) != 0 {
		t.Errorf("expected empty slice for out-of-bounds, got %v", res)
	}
}

// ------------------------- Large Intergration test of the entire interface -------------------------
// 1. ZAdd, ZRange, and ZScore Integration
// Ensures adding members assigns correct scores and ordering is respected.
func TestZAddRangeAndScoreIntegration(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("leaders", 100, "alice")
	_ = s.ZAdd("leaders", 200, "bob")
	_ = s.ZAdd("leaders", 150, "charlie")

	members := s.ZRange("leaders", 0, -1, false)
	if len(members) != 3 || members[0] != "alice" || members[2] != "bob" {
		t.Errorf("expected [alice charlie bob], got %v", members)
	}

	score, ok := s.ZScore("leaders", "charlie")
	if !ok || score != 150 {
		t.Errorf("expected charlie score=150, got %v (ok=%v)", score, ok)
	}
}

// 2. ZRank and ZRevRank Integration
// Ensures rank lookups are correct in ascending and descending order.
func TestZRankAndRevRankIntegration(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("leaders", 50, "x")
	_ = s.ZAdd("leaders", 100, "y")
	_ = s.ZAdd("leaders", 200, "z")

	rank, ok := s.ZRank("leaders", "y")
	if !ok || rank != 1 {
		t.Errorf("expected y rank=1 ascending, got %d ok=%v", rank, ok)
	}

	revRank, ok := s.ZRevRank("leaders", "y")
	if !ok || revRank != 1 {
		t.Errorf("expected y revRank=1 descending, got %d ok=%v", revRank, ok)
	}
}

// 3. Overwrite Score Integration
// Ensures adding same member updates score and reorders properly.
func TestZAddOverwriteIntegration(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("leaders", 10, "alice")
	_ = s.ZAdd("leaders", 20, "bob")

	// Overwrite alice score
	_ = s.ZAdd("leaders", 30, "alice")

	rank, _ := s.ZRank("leaders", "alice")
	if rank != 1 {
		t.Errorf("expected alice rank=1 after overwrite, got %d", rank)
	}

	score, ok := s.ZScore("leaders", "alice")
	if !ok || score != 30 {
		t.Errorf("expected alice score=30 after overwrite, got %v", score)
	}
}

// 4. ZRemove Integration
// Ensures removing a member deletes it and rank lookups fail.
func TestZRemoveIntegration(t *testing.T) {
	s := newTestSortedSet()

	_ = s.ZAdd("leaders", 5, "alice")
	_ = s.ZAdd("leaders", 10, "bob")

	err := s.ZRemove("leaders", "alice")
	if err != nil {
		t.Errorf("unexpected error removing alice: %v", err)
	}

	_, ok := s.ZScore("leaders", "alice")
	if ok {
		t.Errorf("expected alice to be removed")
	}

	rank, ok := s.ZRank("leaders", "bob")
	if !ok || rank != 0 {
		t.Errorf("expected bob rank=0, got %d ok=%v", rank, ok)
	}
}

// 5. Edge Case: Empty / Missing Key Integration
// Ensures operations on missing keys behave safely.
func TestSortedSetMissingKeyIntegration(t *testing.T) {
	s := newTestSortedSet()

	// Range on missing key
	members := s.ZRange("nope", 0, -1, false)
	if len(members) != 0 {
		t.Errorf("expected empty slice for missing key, got %v", members)
	}

	// Rank on missing member
	_, ok := s.ZRank("nope", "ghost")
	if ok {
		t.Errorf("expected false rank lookup on missing member")
	}

	// Remove on missing key should not panic
	err := s.ZRemove("nope", "ghost")
	if err == nil {
		t.Errorf("expected error removing from missing set")
	}
}
