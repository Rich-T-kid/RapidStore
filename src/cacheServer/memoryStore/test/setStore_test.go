package test

import (
	memorystore "RapidStore/memoryStore"
	"testing"
)

func newSetStore() memorystore.SetManager {
	return memorystore.NewSetManager(1024, "LRU")
}

// -------------------- One test per SetManager method --------------------

func TestSAddAndSMembers(t *testing.T) {
	store := newSetStore()
	store.SAdd("colors", "red")
	store.SAdd("colors", "blue")

	members := store.SMembers("colors")
	if len(members) != 2 {
		t.Errorf("expected 2 members, got %d", len(members))
	}
	if !contains(members, "red") || !contains(members, "blue") {
		t.Errorf("expected red and blue in members, got %v", members)
	}
}

func TestSAddDuplicate(t *testing.T) {
	store := newSetStore()
	store.SAdd("nums", 1)
	store.SAdd("nums", 1) // duplicate

	members := store.SMembers("nums")
	if len(members) != 1 {
		t.Errorf("expected 1 unique member, got %d", len(members))
	}
}

func TestSRemExisting(t *testing.T) {
	store := newSetStore()
	store.SAdd("letters", "a")
	ok := store.SRem("letters", "a")

	if !ok {
		t.Errorf("expected removal to succeed")
	}
	if store.SCard("letters") != 0 {
		t.Errorf("expected set empty after removal")
	}
}

func TestSRemNonExisting(t *testing.T) {
	store := newSetStore()
	ok := store.SRem("letters", "x")
	if ok {
		t.Errorf("expected false for non-existing member")
	}
}

func TestSIsMember(t *testing.T) {
	store := newSetStore()
	store.SAdd("fruits", "apple")
	if !store.SIsMember("fruits", "apple") {
		t.Errorf("expected apple to be a member")
	}
	if store.SIsMember("fruits", "banana") {
		t.Errorf("expected banana not to be a member")
	}
}

func TestSCard(t *testing.T) {
	store := newSetStore()
	store.SAdd("nums", 1)
	store.SAdd("nums", 2)
	if store.SCard("nums") != 2 {
		t.Errorf("expected size 2, got %d", store.SCard("nums"))
	}
	if store.SCard("missing") != 0 {
		t.Errorf("expected size 0 for missing key")
	}
}

func TestCurrentSizeSet(t *testing.T) {
	store := newSetStore()
	store.SAdd("nums", 1)
	store.SAdd("letters", "a")
	if store.CurrentSize() != 2 {
		t.Errorf("expected 2 keys, got %d", store.CurrentSize())
	}
}

func TestEvictDoesNotPanicSet(t *testing.T) {
	store := newSetStore()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Evict panicked: %v", r)
		}
	}()
	store.Evict()
}

func TestKeysSet(t *testing.T) {
	store := newSetStore()
	store.SAdd("nums", 1)
	store.SAdd("letters", "a")
	keys := store.Keys()
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}

// -------------------- Integration tests --------------------

func TestAddRemoveIntegration(t *testing.T) {
	store := newSetStore()
	store.SAdd("nums", 1)
	store.SAdd("nums", 2)

	if !store.SIsMember("nums", 2) {
		t.Errorf("expected 2 to be a member")
	}

	store.SRem("nums", 2)
	if store.SIsMember("nums", 2) {
		t.Errorf("expected 2 to be removed")
	}
}

func TestMultipleKeysIntegrationSet(t *testing.T) {
	store := newSetStore()
	store.SAdd("nums", 1)
	store.SAdd("letters", "a")

	if store.SCard("nums") != 1 || store.SCard("letters") != 1 {
		t.Errorf("expected nums=1 and letters=1, got nums=%d letters=%d",
			store.SCard("nums"), store.SCard("letters"))
	}
}

func TestDuplicateAndRemoveIntegration(t *testing.T) {
	store := newSetStore()
	store.SAdd("fruits", "apple")
	store.SAdd("fruits", "apple")
	store.SAdd("fruits", "banana")

	if store.SCard("fruits") != 2 {
		t.Errorf("expected 2 members, got %d", store.SCard("fruits"))
	}

	store.SRem("fruits", "apple")
	if store.SCard("fruits") != 1 {
		t.Errorf("expected 1 member left, got %d", store.SCard("fruits"))
	}
}

// -------------------- helper --------------------

func contains(slice []any, val any) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}

// ------------------------- Large Intergration test of the entire interface -------------------------

// 1. SAdd + Uniqueness Integration
// Ensures that duplicates are not stored and size reflects unique members only.
func TestSAddUniquenessIntegration(t *testing.T) {
	store := newSetStore()

	store.SAdd("nums", 1)
	store.SAdd("nums", 2)
	store.SAdd("nums", 2) // duplicate

	if size := store.SCard("nums"); size != 2 {
		t.Errorf("expected size=2, got %d", size)
	}

	members := store.SMembers("nums")
	if !contains(members, 1) || !contains(members, 2) {
		t.Errorf("expected members [1,2], got %v", members)
	}
}

// 2. SRem + Membership Integration
// Ensures removing an element works and membership reflects removal.
func TestSRemAndMembershipIntegration(t *testing.T) {
	store := newSetStore()

	store.SAdd("letters", "a")
	store.SAdd("letters", "b")

	ok := store.SRem("letters", "a")
	if !ok {
		t.Errorf("expected SRem to succeed for existing element")
	}

	if store.SIsMember("letters", "a") {
		t.Errorf("expected 'a' to be removed")
	}

	if !store.SIsMember("letters", "b") {
		t.Errorf("expected 'b' to remain")
	}
}

// 3. Remove Non-Existent Member Integration
// Ensures removing non-existing elements returns false and doesn't change set.
func TestSRemNonExistentIntegration(t *testing.T) {
	store := newSetStore()

	store.SAdd("nums", 1)
	ok := store.SRem("nums", 99) // doesn't exist
	if ok {
		t.Errorf("expected false when removing non-existent element")
	}

	if size := store.SCard("nums"); size != 1 {
		t.Errorf("expected size=1 after failed removal, got %d", size)
	}
}

// 4. Multiple Keys Isolation Integration
// Ensures sets with different keys are independent.
func TestMultipleKeysIsolationIntegrationSet(t *testing.T) {
	store := newSetStore()

	store.SAdd("nums", 10)
	store.SAdd("letters", "z")

	if !store.SIsMember("nums", 10) {
		t.Errorf("expected 10 in nums")
	}
	if store.SIsMember("nums", "z") {
		t.Errorf("expected z not in nums")
	}

	if !store.SIsMember("letters", "z") {
		t.Errorf("expected z in letters")
	}
}

// 5. Edge Case: Empty Set Operations
// Ensures SMembers on empty set returns empty slice and SCard=0.
func TestEmptySetIntegration(t *testing.T) {
	store := newSetStore()

	if size := store.SCard("missing"); size != 0 {
		t.Errorf("expected 0 size for missing key, got %d", size)
	}

	members := store.SMembers("missing")
	if len(members) != 0 {
		t.Errorf("expected empty slice for SMembers on missing key, got %v", members)
	}
}
