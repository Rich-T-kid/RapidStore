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
