package test

import (
	memorystore "RapidStore/memoryStore"
	"testing"
)

func newListStore() memorystore.ListManager {
	return memorystore.NewListManager(200)
}

// -------------------- One test per ListManager method --------------------

func TestLPushNewKey(t *testing.T) {
	store := newListStore()
	if err := store.LPush("nums", 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v, e := store.LPop("nums")
	if e != nil {
		t.Fatalf("unexpected error:  %v", e)
	}
	if v != 1 {
		t.Fatalf("expected 1 got %d", v)
	}
}

func TestRPushNewKey(t *testing.T) {
	store := newListStore()
	if err := store.RPush("letters", "a"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	res, _ := store.LRange("letters", 0, -1)
	if len(res) != 1 || res[0] != "a" {
		t.Errorf("expected [a], got %v", res)
	}
}

func TestLPopEmptyList(t *testing.T) {
	store := newListStore()
	_, err := store.LPop("missing")
	if err == nil {
		t.Errorf("expected error popping from empty list")
	}
}

func TestRPopEmptyList(t *testing.T) {
	store := newListStore()
	_, err := store.RPop("missing")
	if err == nil {
		t.Errorf("expected error popping from empty list")
	}
}

func TestLPopAfterPush(t *testing.T) {
	store := newListStore()
	store.RPush("nums", 1)
	store.RPush("nums", 2)
	// 1 -> 2
	// -> 2
	val, err := store.LPop("nums")
	if err != nil || val != 1 {
		t.Errorf("expected 1, got %v (err=%v)", val, err)
	}
}

func TestRPopAfterPush(t *testing.T) {
	store := newListStore()
	store.LPush("nums", 1)
	store.RPush("nums", 2)
	val, err := store.RPop("nums")
	if err != nil || val != 2 {
		t.Errorf("expected 2, got %v (err=%v)", val, err)
	}
}

func TestLRangeEmpty(t *testing.T) {
	store := newListStore()
	_, err := store.LRange("missing", 0, -1)
	if err == nil {
		t.Errorf("expected error for missing key")
	}
}

func TestLRangeValid(t *testing.T) {
	store := newListStore()
	store.RPush("nums", 1)
	store.RPush("nums", 2)
	store.RPush("nums", 3)

	res, err := store.LRange("nums", 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 2 || res[0] != 1 || res[1] != 2 {
		t.Errorf("expected [1 2], got %v", res)
	}
}

func TestSizePerKey(t *testing.T) {
	store := newListStore()
	store.RPush("a", 1)
	store.RPush("a", 2)
	store.RPush("b", "x")

	if s := store.Size("a"); s != 2 {
		t.Errorf("expected size of list a = 2, got %d", s)
	}
	if s := store.Size("b"); s != 1 {
		t.Errorf("expected size of list b = 1, got %d", s)
	}
	if s := store.Size("missing"); s != 0 {
		t.Errorf("expected 0 for missing key, got %d", s)
	}
}

func TestCurrentSizeOrdered(t *testing.T) {
	store := newListStore()
	store.RPush("a", 1)
	store.RPush("b", 2)
	if store.CurrentSize() != 2 {
		t.Errorf("expected 2 keys, got %d", store.CurrentSize())
	}
}

// TODO: eviction policy
func TestEvictDoesNotPanic(t *testing.T) {
	t.Skip("Skipping Evict test for now")
}

func TestKeysOrdered(t *testing.T) {
	store := newListStore()
	store.RPush("foo", 1)
	store.RPush("bar", 2)
	keys := store.Keys()
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}

// -------------------- Integration tests --------------------

func TestPushPopIntegration(t *testing.T) {
	store := newListStore()
	store.RPush("nums", 1)
	store.RPush("nums", 2)
	store.RPush("nums", 3)

	v1, _ := store.LPop("nums")
	v2, _ := store.RPop("nums")

	if v1 != 1 || v2 != 3 {
		t.Errorf("expected left=1 right=3, got %v %v", v1, v2)
	}
}

func TestOrderIntegration(t *testing.T) {
	store := newListStore()
	store.RPush("nums", 1) // [1]
	store.RPush("nums", 2) // [1,2]
	store.LPush("nums", 0) // [0,1,2]

	res, _ := store.LRange("nums", 0, -1)
	expected := []any{0, 1, 2}
	for i, v := range expected {
		if res[i] != v {
			t.Errorf("expected %v at index %d, got %v", v, i, res[i])
		}
	}
}

func TestMultipleKeysIntegration(t *testing.T) {
	store := newListStore()
	store.RPush("nums", 1)
	store.RPush("letters", "a")

	if store.CurrentSize() != 2 {
		t.Errorf("expected 2 keys, got %d", store.CurrentSize())
	}
	if store.Size("nums") != 1 || store.Size("letters") != 1 {
		t.Errorf("expected sizes 1 each, got nums=%d letters=%d",
			store.Size("nums"), store.Size("letters"))
	}
}

func TestRangeBoundsIntegration(t *testing.T) {
	store := newListStore()
	for i := 1; i <= 5; i++ {
		store.RPush("nums", i)
	}
	res, _ := store.LRange("nums", 1, 3)
	if len(res) != 3 || res[0] != 2 || res[2] != 4 {
		t.Errorf("expected [2 3 4], got %v", res)
	}
}

func TestPopExhaustsListIntegration(t *testing.T) {
	store := newListStore()
	store.RPush("nums", 1)
	store.RPush("nums", 2)

	store.LPop("nums")
	store.LPop("nums")
	_, err := store.LPop("nums")
	if err == nil {
		t.Errorf("expected error popping exhausted list")
	}
}
