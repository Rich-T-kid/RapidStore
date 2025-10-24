package memorystore

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestAdaptableState(t *testing.T) {
	t.Run("MockState Serialize/Deserialize", func(t *testing.T) {
		// Create original mock state with test data
		original := &MockState{
			ID:     42,
			Name:   "test-state",
			Values: []int{1, 2, 3, 4, 5},
			Metadata: map[string]string{
				"version": "1.0",
				"type":    "mock",
				"env":     "test",
			},
		}

		// Serialize the state
		data, err := original.Serialize()
		if err != nil {
			t.Fatalf("Failed to serialize state: %v", err)
		}

		// Create new instance and deserialize
		restored := NewMockState().(*MockState)
		err = restored.Deserialize(data)
		if err != nil {
			t.Fatalf("Failed to deserialize state: %v", err)
		}

		// Verify all fields match
		if original.ID != restored.ID {
			t.Errorf("ID mismatch: expected %d, got %d", original.ID, restored.ID)
		}

		if original.Name != restored.Name {
			t.Errorf("Name mismatch: expected %s, got %s", original.Name, restored.Name)
		}

		if !reflect.DeepEqual(original.Values, restored.Values) {
			t.Errorf("Values mismatch: expected %v, got %v", original.Values, restored.Values)
		}

		if !reflect.DeepEqual(original.Metadata, restored.Metadata) {
			t.Errorf("Metadata mismatch: expected %v, got %v", original.Metadata, restored.Metadata)
		}
	})

	t.Run("Empty State Serialize/Deserialize", func(t *testing.T) {
		// Test with empty/default state
		original := NewMockState().(*MockState)

		data, err := original.Serialize()
		if err != nil {
			t.Fatalf("Failed to serialize empty state: %v", err)
		}

		restored := NewMockState().(*MockState)
		err = restored.Deserialize(data)
		if err != nil {
			t.Fatalf("Failed to deserialize empty state: %v", err)
		}

		// Verify empty state is preserved
		if !reflect.DeepEqual(original, restored) {
			t.Errorf("Empty state mismatch: expected %+v, got %+v", original, restored)
		}
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		// Verify MockState implements AdaptableState interface
		var _ AdaptableState = &MockState{}
		var _ AdaptableState = NewMockState()

		// Test interface methods work
		state := NewMockState()
		data, err := state.Serialize()
		if err != nil {
			t.Fatalf("Interface Serialize failed: %v", err)
		}

		newState := NewMockState()
		err = newState.Deserialize(data)
		if err != nil {
			t.Fatalf("Interface Deserialize failed: %v", err)
		}
	})
}
func TestBasic(t *testing.T) {
	original := &MockState{
		ID:     42,
		Name:   "test-state",
		Values: []int{1, 2, 3, 4, 5},
		Metadata: map[string]string{
			"version": "1.0",
			"type":    "mock",
			"env":     "test",
		},
	}
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize state: %v", err)
	}
	v := make(map[string]interface{})
	err = json.Unmarshal(data, &v)
	if err != nil {
		t.Fatalf("Failed to unmarshal serialized data: %v", err)
	}
	fmt.Printf("Serialized data as map: %+v\n", v)
}

func TestRapidStoreSerializationEquivalence(t *testing.T) {
	store := NewRapidStore()
	populateRapidStoreCache(store)

	content, err := store.Serialize()
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}

	v := NewRapidStore()
	if err := v.Deserialize(content); err != nil {
		t.Fatalf("Deserialize error: %v", err)
	}

	origBytes, _ := store.Serialize()
	reloadBytes, _ := v.Serialize()

	var orig, reload map[string]interface{}
	json.Unmarshal(origBytes, &orig)
	json.Unmarshal(reloadBytes, &reload)

	if !deepEqualIgnoringSetOrder(orig, reload) {
		t.Fatalf("State mismatch after round-trip (ignoring set order)\nOriginal: %s\nReloaded: %s", origBytes, reloadBytes)
	}
}

func deepEqualIgnoringSetOrder(a, b interface{}) bool {
	switch aVal := a.(type) {
	case []interface{}:
		bVal, ok := b.([]interface{})
		if !ok || len(aVal) != len(bVal) {
			return false
		}
		// Treat slices as sets (order doesn't matter)
		setA := map[interface{}]struct{}{}
		for _, v := range aVal {
			setA[v] = struct{}{}
		}
		for _, v := range bVal {
			if _, ok := setA[v]; !ok {
				return false
			}
		}
		return true
	case map[string]interface{}:
		bVal, ok := b.(map[string]interface{})
		if !ok || len(aVal) != len(bVal) {
			return false
		}
		for k, v := range aVal {
			if !deepEqualIgnoringSetOrder(v, bVal[k]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a, b)
	}
}

// populateRapidStoreCache populates the cache with different data types for testing
func populateRapidStoreCache(store *RapidStoreServer) {
	// Populate Key-Value pairs (simple strings, numbers, booleans)
	store.SetKey("user:1:name", "Alice")
	store.SetKey("user:1:age", 30)
	store.SetKey("user:1:active", true)
	store.SetKey("counter", 100)
	store.SetKey("temperature", 98.6)

	// Populate Hash/Field data (user profiles, settings)
	store.HSet("user:2:profile", "name", "Bob")
	store.HSet("user:2:profile", "email", "bob@example.com")
	store.HSet("user:2:profile", "age", 25)
	store.HSet("user:2:profile", "city", "New York")

	store.HSet("app:settings", "theme", "dark")
	store.HSet("app:settings", "notifications", true)
	store.HSet("app:settings", "timeout", 30)

	// Populate Lists/Sequences (task queues, recent items)
	store.LPush("task:queue", "task1")
	store.LPush("task:queue", "task2")
	store.RPush("task:queue", "task3")
	store.RPush("task:queue", "task4")

	store.LPush("recent:views", "page1")
	store.LPush("recent:views", "page2")
	store.LPush("recent:views", "page3")

	// Populate Sets (unique items, tags, permissions)
	store.SAdd("user:1:permissions", "read")
	store.SAdd("user:1:permissions", "write")
	store.SAdd("user:1:permissions", "admin")

	store.SAdd("tags:article:123", "golang")
	store.SAdd("tags:article:123", "programming")
	store.SAdd("tags:article:123", "tutorial")
	store.SAdd("tags:article:123", "backend")

	// Populate Sorted Sets (leaderboards, rankings)
	store.ZAdd("game:leaderboard", 1500.0, "player1")
	store.ZAdd("game:leaderboard", 2300.0, "player2")
	store.ZAdd("game:leaderboard", 1800.0, "player3")
	store.ZAdd("game:leaderboard", 2100.0, "player4")

	store.ZAdd("popular:articles", 150.0, "article1")
	store.ZAdd("popular:articles", 300.0, "article2")
	store.ZAdd("popular:articles", 89.0, "article3")
}
func compareStringSlices(a, b []string) bool {
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

// populateCache populates the cache interface with different data types for testing
func populateCache(cache Cache) {
	// Populate Key-Value pairs (simple strings, numbers, booleans)
	cache.SetKey("user:1:name", "Alice")
	cache.SetKey("user:1:age", 30)
	cache.SetKey("user:1:active", true)
	cache.SetKey("counter", 100)
	cache.SetKey("temperature", 98.6)

	// Populate Hash/Field data (user profiles, settings)
	cache.HSet("user:2:profile", "name", "Bob")
	cache.HSet("user:2:profile", "email", "bob@example.com")
	cache.HSet("user:2:profile", "age", 25)
	cache.HSet("user:2:profile", "city", "New York")

	cache.HSet("app:settings", "theme", "dark")
	cache.HSet("app:settings", "notifications", true)
	cache.HSet("app:settings", "timeout", 30)

	// Populate Lists/Sequences (task queues, recent items)
	cache.LPush("task:queue", "task1")
	cache.LPush("task:queue", "task2")
	cache.RPush("task:queue", "task3")
	cache.RPush("task:queue", "task4")

	cache.LPush("recent:views", "page1")
	cache.LPush("recent:views", "page2")
	cache.LPush("recent:views", "page3")

	// Populate Sets (unique items, tags, permissions)
	cache.SAdd("user:1:permissions", "read")
	cache.SAdd("user:1:permissions", "write")
	cache.SAdd("user:1:permissions", "admin")

	cache.SAdd("tags:article:123", "golang")
	cache.SAdd("tags:article:123", "programming")
	cache.SAdd("tags:article:123", "tutorial")
	cache.SAdd("tags:article:123", "backend")

	// Populate Sorted Sets (leaderboards, rankings)
	cache.ZAdd("game:leaderboard", 1500.0, "player1")
	cache.ZAdd("game:leaderboard", 2300.0, "player2")
	cache.ZAdd("game:leaderboard", 1800.0, "player3")
	cache.ZAdd("game:leaderboard", 2100.0, "player4")

	cache.ZAdd("popular:articles", 150.0, "article1")
	cache.ZAdd("popular:articles", 300.0, "article2")
	cache.ZAdd("popular:articles", 89.0, "article3")
}

func TestCacheInterfaceSerializationEquivalence(t *testing.T) {
	store := NewCache()
	populateCache(store)

	content, err := store.Serialize()
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}

	v := NewCache()
	if err := v.Deserialize(content); err != nil {
		t.Fatalf("Deserialize error: %v", err)
	}

	origBytes, _ := store.Serialize()
	reloadBytes, _ := v.Serialize()

	var orig, reload map[string]interface{}
	json.Unmarshal(origBytes, &orig)
	json.Unmarshal(reloadBytes, &reload)

	if !deepEqualIgnoringSetOrder(orig, reload) {
		t.Fatalf("State mismatch after round-trip (ignoring set order)\nOriginal: %s\nReloaded: %s", origBytes, reloadBytes)
	}
}

func TestCacheTruncatedSerializationShouldFail(t *testing.T) {
	store := NewCache()
	populateCache(store)

	content, err := store.Serialize()
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}

	// Truncate the last 10 bytes (simulate corruption)
	if len(content) > 10 {
		content = content[:len(content)-10]
	}

	v := NewCache()
	_ = v.Deserialize(content) // ignore error for this test

	origBytes, _ := store.Serialize()
	reloadBytes, _ := v.Serialize()

	var orig, reload map[string]interface{}
	json.Unmarshal(origBytes, &orig)
	json.Unmarshal(reloadBytes, &reload)

	if deepEqualIgnoringSetOrder(orig, reload) {
		t.Fatalf("Truncated state should NOT match after round-trip!\nOriginal: %s\nReloaded: %s", origBytes, reloadBytes)
	}
}
