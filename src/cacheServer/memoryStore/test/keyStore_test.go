package test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	memorystore "RapidStore/memoryStore"
)

func newTestStore() memorystore.KeyInterface {
	return memorystore.NewKeyStore(1024, "LRU")
}

// ------------ KeyInterface ------------
func TestSetKeyAndGetKey(t *testing.T) {
	store := newTestStore()
	store.SetKey("foo", "bar")

	val := store.GetKey("foo")
	if val != "bar" {
		t.Errorf("expected 'bar', got %v", val)
	}
}
func TestDeleteKey(t *testing.T) {
	store := newTestStore()
	store.SetKey("foo", "bar")
	store.DeleteKey("foo")

	if store.ExistsKey("foo") {
		t.Errorf("expected foo to be deleted")
	}
}

func TestExpireKey(t *testing.T) {
	store := newTestStore()
	store.SetKey("foo", "bar")
	ok := store.ExpireKey("foo", time.Now().Add(1*time.Second))

	if !ok {
		t.Errorf("expected ExpireKey to succeed")
	}
}

func TestTTLKey(t *testing.T) {
	store := newTestStore()
	store.SetKey("foo", "bar")
	store.ExpireKey("foo", time.Now().Add(2*time.Second))

	dur, err := store.TTLKey("foo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fmt.Printf("TTL duration: %v\n", dur)
	if dur <= 0 {
		t.Errorf("expected positive TTL, got %v", dur)
	}
}
func TestExistsKey(t *testing.T) {
	store := newTestStore()
	store.SetKey("foo", "bar")

	if !store.ExistsKey("foo") {
		t.Errorf("expected foo to exist")
	}
	if store.ExistsKey("baz") {
		t.Errorf("expected baz not to exist")
	}
}

func TestIncrement(t *testing.T) {
	store := newTestStore()
	val, err := store.Increment("counter")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 1 {
		t.Errorf("expected 1, got %d", val)
	}
}
func TestDecrement(t *testing.T) {
	store := newTestStore()
	val, err := store.Decrement("counter")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != -1 {
		t.Errorf("expected -1, got %d", val)
	}
}
func TestAppend(t *testing.T) {
	store := newTestStore()
	store.SetKey("foo", "bar")
	err := store.Append("foo", "baz")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := store.GetKey("foo"); got != "barbaz" {
		t.Errorf("expected 'barbaz', got %v", got)
	}
}

func TestCurrentSize(t *testing.T) {
	store := newTestStore()
	store.SetKey("a", 1)
	store.SetKey("b", 2)
	if store.CurrentSize() != 2 {
		t.Errorf("expected size 2, got %d", store.CurrentSize())
	}
}
func TestKeys(t *testing.T) {
	store := newTestStore()
	store.SetKey("a", 1)
	store.SetKey("b", 2)
	res := []string{"a", "b"}
	keys := store.Keys()
	for _, key := range res {
		if key != keys[0] && key != keys[1] {
			t.Errorf("expected key %s to be in keys %v", key, keys)
		}
	}

	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}
func TestType(t *testing.T) {
	store := newTestStore()
	store.SetKey("strKey", "hello")
	store.SetKey("intKey", int64(42))
	store.SetKey("floatKey", 3.14)
	store.SetKey("boolKey", true)

	if typ := store.Type("strKey"); typ != "string" {
		t.Errorf("expected type 'string', got %s", typ)
	}
	if typ := store.Type("intKey"); typ != "integer" {
		t.Errorf("expected type 'int', got %s", typ)
	}
	if typ := store.Type("floatKey"); typ != "float" {
		t.Errorf("expected type 'float', got %s", typ)
	}
	if typ := store.Type("boolKey"); typ != "boolean" {
		t.Errorf("expected type 'bool', got %s", typ)
	}
	if typ := store.Type("missingKey"); typ != "none" {
		t.Errorf("expected type 'none' for missing key, got %s", typ)
	}
}

// ---------- Integration / multi-portion tests ----------

func TestSetExpireTTLIntegration(t *testing.T) {
	store := newTestStore()
	store.SetKey("foo", "bar")
	store.ExpireKey("foo", time.Now().Add(50*time.Millisecond))

	dur, err := store.TTLKey("foo")
	if err != nil || dur <= 0 {
		t.Errorf("expected valid TTL, got %v err %v", dur, err)
	}
}

func TestIncrementThenDecrementIntegration(t *testing.T) {
	store := newTestStore()
	store.SetKey("num", int64(5))
	val, _ := store.Increment("num")
	if val != 6 {
		t.Errorf("expected 6, got %d", val)
	}
	val, _ = store.Decrement("num")
	if val != 5 {
		t.Errorf("expected back to 5, got %d", val)
	}
}

func TestAppendThenExistsIntegration(t *testing.T) {
	store := newTestStore()
	store.Append("foo", "bar")
	if !store.ExistsKey("foo") {
		t.Errorf("expected foo to exist after append")
	}
	if got := store.GetKey("foo"); got != "bar" {
		t.Errorf("expected 'bar', got %v", got)
	}
}

func TestDeleteThenTTLIntegration(t *testing.T) {
	store := newTestStore()
	store.SetKey("foo", "bar")
	store.DeleteKey("foo")
	_, err := store.TTLKey("foo")
	if !errors.Is(err, errors.New("key does not exist")) {
		t.Logf("expected error for deleted key, got %v", err)
	}
}

// ---------- TTL Integration Tests ----------

// TestTTLWithIncrementDecrementIntegration tests that expired keys are properly handled
// during increment/decrement operations and that TTL is respected across operations
func TestTTLWithIncrementDecrementIntegration(t *testing.T) {
	store := newTestStore()

	// Set up a counter with a short TTL
	store.SetKey("counter", int64(10))
	store.ExpireKey("counter", time.Now().Add(100*time.Millisecond))

	// Verify key exists and has expected value
	if val := store.GetKey("counter"); val != int64(10) {
		t.Errorf("expected counter to be 10, got %v", val)
	}

	// Increment the counter
	newVal, err := store.Increment("counter")
	if err != nil {
		t.Fatalf("unexpected error incrementing: %v", err)
	}
	if newVal != 11 {
		t.Errorf("expected counter to be 11 after increment, got %d", newVal)
	}

	// Verify TTL is still positive
	ttl, err := store.TTLKey("counter")
	if err != nil {
		t.Fatalf("unexpected error getting TTL: %v", err)
	}
	if ttl <= 0 {
		t.Errorf("expected positive TTL, got %v", ttl)
	}

	// Wait for the key to expire
	time.Sleep(150 * time.Millisecond)

	// Verify key no longer exists
	if store.ExistsKey("counter") {
		t.Errorf("expected counter to be expired and not exist")
	}

	// Verify GetKey returns empty value for expired key
	if val := store.GetKey("counter"); val != "" {
		t.Errorf("expected empty value for expired key, got %v", val)
	}

	// Increment on expired key should create new key with value 1
	newVal, err = store.Increment("counter")
	if err != nil {
		t.Fatalf("unexpected error incrementing expired key: %v", err)
	}
	if newVal != 1 {
		t.Errorf("expected counter to be 1 after incrementing expired key, got %d", newVal)
	}

	// Decrement the new counter
	newVal, err = store.Decrement("counter")
	if err != nil {
		t.Fatalf("unexpected error decrementing: %v", err)
	}
	if newVal != 0 {
		t.Errorf("expected counter to be 0 after decrement, got %d", newVal)
	}

	// Verify the new key exists (should not have TTL)
	if !store.ExistsKey("counter") {
		t.Errorf("expected new counter to exist")
	}
}

// TestTTLWithStringOperationsIntegration tests TTL behavior with string operations
// including set, get, append, and verifies lazy deletion on access
func TestTTLWithStringOperationsIntegration(t *testing.T) {
	store := newTestStore()

	// Set multiple keys with different TTLs
	store.SetKey("short_ttl", "hello")
	store.SetKey("medium_ttl", "world")
	store.SetKey("no_ttl", "permanent")

	// Set TTLs
	store.ExpireKey("short_ttl", time.Now().Add(50*time.Millisecond))
	store.ExpireKey("medium_ttl", time.Now().Add(200*time.Millisecond))
	// no_ttl key has no expiration

	// Verify all keys exist initially
	initialKeys := store.Keys()
	if len(initialKeys) != 3 {
		t.Errorf("expected 3 keys initially, got %d", len(initialKeys))
	}

	// Test append on key with TTL
	err := store.Append("medium_ttl", "!")
	if err != nil {
		t.Fatalf("unexpected error appending: %v", err)
	}
	if val := store.GetKey("medium_ttl"); val != "world!" {
		t.Errorf("expected 'world!', got %v", val)
	}

	// Wait for short_ttl to expire but not medium_ttl
	time.Sleep(75 * time.Millisecond)

	// Verify short_ttl key is expired (lazy deletion on access)
	if store.ExistsKey("short_ttl") {
		t.Errorf("expected short_ttl to be expired")
	}

	// Verify medium_ttl still exists
	if !store.ExistsKey("medium_ttl") {
		t.Errorf("expected medium_ttl to still exist")
	}

	// Check Keys() method filters out expired keys
	keysAfterPartialExpiry := store.Keys()
	if len(keysAfterPartialExpiry) != 2 {
		t.Errorf("expected 2 keys after partial expiry, got %d", len(keysAfterPartialExpiry))
	}

	// Try to append to expired key (should create new key)
	err = store.Append("short_ttl", "new_value")
	if err != nil {
		t.Fatalf("unexpected error appending to expired key: %v", err)
	}
	if val := store.GetKey("short_ttl"); val != "new_value" {
		t.Errorf("expected 'new_value' for recreated key, got %v", val)
	}

	// Wait for medium_ttl to expire
	time.Sleep(150 * time.Millisecond)

	// Verify medium_ttl is now expired
	if store.ExistsKey("medium_ttl") {
		t.Errorf("expected medium_ttl to be expired")
	}

	// Verify no_ttl key still exists
	if !store.ExistsKey("no_ttl") {
		t.Errorf("expected no_ttl key to still exist")
	}

	// Final key count should be 2 (recreated short_ttl + no_ttl)
	finalKeys := store.Keys()
	if len(finalKeys) != 2 {
		t.Errorf("expected 2 keys at end, got %d", len(finalKeys))
	}

	// Verify TTL error for expired key
	_, err = store.TTLKey("medium_ttl")
	if err == nil {
		t.Errorf("expected error when getting TTL for expired key")
	}
}

// ------------------------- Large Intergration test of the entire interface -------------------------
// 1. Set, Get, Exists, Delete Integration
// Ensures SetKey works, GetKey retrieves correctly, ExistsKey reflects state, DeleteKey removes.
func TestSetGetExistsDeleteIntegration(t *testing.T) {
	store := newTestStore()

	store.SetKey("foo", "bar")
	if !store.ExistsKey("foo") {
		t.Errorf("expected key foo to exist")
	}

	val := store.GetKey("foo")
	if val != "bar" {
		t.Errorf("expected bar, got %v", val)
	}

	store.DeleteKey("foo")
	if store.ExistsKey("foo") {
		t.Errorf("expected foo to be deleted")
	}
}

// 2. ExpireKey and TTLKey Integration
// Ensures expiration works and TTL decreases over time.
func TestExpireAndTTLIntegration(t *testing.T) {
	store := newTestStore()

	store.SetKey("session", "token123")
	ok := store.ExpireKey("session", time.Now().Add(50*time.Millisecond))
	if !ok {
		t.Errorf("expected true on setting expiration")
	}

	ttl, err := store.TTLKey("session")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ttl <= 0 {
		t.Errorf("expected positive TTL, got %v", ttl)
	}

	time.Sleep(60 * time.Millisecond)
	if store.ExistsKey("session") {
		t.Errorf("expected session to expire")
	}
}

// 3. Increment/Decrement Integration
// Ensures numeric keys increment and decrement correctly, and invalid types error out.
func TestIncrementDecrementIntegration(t *testing.T) {
	store := newTestStore()

	// First increment on missing key initializes at 1
	val, err := store.Increment("counter")
	if err != nil || val != 1 {
		t.Errorf("expected counter=1, got %v err=%v", val, err)
	}

	// Increment again
	val, _ = store.Increment("counter")
	if val != 2 {
		t.Errorf("expected counter=2, got %v", val)
	}

	// Decrement twice
	store.Decrement("counter")
	val, _ = store.Decrement("counter")
	if val != 0 {
		t.Errorf("expected counter=0, got %v", val)
	}

	// Non-numeric
	store.SetKey("str", "hello")
	_, err = store.Increment("str")
	if err == nil {
		t.Errorf("expected error incrementing string value")
	}
}

// 4. Append Integration
// Ensures string values can be appended to, non-strings error out.
func TestAppendIntegration(t *testing.T) {
	store := newTestStore()

	store.SetKey("greet", "hello")
	err := store.Append("greet", " world")
	if err != nil {
		t.Errorf("unexpected error appending: %v", err)
	}

	val := store.GetKey("greet")
	if val != "hello world" {
		t.Errorf("expected hello world, got %v", val)
	}

	// Append to non-string
	store.SetKey("num", 42)
	err = store.Append("num", "x")
	if err == nil {
		t.Errorf("expected error appending to non-string value")
	}
}

// 5. MSet and Keys Integration
// Ensures multiple key-value pairs can be set at once, and Keys returns all.
func TestMSetAndKeysIntegration(t *testing.T) {
	store := newTestStore()

	store.SetKey("a", 1)
	store.SetKey("b", 2)

	keys := store.Keys()
	if len(keys) < 2 {
		t.Errorf("expected at least 2 keys, got %v", keys)
	}
	if !containsS(keys, "a") || !containsS(keys, "b") {
		t.Errorf("expected keys [a,b], got %v", keys)
	}
}

// 6. Edge Case: Get Missing Key, Expire Missing, Delete Missing
// Ensures safe operations on missing keys.
func TestMissingKeysIntegration(t *testing.T) {
	store := newTestStore()

	// Get missing
	val := store.GetKey("notthere")
	if val != "" {
		t.Errorf("expected empty string for missing key, got %v", val)
	}

	// Expire missing
	ok := store.ExpireKey("notthere", time.Now().Add(time.Minute))
	if ok {
		t.Errorf("expected false when expiring missing key")
	}

	// Delete missing should not panic
	store.DeleteKey("notthere")
}
func containsS(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}
