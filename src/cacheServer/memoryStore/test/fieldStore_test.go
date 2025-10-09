package test

import (
	memorystore "RapidStore/memoryStore"
	"fmt"
	"testing"
	"time"
)

func newFieldStore() memorystore.HashTableManager {
	return memorystore.NewFieldStore(1024, "LRU")
}

// -------------------- One test per interface method --------------------

func TestHSetNewKey(t *testing.T) {
	store := newFieldStore()
	created := store.HSet("user:1", "name", "alice", time.Minute)
	if created != false {
		t.Errorf("expected false for new key, got %v", created)
	}
	val, err := store.HGet("user:1", "name")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v := val.(memorystore.GeneralValue).Value; v != "alice" {
		t.Errorf("expected alice, got %v", v)
	}
}

func TestHSetExistingKey(t *testing.T) {
	store := newFieldStore()
	store.HSet("user:1", "name", "alice", time.Minute)
	created := store.HSet("user:1", "name", "bob", time.Minute)

	if created != true {
		t.Errorf("expected true for existing key, got %v", created)
	}

	val, _ := store.HGet("user:1", "name")
	if v := val.(memorystore.GeneralValue).Value; v != "bob" {
		t.Errorf("expected bob, got %v", v)
	}
}

func TestHGetMissingKey(t *testing.T) {
	store := newFieldStore()
	_, err := store.HGet("user:99", "name")
	if err == nil {
		t.Errorf("expected error for missing key")
	}
}

func TestHGetExpiredField(t *testing.T) {
	store := newFieldStore()
	store.HSet("user:1", "age", 30, 1*time.Millisecond)
	time.Sleep(2 * time.Millisecond)

	_, err := store.HGet("user:1", "age")
	if err == nil {
		t.Errorf("expected error for expired field")
	}
}

func TestHGetAllValidFields(t *testing.T) {
	store := newFieldStore()
	store.HSet("user:1", "name", "alice", time.Minute)
	store.HSet("user:1", "age", 30, time.Minute)

	all := store.HGetAll("user:1")
	fmt.Printf("%v\n", all)
	if len(all) != 2 {
		t.Errorf("expected 2 fields, got %d", len(all))
	}
	if all["name"].(memorystore.GeneralValue).Value != "alice" {
		t.Errorf("expected alice")
	}
}

func TestHGetAllSkipsExpired(t *testing.T) {
	store := newFieldStore()
	store.HSet("user:1", "age", 30, 1*time.Millisecond)
	store.HSet("user:1", "city", "NY", time.Minute)
	time.Sleep(2 * time.Millisecond)

	all := store.HGetAll("user:1")
	if len(all) != 1 {
		t.Errorf("expected 1 valid field, got %d", len(all))
	}
	if all["city"].(memorystore.GeneralValue).Value != "NY" {
		t.Errorf("expected NY")
	}
}

func TestHDelRemovesField(t *testing.T) {
	store := newFieldStore()
	store.HSet("user:1", "name", "alice", time.Minute)
	store.HDel("user:1", "name")

	_, err := store.HGet("user:1", "name")
	if err == nil {
		t.Errorf("expected error after HDel")
	}
}

func TestHDelNonExistentField(t *testing.T) {
	store := newFieldStore()
	store.HDel("user:1", "random") // should not panic
}

func TestHExistsMissingField(t *testing.T) {
	store := newFieldStore()
	if store.HExists("user:1", "name") {
		t.Errorf("expected field not to exist")
	}
}

func TestHExistsExpiredField(t *testing.T) {
	store := newFieldStore()
	store.HSet("user:1", "age", 30, 1*time.Millisecond)
	time.Sleep(2 * time.Millisecond)
	if store.HExists("user:1", "age") {
		t.Errorf("expected field to be expired")
	}
}

func TestCurrentSizeField(t *testing.T) {
	store := newFieldStore()
	store.HSet("user:1", "name", "alice", time.Minute)
	store.HSet("user:2", "name", "bob", time.Minute)
	if store.CurrentSize() != 2 {
		t.Errorf("expected size 2, got %d", store.CurrentSize())
	}
}

// -------------------- Integration tests --------------------
func TestHSetGetDeleteIntegration(t *testing.T) {
	store := newFieldStore()
	store.HSet("user:1", "name", "alice", time.Minute)
	val, _ := store.HGet("user:1", "name")
	if v := val.(memorystore.GeneralValue).Value; v != "alice" {
		t.Errorf("expected alice, got %v", v)
	}

	store.HDel("user:1", "name")
	if store.HExists("user:1", "name") {
		t.Errorf("expected field deleted")
	}
}

func TestMultipleFieldsIntegration(t *testing.T) {
	store := newFieldStore()
	store.HSet("user:1", "name", "alice", time.Minute)
	store.HSet("user:1", "age", 30, time.Minute)
	store.HSet("user:1", "city", "NY", time.Minute)

	all := store.HGetAll("user:1")
	if len(all) != 3 {
		t.Errorf("expected 3 fields, got %d", len(all))
	}
}

func TestExpirationIntegration(t *testing.T) {
	store := newFieldStore()
	store.HSet("user:1", "token", "xyz", 1*time.Millisecond)
	time.Sleep(2 * time.Millisecond)

	if store.HExists("user:1", "token") {
		t.Errorf("expected token to expire")
	}
}

func TestOverwriteIntegration(t *testing.T) {
	store := newFieldStore()
	store.HSet("user:1", "name", "alice", time.Minute)
	store.HSet("user:1", "name", "bob", time.Minute)

	val, _ := store.HGet("user:1", "name")
	if v := val.(memorystore.GeneralValue).Value; v != "bob" {
		t.Errorf("expected bob, got %v", v)
	}
}

// ------------------------- Large Intergration test of the entire interface -------------------------
// 1. Overwrite + Expiration Integration
// Ensures that setting a field twice overwrites the old value,
// and that TTL expiration actually removes the field.
func TestHSetOverwriteAndExpireIntegration(t *testing.T) {
	store := newFieldStore()

	// First set
	created := store.HSet("user:1", "name", "alice", 50*time.Millisecond)
	if created {
		t.Errorf("expected new hashset creation to return false")
	}

	// Overwrite same field
	created = store.HSet("user:1", "name", "bob", 50*time.Millisecond)
	if !created {
		t.Errorf("expected overwrite to return true")
	}

	val, _ := store.HGet("user:1", "name")
	if gv := val.(memorystore.GeneralValue).Value; gv != "bob" {
		t.Errorf("expected bob, got %v", gv)
	}

	// Wait for expiration
	time.Sleep(60 * time.Millisecond)

	if store.HExists("user:1", "name") {
		t.Errorf("expected field to expire")
	}
}

// 2. HGetAll With Mixed Expired And Valid Fields
// Ensures expired fields are skipped, valid fields remain.
func TestHGetAllWithExpiredAndValidIntegration(t *testing.T) {
	store := newFieldStore()

	store.HSet("user:2", "name", "charlie", 1*time.Millisecond)
	store.HSet("user:2", "age", 30, time.Minute)

	time.Sleep(5 * time.Millisecond) // allow "name" to expire

	all := store.HGetAll("user:2")

	if _, ok := all["name"]; ok {
		t.Errorf("expected expired field 'name' to be skipped")
	}
	if v := all["age"].(memorystore.GeneralValue).Value; v != 30 {
		t.Errorf("expected age=30, got %v", v)
	}
}

// 3. Delete Then Reinsert Integration
// Ensures that deleting a field really removes it and that it can be reinserted cleanly.
func TestHDelAndReinsertIntegration(t *testing.T) {
	store := newFieldStore()

	store.HSet("user:3", "city", "NYC", time.Minute)
	if !store.HExists("user:3", "city") {
		t.Fatalf("expected field to exist before deletion")
	}

	store.HDel("user:3", "city")
	if store.HExists("user:3", "city") {
		t.Errorf("expected field to be deleted")
	}

	// Reinsert
	store.HSet("user:3", "city", "Boston", time.Minute)
	val, _ := store.HGet("user:3", "city")
	if gv := val.(memorystore.GeneralValue).Value; gv != "Boston" {
		t.Errorf("expected Boston, got %v", gv)
	}
}

// 4. Multiple Keys Isolation Integration
// Ensures different keys do not interfere with each other’s fields.
func TestMultipleKeysIsolationIntegration(t *testing.T) {
	store := newFieldStore()

	store.HSet("user:4", "name", "dave", time.Minute)
	store.HSet("user:5", "name", "eve", time.Minute)

	val1, _ := store.HGet("user:4", "name")
	val2, _ := store.HGet("user:5", "name")

	if gv := val1.(memorystore.GeneralValue).Value; gv != "dave" {
		t.Errorf("expected dave, got %v", gv)
	}
	if gv := val2.(memorystore.GeneralValue).Value; gv != "eve" {
		t.Errorf("expected eve, got %v", gv)
	}
}

// 5. Edge Case: HDel Non-Existent Field + HGet Non-Existent Field
// Ensures deleting missing fields is a no-op, and HGet returns an error for missing fields.
func TestHDelAndHGetMissingFieldIntegration(t *testing.T) {
	store := newFieldStore()

	// Delete on empty key should not panic or fail
	store.HDel("user:6", "nickname")

	// HGet on missing key
	_, err := store.HGet("user:6", "nickname")
	if err == nil {
		t.Errorf("expected error fetching missing field")
	}

	// HExists should be false
	if store.HExists("user:6", "nickname") {
		t.Errorf("expected false for non-existent field")
	}
}
