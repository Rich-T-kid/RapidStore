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
