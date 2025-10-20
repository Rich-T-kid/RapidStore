package server

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// helper to create a WAL backed by a temp file and return a cleanup func
func makeTempWAL(t *testing.T, maxSize uint32, maxDuration time.Duration) (*WriteAheadLog, func()) {
	t.Helper()
	tf, err := os.CreateTemp("", "wal_test_*.wal")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	_ = tf.Close()

	w := newWAL(tf.Name(), maxSize, maxDuration)

	cleanup := func() {
		// cancel background goroutine and close file
		w.sync()
		_ = os.Remove(tf.Name())
	}

	return w, cleanup
}

// Test 1: append basic key commands and ensure Append returns no error and sequenceNumber matches
func TestAppendKeys(t *testing.T) {
	w, cleanup := makeTempWAL(t, 1024, 500*time.Millisecond)
	defer cleanup()

	entries := []entryLog{
		NewSetEntry("user:1", "alice", 0),
		NewGetEntry("user:1"),
		NewDeleteKeyEntry("user:1"),
		NewExpireKey("user:2", 10*time.Second),
		NewExistKey("user:3"),
		NewType("user:4"),
		NewIncrement("counter"),
		NewDecrement("counter"),
		NewAppend("list:key", "suf"),
	}
	var size uint64
	for i, e := range entries {
		size += uint64(len(e)) + 20 // entry plus overhead of each entry
		if err := w.Append(e); err != nil {
			t.Fatalf("append #%d failed: %v", i, err)
		}
	}

	got := atomic.LoadUint64(&w.sequenceNumber)
	if got != size {
		t.Fatalf("expected sequenceNumber %d, got %d", size, got)
	}
}

// Test 2: append various data-structure operations (hash/list/set/sortedset) and ensure no error
func TestAppendDataStructures(t *testing.T) {
	w, _ := makeTempWAL(t, 2048, 500*time.Millisecond)
	//defer cleanup()

	entries := []entryLog{
		NewHSet("hash:1", "fieldA", "valueA", time.Millisecond*60),
		NewHGet("hash:1", "fieldA"),
		NewHGetAll("hash:1"),
		NewHDel("hash:1", "fieldA"),
		NewHExists("hash:1", "fieldA"),

		NewLPush("list:1", "a"),
		NewRPush("list:1", "b"),
		NewLPop("list:1"),
		NewRPop("list:1"),
		NewLRange("list:1", 0, 10),

		NewSAdd("set:1", "m1"),
		NewSMembers("set:1"),
		NewSRem("set:1", "m1"),
		NewSIsMember("set:1", "m1"),
		NewSCard("set:1"),

		NewZAdd("zset:1", 10.5, "member1"),
		NewZRemove("zset:1", "member1"),
		NewZRange("zset:1", 0, -1, true),
		NewZRank("zset:1", "member1"),
		NewZRevRank("zset:1", "member1"),
		NewZScore("zset:1", "member1"),
	}
	var size uint64
	for i, e := range entries {
		size += uint64(len(e)) + 20 // entry plus overhead of each entry
		if err := w.Append(e); err != nil {
			t.Fatalf("append data-struct #%d failed: %v", i, err)
		}
	}
	got := atomic.LoadUint64(&w.sequenceNumber)
	if got != size {
		t.Fatalf("expected sequenceNumber %d, got %d", size, got)
	}
}

func TestRead(t *testing.T) {
	wal, _ := makeTempWAL(t, 4096, time.Second)
	defer wal.Close()
	// Write test data
	wal.Append(NewSAdd("user", 1))
	wal.Append(NewSAdd("user", 2))
	wal.Append(NewSAdd("user", 3))
	wal.Append(NewSAdd("user", 4))
	wal.Append(NewSAdd("user", 5))

	// Close and reopen for reading
	fileName := wal.file.Name()
	wal.Close()

	file, err := os.Open(fileName)
	if err != nil {
		t.Fatalf("Failed to open file for reading: %v", err)
	}
	defer file.Close()

	// Read from channel
	entryChan := wal.ReadWal(file)

	entryCount := 0
	for entry := range entryChan {
		fmt.Printf("entry %d -> MagicNumber: %d, Size: %d, Entry: %s, Checksum: %d\n",
			entryCount, entry.MagicNumber, entry.EntrySize, string(entry.Entry), entry.Checksum)
		entryCount++
	}

	if entryCount != 5 {
		t.Errorf("Expected 5 entries, got %d", entryCount)
	}

	fmt.Printf("Successfully read %d entries from WAL\n", entryCount)
}

func TestWALReadWrite(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "wal_test_*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create WAL instance
	wal := newWAL(tmpFile.Name(), 4096, time.Second)
	defer wal.Close()

	// Test data - 4 operations
	testEntries := []string{
		"SET key1 value1",
		"SET key2 value2",
		"DEL key1",
		"INCR counter",
	}

	// Write entries to WAL
	for _, entry := range testEntries {
		err := wal.Append(entryLog(entry))
		if err != nil {
			t.Fatalf("Failed to append entry '%s': %v", entry, err)
		}
	}

	// Force flush and close to ensure data is written
	wal.Close()

	// Reopen file for reading
	file, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open file for reading: %v", err)
	}
	defer file.Close()

	// Read entries back from channel
	entryChan := wal.ReadWal(file)

	var readEntries []walEntry
	for entry := range entryChan {
		readEntries = append(readEntries, entry)
	}

	// Verify we got the same number of entries
	if len(readEntries) != len(testEntries) {
		t.Fatalf("Expected %d entries, got %d", len(testEntries), len(readEntries))
	}

	// Verify each entry content matches
	for i, expected := range testEntries {
		actual := string(readEntries[i].Entry)
		if actual != expected {
			t.Errorf("Entry %d: expected '%s', got '%s'", i, expected, actual)
		}

		// Verify magic number
		if readEntries[i].MagicNumber != magicNumber {
			t.Errorf("Entry %d: wrong magic number, expected %d, got %d",
				i, magicNumber, readEntries[i].MagicNumber)
		}

		// Verify entry size
		expectedSize := uint32(len(expected))
		if readEntries[i].EntrySize != expectedSize {
			t.Errorf("Entry %d: wrong size, expected %d, got %d",
				i, expectedSize, readEntries[i].EntrySize)
		}
	}

	t.Logf("Successfully wrote and read %d entries", len(testEntries))
}

func TestWALSingleEntry(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "wal_single_*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create WAL instance
	wal := newWAL(tmpFile.Name(), 4096, time.Second)

	// Write a single entry
	testEntry := "HSET user:123 name alice"
	err = wal.Append(entryLog(testEntry))
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	wal.Close()

	// Read it back
	file, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	entryChan := wal.ReadWal(file)

	// Should get exactly one entry
	entry, ok := <-entryChan
	if !ok {
		t.Fatal("Expected one entry, but channel was closed")
	}

	if string(entry.Entry) != testEntry {
		t.Errorf("Expected '%s', got '%s'", testEntry, string(entry.Entry))
	}

	// Should be no more entries
	_, stillOpen := <-entryChan
	if stillOpen {
		t.Error("Expected channel to be closed after one entry")
	}

	t.Logf("Successfully wrote and read single entry: %s", testEntry)
}

// Comprehensive WAL integration test with updateState validation
func TestWALUpdateStateIntegration(t *testing.T) {
	// Create server instance
	server := NewServer()

	// Create temporary WAL file
	tmpFile, err := os.CreateTemp("", "wal_integration_*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create WAL instance
	wal := newWAL(tmpFile.Name(), 4096, time.Second)
	defer wal.Close()

	// Test 1: Key operations
	t.Run("KeyOperations", func(t *testing.T) {
		keyOps := []entryLog{
			NewSetEntry("user:123", "alice", 300*time.Second),
			NewSetEntry("counter", "10", 0), // Set as string, increment/decrement will convert to int64
			NewIncrement("counter"),
			NewIncrement("counter"),
			NewDecrement("counter"),
			NewSetEntry("greeting", "", time.Minute), // Initialize greeting key first
			NewAppend("greeting", "Hello"),
			NewAppend("greeting", " World"),
			NewExpireKey("temp", 60*time.Second),
		}

		// Write to WAL
		for _, op := range keyOps {
			if err := wal.Append(op); err != nil {
				t.Fatalf("Failed to append to WAL: %v", err)
			}
		}

		// Apply to server state
		for _, op := range keyOps {
			if err := server.updateState(op); err != nil {
				t.Errorf("updateState failed for %s: %v", string(op), err)
			}
		}

		// Validate results
		if val := server.ramCache.GetKey("user:123"); val != "alice" {
			t.Errorf("Expected user:123='alice', got %v", val)
		}

		if val := server.ramCache.GetKey("counter"); val != int64(11) {
			t.Errorf("Expected counter=11 (int64), got %v of type %T", val, val)
		}

		if val := server.ramCache.GetKey("greeting"); val != "Hello World" {
			t.Errorf("Expected greeting='Hello World', got %v", val)
		}

		// Check TTL was set
		if ttl, err := server.ramCache.TTLKey("user:123"); err != nil || ttl <= 0 {
			t.Errorf("Expected positive TTL for user:123, got %v (err: %v)", ttl, err)
		}
	})

	// Test 2: Hash operations
	t.Run("HashOperations", func(t *testing.T) {
		hashOps := []entryLog{
			NewHSet("profile:user1", "name", "john", 3600*time.Second),
			NewHSet("profile:user1", "age", "25", 3600*time.Second),
			NewHSet("profile:user1", "city", "NYC", 3600*time.Second),
			NewHDel("profile:user1", "age"),
			NewHSet("settings:app", "theme", "dark", 3600*time.Second),
		}

		// Write and apply
		for _, op := range hashOps {
			if err := wal.Append(op); err != nil {
				t.Fatalf("Failed to append hash op: %v", err)
			}
			if err := server.updateState(op); err != nil {
				t.Errorf("updateState failed for hash op %s: %v", string(op), err)
			}
		}

		// Validate hash results
		if name, err := server.ramCache.HGet("profile:user1", "name"); err != nil || name != "john" {
			t.Errorf("Expected name='john', got %v (err: %v)", name, err)
		}

		if server.ramCache.HExists("profile:user1", "age") {
			t.Error("Field 'age' should have been deleted")
		}

		if city, err := server.ramCache.HGet("profile:user1", "city"); err != nil || city != "NYC" {
			t.Errorf("Expected city='NYC', got %v (err: %v)", city, err)
		}

		if theme, err := server.ramCache.HGet("settings:app", "theme"); err != nil || theme != "dark" {
			t.Errorf("Expected theme='dark', got %v (err: %v)", theme, err)
		}
	})

	// Test 3: List operations
	t.Run("ListOperations", func(t *testing.T) {
		listOps := []entryLog{
			NewLPush("tasks", "task1"),
			NewLPush("tasks", "task2"),
			NewRPush("tasks", "task3"),
			NewRPush("queue", "item1"),
			NewLPush("queue", "item0"),
		}

		// Write and apply
		for _, op := range listOps {
			if err := wal.Append(op); err != nil {
				t.Fatalf("Failed to append list op: %v", err)
			}
			if err := server.updateState(op); err != nil {
				t.Errorf("updateState failed for list op %s: %v", string(op), err)
			}
		}

		// Validate list results
		if size := server.ramCache.Size("tasks"); size != 3 {
			t.Errorf("Expected tasks list size 3, got %d", size)
		}

		if size := server.ramCache.Size("queue"); size != 2 {
			t.Errorf("Expected queue list size 2, got %d", size)
		}
	})

	// Test 4: Set operations
	t.Run("SetOperations", func(t *testing.T) {
		setOps := []entryLog{
			NewSAdd("users", "alice"),
			NewSAdd("users", "bob"),
			NewSAdd("users", "charlie"),
			NewSAdd("admins", "alice"),
			NewSRem("users", "bob"),
		}

		// Write and apply
		for _, op := range setOps {
			if err := wal.Append(op); err != nil {
				t.Fatalf("Failed to append set op: %v", err)
			}
			if err := server.updateState(op); err != nil {
				t.Errorf("updateState failed for set op %s: %v", string(op), err)
			}
		}

		// Validate set results
		if !server.ramCache.SIsMember("users", "alice") {
			t.Error("alice should be in users set")
		}

		if server.ramCache.SIsMember("users", "bob") {
			t.Error("bob should have been removed from users set")
		}

		if !server.ramCache.SIsMember("users", "charlie") {
			t.Error("charlie should be in users set")
		}

		if !server.ramCache.SIsMember("admins", "alice") {
			t.Error("alice should be in admins set")
		}

		if card := server.ramCache.SCard("users"); card != 2 {
			t.Errorf("Expected users set cardinality 2, got %d", card)
		}
	})

	// Test 5: Sorted Set operations
	t.Run("SortedSetOperations", func(t *testing.T) {
		zsetOps := []entryLog{
			NewZAdd("leaderboard", 100.5, "player1"),
			NewZAdd("leaderboard", 200.0, "player2"),
			NewZAdd("leaderboard", 150.75, "player3"),
			NewZRemove("leaderboard", "player2"),
			NewZAdd("scores", 50.0, "game1"),
		}

		// Write and apply
		for _, op := range zsetOps {
			if err := wal.Append(op); err != nil {
				t.Fatalf("Failed to append zset op: %v", err)
			}
			if err := server.updateState(op); err != nil {
				t.Errorf("updateState failed for zset op %s: %v", string(op), err)
			}
		}

		// Validate sorted set results
		if score, exists := server.ramCache.ZScore("leaderboard", "player1"); !exists || score != 100.5 {
			t.Errorf("Expected player1 score 100.5, got %f (exists: %v)", score, exists)
		}

		if score, exists := server.ramCache.ZScore("leaderboard", "player3"); !exists || score != 150.75 {
			t.Errorf("Expected player3 score 150.75, got %f (exists: %v)", score, exists)
		}

		if _, exists := server.ramCache.ZScore("leaderboard", "player2"); exists {
			t.Error("player2 should have been removed from leaderboard")
		}

		if score, exists := server.ramCache.ZScore("scores", "game1"); !exists || score != 50.0 {
			t.Errorf("Expected game1 score 50.0, got %f (exists: %v)", score, exists)
		}
	})

	// Test 6: MSET operations
	t.Run("MsetOperations", func(t *testing.T) {
		// Create MSET entries
		pairs1 := map[string]any{
			"config:db":   "postgres",
			"config:port": "5432",
			"config:host": "localhost",
		}

		pairs2 := map[string]any{
			"session:user1": "abc123",
			"session:user2": "def456",
		}

		msetOps := []entryLog{
			NewMset(pairs1),
			NewMset(pairs2),
		}

		// Write and apply
		for _, op := range msetOps {
			if err := wal.Append(op); err != nil {
				t.Fatalf("Failed to append mset op: %v", err)
			}
			if err := server.updateState(op); err != nil {
				t.Errorf("updateState failed for mset op %s: %v", string(op), err)
			}
		}

		// Validate MSET results
		if val := server.ramCache.GetKey("config:db"); val != "postgres" {
			t.Errorf("Expected config:db='postgres', got %v", val)
		}

		if val := server.ramCache.GetKey("config:port"); val != "5432" {
			t.Errorf("Expected config:port='5432', got %v, %T", val, val)
		}

		if val := server.ramCache.GetKey("session:user1"); val != "abc123" {
			t.Errorf("Expected session:user1='abc123', got %v", val)
		}
	})

	// Test 7: Delete operations
	t.Run("DeleteOperations", func(t *testing.T) {
		// Set up some data first
		setupOps := []entryLog{
			NewSetEntry("temp1", "value1", 0),
			NewSetEntry("temp2", "value2", 0),
			NewHSet("temp_hash", "field1", "value1", 0),
		}

		deleteOps := []entryLog{
			NewDeleteKeyEntry("temp1"),
			NewDeleteKeyEntry("temp2"),
			NewDeleteKeyEntry("temp_hash"),
		}

		// Setup
		for _, op := range setupOps {
			if err := wal.Append(op); err != nil {
				t.Fatalf("Failed to append setup op: %v", err)
			}
			if err := server.updateState(op); err != nil {
				t.Errorf("updateState failed for setup op: %v", err)
			}
		}

		// Verify setup worked
		if !server.ramCache.ExistsKey("temp1") {
			t.Error("temp1 should exist after setup")
		}

		// Apply deletions
		for _, op := range deleteOps {
			if err := wal.Append(op); err != nil {
				t.Fatalf("Failed to append delete op: %v", err)
			}
			if err := server.updateState(op); err != nil {
				t.Errorf("updateState failed for delete op %s: %v", string(op), err)
			}
		}

		// Validate deletions
		if server.ramCache.ExistsKey("temp1") {
			t.Error("temp1 should be deleted")
		}

		if server.ramCache.ExistsKey("temp2") {
			t.Error("temp2 should be deleted")
		}

		if server.ramCache.ExistsKey("temp_hash") {
			t.Error("temp_hash should be deleted")
		}
	})

	t.Logf("WAL integration test completed - all operations validated successfully")
}

func TestWALMalformedEntries(t *testing.T) {
	server := NewServer()

	malformedEntries := []entryLog{
		[]byte("INVALID_COMMAND key value"),
		[]byte("SET"),                       // Missing arguments
		[]byte("SET key"),                   // Missing value and TTL
		[]byte("INCR"),                      // Missing key
		[]byte("HSET key"),                  // Missing field and value
		[]byte(""),                          // Empty entry
		[]byte("SET key value invalid_ttl"), // Invalid TTL
	}

	for i, entry := range malformedEntries {
		err := server.updateState(entry)
		if err == nil {
			t.Errorf("Expected error for malformed entry %d: %s", i, string(entry))
		} else {
			t.Logf("Correctly rejected malformed entry %d: %v", i, err)
		}
	}
}
