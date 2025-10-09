package internal

type EvictionPolicy int

const (
	NoEviction     EvictionPolicy = iota // New values aren't saved when memory limit is reached When a database uses replication, this applies to the primary database
	LRU                                  // Least Recently Used | Keeps most recently used keys; removes least recently used (LRU) keys
	LFU                                  // Least Frequently Used | Keeps frequently used keys; removes least frequently used (LFU) keys
	RANDOM                               // Random | Removes random keys
	VolatileLRU                          // Volatile Least Recently Used | Removes least recently used keys with expire field set to true
	VolatileLFU                          // Volatile Least Frequently Used | Removes least frequently used keys with expire field set to true
	VolatileRandom                       // Volatile Random | Removes random keys with expire field set to true
	VolatileTTL                          // Volatile Time-To-Live | Removes least frequently used keys with expire field set to true and the shortest remaining time-to-live (TTL) value
)
