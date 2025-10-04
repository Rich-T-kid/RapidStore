package server

import (
	"runtime"
	"time"

	"github.com/shirou/gopsutil/cpu"
)

// Metadata holds server metadata information.
type ServerInfo struct {
	Timestamp         time.Time `json:"timestamp"`          // when info was collected
	TotalKeys         uint64    `json:"total_keys"`         // number of keys stored
	MemoryUsageBytes  uint64    `json:"memory_usage_bytes"` // memory consumed by the store
	WriteOps          uint64    `json:"write_ops"`          // total write operations
	ReadOps           uint64    `json:"read_ops"`           // total read operations
	ActiveConnections uint64    `json:"active_connections"` // currently open client connections
	TotalRequests     uint64    `json:"total_requests"`     // all requests processed
	LastCommand       string    `json:"last_command"`       // last executed command
	IndepthStats      GcStats   `json:"indepth_stats"`      // detailed stats about the Go runtime
}
type GcStats struct {
	UptimeSeconds  uint64  `json:"uptime_seconds"` // how long server has been up
	NumGC          uint32  // number of garbage collections
	CPULoadPercent float64 `json:"cpu_load_percent"` // CPU load percentage
	PauseTotalNs   uint64  // total pause time in nanoseconds
	HeapAlloc      uint64  // bytes allocated and still in use
	HeapSys        uint64  // bytes obtained from system
	HeapIdle       uint64  // bytes in idle spans
	HeapInuse      uint64  // bytes in non-idle spans
	HeapReleased   uint64  // bytes released to the OS
	GCCPUFraction  float64 // fraction of CPU time used by GC
	Goroutines     int     // number of goroutines
}

var startTime = time.Now()

func CollectGcStats() (*GcStats, error) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	// CPU usage (over ~200ms sample)
	percent, err := cpu.Percent(200*time.Millisecond, false)
	if err != nil {
		return nil, err
	}

	stats := &GcStats{
		UptimeSeconds:  uint64(time.Since(startTime).Seconds()),
		NumGC:          mem.NumGC,
		CPULoadPercent: percent[0],
		PauseTotalNs:   mem.PauseTotalNs,
		HeapAlloc:      mem.HeapAlloc,
		HeapSys:        mem.HeapSys,
		HeapIdle:       mem.HeapIdle,
		HeapInuse:      mem.HeapInuse,
		HeapReleased:   mem.HeapReleased,
		GCCPUFraction:  mem.GCCPUFraction,
		Goroutines:     runtime.NumGoroutine(),
	}

	return stats, nil
}

type CacheUtility struct {
	infoChan    chan ServerInfo
	commandChan chan string
	clearDB     chan bool
}

func NewCacheUtility() *CacheUtility {
	return &CacheUtility{
		infoChan:    make(chan ServerInfo),
		commandChan: make(chan string),
		clearDB:     make(chan bool),
	}
}
func (c *CacheUtility) Info() <-chan ServerInfo {
	return c.infoChan
}
func (c *CacheUtility) Monitor() <-chan string {
	return c.commandChan
}
func (c *CacheUtility) FlushDB() <-chan bool {
	return c.clearDB
}
func NewUtilityManger() UtilityManager {
	return NewCacheUtility()
}

/* ---------------------- Implements the UtilityManager Interface --------------------- */
type UtilityManager interface {
	// mabey change this to a stream of a struct
	Info() <-chan ServerInfo // server metadata (uptime,timestamp,total keys, memory usage, Write/Read ops, size of Write ahead log, active connections, total request, cpu load, last command)
	Monitor() <-chan string  // chan of all the commands processed by the server (commands and their args)
	FlushDB() <-chan bool    // clear current db
}

type ServerConfig struct {
	Address         string
	Port            int
	HealthCheckPort int
	MaxClients      uint
	timeout         time.Duration
	idleTimeout     time.Duration

	persistence *PersistenceConfig
	monitoring  *MonitoringConfig
	election    *ElectionConfig
}
type PersistenceConfig struct {
	WALSyncInterval time.Duration // how often to sync WAL to disk
	WALPath         string        // path to WAL file
	WALMaxSize      uint64        // max WAL file size before rotation
}

func defaultPersistenceConfig() *PersistenceConfig {
	return &PersistenceConfig{
		WALSyncInterval: 1 * time.Second,
		WALPath:         "./wal.log",
		WALMaxSize:      10 * 1024 * 1024, // 10 MB
	}
}

type MonitoringConfig struct {
	MetricsPort int
	MetricsPath string

	LogFile string

	HealthCheckEnabled bool
	HealthCheckPort    int
}

func defaultMonitoringConfig() *MonitoringConfig {
	return &MonitoringConfig{
		MetricsPort:        9090,
		MetricsPath:        "/metrics",
		LogFile:            "./rapidstore.log",
		HealthCheckEnabled: true,
		HealthCheckPort:    8080,
	}
}

type ElectionConfig struct {
	live             bool     // since we havnt incorporated elections yet this will just be a placeholder for now
	ZookeeperServers []string // connection endpoints
	ElectionPath     string   // e.g. "/service/leader"
	NodeID           string   // unique identifier for this node
	Timeout          time.Duration
}

func defaultElectionConfig() *ElectionConfig {
	return &ElectionConfig{
		live:             false,
		ZookeeperServers: []string{"localhost:2181"},
		ElectionPath:     "/rapidstore/leader",
		NodeID:           "",
		Timeout:          5 * time.Second,
	}
}
