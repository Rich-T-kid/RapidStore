package server

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/cpu"
)

// Metadata holds server metadata information.
type ServerInfoMetaData struct {
	WriteOps          uint64  `json:"write_ops"`          // total write operations
	ReadOps           uint64  `json:"read_ops"`           // total read operations
	ActiveConnections uint64  `json:"active_connections"` // currently open client connections
	TotalRequests     uint64  `json:"total_requests"`     // all requests processed
	IndepthStats      GcStats `json:"indepth_stats"`      // detailed stats about the Go runtime
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

func (smd *ServerInfoMetaData) IncrementReadOps() {
	atomic.AddUint64(&smd.ReadOps, 1)
}
func (smd *ServerInfoMetaData) IncrementWriteOps() {
	atomic.AddUint64(&smd.WriteOps, 1)
}
func (smd *ServerInfoMetaData) IncrementTotalRequests() {
	atomic.AddUint64(&smd.TotalRequests, 1)
}
func (smd *ServerInfoMetaData) IncrementActiveConnections() {
	atomic.AddUint64(&smd.ActiveConnections, 1)
}
func (smd *ServerInfoMetaData) DecrementActiveConnections() {
	atomic.AddUint64(&smd.ActiveConnections, ^uint64(0))
}

var startTime = time.Now()
var gcStatsCache atomic.Value

// cachedGcStats holds both the stats and the timestamp when they were collected
type cachedGcStats struct {
	stats     *GcStats
	timestamp time.Time
}

func (m *MonitoringConfig) collectGcStats() (*GcStats, error) {
	var cacheDuration = m.MetricsInterval

	// Check if we have cached data that's still valid
	if cached := gcStatsCache.Load(); cached != nil {
		if cachedData, ok := cached.(*cachedGcStats); ok {
			if time.Since(cachedData.timestamp) < cacheDuration {
				// Return cached data if it's less than 250ms old
				return cachedData.stats, nil
			}
		}
	}

	// Cache miss or expired - collect new stats
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

	// Store in cache with current timestamp
	gcStatsCache.Store(&cachedGcStats{
		stats:     stats,
		timestamp: time.Now(),
	})

	return stats, nil
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
	MetricsPort     int
	MetricsPath     string
	MetricsInterval time.Duration
	LogFile         string
}

func defaultMonitoringConfig() *MonitoringConfig {
	return &MonitoringConfig{
		MetricsPort:     9090,
		MetricsPath:     "/metrics",
		MetricsInterval: 750 * time.Millisecond,
		LogFile:         "./rapidstore.log",
	}
}

type ElectionConfig struct {
	live             bool          // since we havnt incorporated elections yet this will just be a placeholder for now
	ZookeeperServers []string      // connection endpoints
	ElectionPath     string        // e.g. "/service/leader"
	NodeID           string        // unique identifier for this node
	Timeout          time.Duration //heartbeat timeout try to keep relatively low so we know quickly if a node is down
}

func defaultElectionConfig() *ElectionConfig {
	return &ElectionConfig{
		live:             false,
		ZookeeperServers: []string{"localhost:2181"},
		ElectionPath:     "/rapidstore/leader",
		NodeID:           "(TDB) remove and read from zookeeper", // TODO:
		Timeout:          5 * time.Second,
	}
}
