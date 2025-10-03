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
