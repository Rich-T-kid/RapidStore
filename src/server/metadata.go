package server

import (
	memorystore "RapidStore/memoryStore"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"gopkg.in/yaml.v3"
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
	Address         string `json:"address" yml:"address"`
	Port            int    `json:"port" yml:"port"`
	HealthCheckPort int    `json:"health_check_port" yml:"health_check_port"`
	MaxClients      uint   `json:"max_clients" yml:"max_clients"`
	timeout         time.Duration
	idleTimeout     time.Duration

	persistence *PersistenceConfig
	monitoring  *MonitoringConfig
	election    *ElectionConfig
}

// TODO: Delete was just needed for testing
func (s *Server) ExposeConfig() map[string]interface{} {
	return map[string]interface{}{
		"pers": *s.config.persistence,
		"mon":  *s.config.monitoring,
		"elec": *s.config.election,
	}
}

type PersistenceConfig struct {
	WALSyncInterval time.Duration `json:"walsyncinterval" yml:"wal_sync_interval"` // how often to sync WAL to disk
	WALPath         string        `json:"walpath" yml:"wal_path"`                  // path to WAL file
	WALMaxSize      uint64        `json:"walmaxsize" yml:"wal_max_size"`           // max size of WAL file before rotation
}

func defaultPersistenceConfig() *PersistenceConfig {
	return &PersistenceConfig{
		WALSyncInterval: 1 * time.Second,
		WALPath:         "./wal.log",
		WALMaxSize:      10 * 1024 * 1024, // 10 MB
	}
}

type MonitoringConfig struct {
	MetricsPort     int           `json:"metricsport" yml:"metrics_port"`
	MetricsPath     string        `json:"metricspath" yml:"metrics_path"`
	MetricsInterval time.Duration `json:"metricsinterval" yml:"metrics_interval"`
	LogFile         string        `json:"logfile" yml:"log_file"`
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
	live             bool
	ZookeeperServers []string      `json:"zookeeperservers" yml:"zookeeper_servers"` // connection endpoints
	ElectionPath     string        `json:"electionpath" yml:"election_path"`         // e.g. "/service/leader"
	NodeID           string        `json:"nodeid" yml:"node_id"`                     // unique identifier for this node
	Timeout          time.Duration `json:"timeout" yml:"timeout"`                    //heartbeat timeout try to keep relatively low so we know quickly if a node is down
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

func ServerFromConfig(fileName string, r io.Reader) (*Server, error) {
	parts := strings.Split(filepath.Base(fileName), ".")
	ext := strings.ToLower(parts[len(parts)-1])

	// First, initialize with defaults
	var config = defeaultServerConfig()
	var err error

	switch ext {
	case "json":
		*config, err = serverConfigFromJson(r, config)
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON config: %w", err)
		}
	case "yml", "yaml":
		*config, err = serverConfigFromYml(r, config)
		if err != nil {
			return nil, fmt.Errorf("failed to parse YAML config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported config file format: %s", ext)
	}
	s := &Server{
		config:          config,
		productionStats: ServerInfoMetaData{},
		ramCache:        memorystore.NewCache(),
		close:           make(chan struct{}),
	}
	return s, nil
}
func serverConfigFromJson(r io.Reader, c *ServerConfig) (ServerConfig, error) {

	// Define the outer JSON structure
	var outerConfig struct {
		ServerConfig      map[string]interface{} `json:"ServerConfig"`
		PersistenceConfig map[string]interface{} `json:"PersistenceConfig"` // Note: keeping your spelling
		MonitoringConfig  map[string]interface{} `json:"MonitoringConfig"`  // Note: keeping your spelling
		ElectionConfig    map[string]interface{} `json:"ElectionConfig"`
	}

	fmt.Printf("Peristance %+#v\n", outerConfig.PersistenceConfig)
	// Parse the JSON
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&outerConfig); err != nil {
		return ServerConfig{}, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Process serverConfig section if present
	if outerConfig.ServerConfig != nil {
		if address, ok := outerConfig.ServerConfig["address"].(string); ok {
			c.Address = address
		}
		if port, ok := outerConfig.ServerConfig["port"].(float64); ok {
			c.Port = int(port)
		}
		if healthPort, ok := outerConfig.ServerConfig["health_check_port"].(float64); ok {
			c.HealthCheckPort = int(healthPort)
		}
		if maxClients, ok := outerConfig.ServerConfig["max_clients"].(float64); ok {
			c.MaxClients = uint(maxClients)
		}
		if timeout, ok := outerConfig.ServerConfig["timeout"].(string); ok {
			if duration, err := time.ParseDuration(timeout); err == nil {
				c.timeout = duration
			}
		}
		if idleTimeout, ok := outerConfig.ServerConfig["idle_timeout"].(string); ok {
			if duration, err := time.ParseDuration(idleTimeout); err == nil {
				c.idleTimeout = duration
			}
		}
	}

	// ==================== PERSISTENCE CONFIG SECTION ====================
	if outerConfig.PersistenceConfig != nil {
		if walSyncInterval, ok := outerConfig.PersistenceConfig["wal_sync_interval"].(string); ok {
			if duration, err := time.ParseDuration(walSyncInterval); err == nil {
				c.persistence.WALSyncInterval = duration
			}
		}
		if walPath, ok := outerConfig.PersistenceConfig["wal_path"].(string); ok {
			c.persistence.WALPath = walPath
		}
		if walMaxSize, ok := outerConfig.PersistenceConfig["wal_max_size"].(float64); ok {
			c.persistence.WALMaxSize = uint64(walMaxSize)
		}
	}

	// ==================== MONITORING CONFIG SECTION ====================
	if outerConfig.MonitoringConfig != nil {
		if metricsPort, ok := outerConfig.MonitoringConfig["metrics_port"].(float64); ok {
			c.monitoring.MetricsPort = int(metricsPort)
		}
		if metricsPath, ok := outerConfig.MonitoringConfig["metrics_path"].(string); ok {
			c.monitoring.MetricsPath = metricsPath
		}
		if metricsInterval, ok := outerConfig.MonitoringConfig["metrics_interval"].(string); ok {
			if duration, err := time.ParseDuration(metricsInterval); err == nil {
				c.monitoring.MetricsInterval = duration
			}
		}
		if logFile, ok := outerConfig.MonitoringConfig["log_file"].(string); ok {
			c.monitoring.LogFile = logFile
		}
	}

	// ==================== ELECTION CONFIG SECTION ====================
	if outerConfig.ElectionConfig != nil {
		if zkServers, ok := outerConfig.ElectionConfig["zookeeper_servers"].([]interface{}); ok {
			servers := make([]string, len(zkServers))
			for i, server := range zkServers {
				if serverStr, ok := server.(string); ok {
					servers[i] = serverStr
				}
			}
			c.election.ZookeeperServers = servers
		}
		if electionPath, ok := outerConfig.ElectionConfig["election_path"].(string); ok {
			c.election.ElectionPath = electionPath
		}
		if nodeID, ok := outerConfig.ElectionConfig["node_id"].(string); ok {
			c.election.NodeID = nodeID
		}
		if timeout, ok := outerConfig.ElectionConfig["timeout"].(string); ok {
			if duration, err := time.ParseDuration(timeout); err == nil {
				c.election.Timeout = duration
			}
		}
	}

	return *c, nil
}

func serverConfigFromYml(r io.Reader, c *ServerConfig) (ServerConfig, error) {

	// Define the outer YAML structure (same as JSON but using yaml tags)
	var outerConfig struct {
		ServerConfig      map[string]interface{} `yaml:"ServerConfig"`
		PersistenceConfig map[string]interface{} `yaml:"PersistenceConfig"` // Note: keeping your spelling
		MonitoringConfig  map[string]interface{} `yaml:"MonitoringConfig"`  // Note: keeping your spelling
		ElectionConfig    map[string]interface{} `yaml:"ElectionConfig"`
	}

	// Parse the YAML
	decoder := yaml.NewDecoder(r)
	if err := decoder.Decode(&outerConfig); err != nil {
		return ServerConfig{}, fmt.Errorf("failed to parse YAML: %w", err)
	}

	// ==================== SERVER CONFIG SECTION ====================
	if outerConfig.ServerConfig != nil {
		if address, ok := outerConfig.ServerConfig["address"].(string); ok {
			c.Address = address
		}
		if port, ok := outerConfig.ServerConfig["port"].(int); ok {
			c.Port = port
		}
		if healthPort, ok := outerConfig.ServerConfig["health_check_port"].(int); ok {
			c.HealthCheckPort = healthPort
		}
		if maxClients, ok := outerConfig.ServerConfig["max_clients"].(int); ok {
			c.MaxClients = uint(maxClients)
		}
		if timeout, ok := outerConfig.ServerConfig["timeout"].(string); ok {
			if duration, err := time.ParseDuration(timeout); err == nil {
				c.timeout = duration
			}
		}
		if idleTimeout, ok := outerConfig.ServerConfig["idle_timeout"].(string); ok {
			if duration, err := time.ParseDuration(idleTimeout); err == nil {
				c.idleTimeout = duration
			}
		}
	}

	// ==================== PERSISTENCE CONFIG SECTION ====================
	if outerConfig.PersistenceConfig != nil {
		if walSyncInterval, ok := outerConfig.PersistenceConfig["wal_sync_interval"].(string); ok {
			if duration, err := time.ParseDuration(walSyncInterval); err == nil {
				c.persistence.WALSyncInterval = duration
			}
		}
		if walPath, ok := outerConfig.PersistenceConfig["wal_path"].(string); ok {
			c.persistence.WALPath = walPath
		}
		if walMaxSize, ok := outerConfig.PersistenceConfig["wal_max_size"].(int); ok {
			c.persistence.WALMaxSize = uint64(walMaxSize)
		}
	}

	// ==================== MONITORING CONFIG SECTION ====================
	if outerConfig.MonitoringConfig != nil {
		if metricsPort, ok := outerConfig.MonitoringConfig["metrics_port"].(int); ok {
			c.monitoring.MetricsPort = metricsPort
		}
		if metricsPath, ok := outerConfig.MonitoringConfig["metrics_path"].(string); ok {
			c.monitoring.MetricsPath = metricsPath
		}
		if metricsInterval, ok := outerConfig.MonitoringConfig["metrics_interval"].(string); ok {
			if duration, err := time.ParseDuration(metricsInterval); err == nil {
				c.monitoring.MetricsInterval = duration
			}
		}
		if logFile, ok := outerConfig.MonitoringConfig["log_file"].(string); ok {
			c.monitoring.LogFile = logFile
		}
	}

	// ==================== ELECTION CONFIG SECTION ====================
	if outerConfig.ElectionConfig != nil {
		if zkServers, ok := outerConfig.ElectionConfig["zookeeper_servers"].([]interface{}); ok {
			servers := make([]string, len(zkServers))
			for i, server := range zkServers {
				if serverStr, ok := server.(string); ok {
					servers[i] = serverStr
				}
			}
			c.election.ZookeeperServers = servers
		}
		if electionPath, ok := outerConfig.ElectionConfig["election_path"].(string); ok {
			c.election.ElectionPath = electionPath
		}
		if nodeID, ok := outerConfig.ElectionConfig["node_id"].(string); ok {
			c.election.NodeID = nodeID
		}
		if timeout, ok := outerConfig.ElectionConfig["timeout"].(string); ok {
			if duration, err := time.ParseDuration(timeout); err == nil {
				c.election.Timeout = duration
			}
		}
	}

	return *c, nil
}
