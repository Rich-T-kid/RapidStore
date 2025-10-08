package server

import (
	"os"
	"testing"
	"time"
)

func TestServerConfigPropagation(t *testing.T) {
	// Test all WithOption functions to ensure configuration propagation works correctly
	server := NewServer(
		// Basic server configuration
		WithAddress("192.168.1.100"),
		WithPort(9999),
		WithMaxClients(5000),
		WithTimeout(45*time.Second),
		WithIdleTimeout(10*time.Minute),

		// Persistence configuration - individual fields
		WithWALSyncInterval(2*time.Second),
		WithWALPath("/custom/wal/path.log"),
		WithWALMaxSize(200*1024*1024), // 200MB

		// Monitoring configuration - individual fields
		WithMetricsPort(8090),
		WithMetricsPath("/custom-metrics"),
		WithLogFile("/var/log/custom-rapidstore.log"),
	)

	config := server.config

	// Test basic server configuration
	t.Run("BasicServerConfig", func(t *testing.T) {
		if config.Address != "192.168.1.100" {
			t.Errorf("Expected Address to be '192.168.1.100', got '%s'", config.Address)
		}

		if config.Port != 9999 {
			t.Errorf("Expected Port to be 9999, got %d", config.Port)
		}

		if config.MaxClients != 5000 {
			t.Errorf("Expected MaxClients to be 5000, got %d", config.MaxClients)
		}

		if config.timeout != 45*time.Second {
			t.Errorf("Expected timeout to be 45s, got %v", config.timeout)
		}

		if config.idleTimeout != 10*time.Minute {
			t.Errorf("Expected idleTimeout to be 10m, got %v", config.idleTimeout)
		}
	})

	// Test persistence configuration
	t.Run("PersistenceConfig", func(t *testing.T) {
		if config.persistence == nil {
			t.Fatal("Expected persistence config to be initialized, got nil")
		}

		if config.persistence.WALSyncInterval != 2*time.Second {
			t.Errorf("Expected WALSyncInterval to be 2s, got %v", config.persistence.WALSyncInterval)
		}

		if config.persistence.WALPath != "/custom/wal/path.log" {
			t.Errorf("Expected WALPath to be '/custom/wal/path.log', got '%s'", config.persistence.WALPath)
		}

		expectedWALMaxSize := uint64(200 * 1024 * 1024) // 200MB
		if config.persistence.WALMaxSize != expectedWALMaxSize {
			t.Errorf("Expected WALMaxSize to be %d, got %d", expectedWALMaxSize, config.persistence.WALMaxSize)
		}
	})

	// Test monitoring configuration
	t.Run("MonitoringConfig", func(t *testing.T) {
		if config.monitoring == nil {
			t.Fatal("Expected monitoring config to be initialized, got nil")
		}

		if config.monitoring.MetricsPort != 8090 {
			t.Errorf("Expected MetricsPort to be 8090, got %d", config.monitoring.MetricsPort)
		}

		if config.monitoring.MetricsPath != "/custom-metrics" {
			t.Errorf("Expected MetricsPath to be '/custom-metrics', got '%s'", config.monitoring.MetricsPath)
		}

		if config.monitoring.LogFile != "/var/log/custom-rapidstore.log" {
			t.Errorf("Expected LogFile to be '/var/log/custom-rapidstore.log', got '%s'", config.monitoring.LogFile)
		}

	})

}

func TestServerConfigWithCompleteStructs(t *testing.T) {
	// Test using complete config structs instead of individual field setters
	customPersistence := &PersistenceConfig{
		WALSyncInterval: 500 * time.Millisecond,
		WALPath:         "/struct/test/wal.log",
		WALMaxSize:      50 * 1024 * 1024, // 50MB
	}

	customMonitoring := &MonitoringConfig{
		MetricsPort: 7070,
		MetricsPath: "/struct-metrics",
		LogFile:     "/struct/test/log.log",
	}

	customElection := &ElectionConfig{
		ZookeeperServers: []string{"136.112.251.137:2181"},
		ElectionPath:     "/struct/test/leader",
		NodeID:           "struct-test-node",
		Timeout:          20 * time.Second,
	}

	server := NewServer(
		WithPort(7777),
		WithPersistence(customPersistence),
		WithMonitoring(customMonitoring),
		WithElection(customElection),
	)

	config := server.config

	// Verify complete struct propagation
	t.Run("CompletePersistenceStruct", func(t *testing.T) {
		if config.persistence != customPersistence {
			t.Error("Expected persistence config to be the exact same struct instance")
		}

		if config.persistence.WALSyncInterval != 500*time.Millisecond {
			t.Errorf("Expected WALSyncInterval to be 500ms, got %v", config.persistence.WALSyncInterval)
		}
	})

	t.Run("CompleteMonitoringStruct", func(t *testing.T) {
		if config.monitoring != customMonitoring {
			t.Error("Expected monitoring config to be the exact same struct instance")
		}

		if config.monitoring.MetricsPort != 7070 {
			t.Errorf("Expected MetricsPort to be 7070, got %d", config.monitoring.MetricsPort)
		}
	})

}

func TestServerConfigDefaults(t *testing.T) {
	// Test that a server created with no options has proper defaults
	server := NewServer()
	config := server.config

	t.Run("DefaultValues", func(t *testing.T) {
		// Check some key default values
		if config.Address != "0.0.0.0" {
			t.Errorf("Expected default Address to be '0.0.0.0', got '%s'", config.Address)
		}

		if config.Port != 6380 {
			t.Errorf("Expected default Port to be 6380, got %d", config.Port)
		}

		if config.MaxClients != 1000 {
			t.Errorf("Expected default MaxClients to be 1000, got %d", config.MaxClients)
		}

		// Verify nested configs are initialized with defaults
		if config.persistence == nil {
			t.Error("Expected default persistence config to be initialized")
		}

		if config.monitoring == nil {
			t.Error("Expected default monitoring config to be initialized")
		}

	})
}

// ==================== CONFIG FILE PARSING TESTS ====================

func TestServerFromConfig_JSON_TestConfig(t *testing.T) {
	// Test parsing test_config.json
	file, err := os.Open("test_data/test_config.json")
	if err != nil {
		t.Fatalf("Failed to open test config file: %v", err)
	}
	defer file.Close()

	server, err := ServerFromConfig("test_config.json", file)
	if err != nil {
		t.Fatalf("Failed to create server from JSON config: %v", err)
	}

	// Verify server config values
	if server.config.Address != "127.0.0.1" {
		t.Errorf("Expected address '127.0.0.1', got '%s'", server.config.Address)
	}
	if server.config.Port != 7000 {
		t.Errorf("Expected port 7000, got %d", server.config.Port)
	}
	if server.config.HealthCheckPort != 9000 {
		t.Errorf("Expected health check port 9000, got %d", server.config.HealthCheckPort)
	}
	if server.config.MaxClients != 500 {
		t.Errorf("Expected max clients 500, got %d", server.config.MaxClients)
	}
	if server.config.timeout != 45*time.Second {
		t.Errorf("Expected timeout 45s, got %v", server.config.timeout)
	}
	if server.config.idleTimeout != 600*time.Second {
		t.Errorf("Expected idle timeout 600s, got %v", server.config.idleTimeout)
	}

	// Verify persistence config
	if server.config.persistence.WALSyncInterval != 2*time.Second {
		t.Errorf("Expected WAL sync interval 2s, got %v", server.config.persistence.WALSyncInterval)
	}
	if server.config.persistence.WALPath != "./test_wal.log" {
		t.Errorf("Expected WAL path './test_wal.log', got '%s'", server.config.persistence.WALPath)
	}
	if server.config.persistence.WALMaxSize != 5242880 {
		t.Errorf("Expected WAL max size 5242880, got %d", server.config.persistence.WALMaxSize)
	}

	// Verify monitoring config
	if server.config.monitoring.MetricsPort != 8090 {
		t.Errorf("Expected metrics port 8090, got %d", server.config.monitoring.MetricsPort)
	}
	if server.config.monitoring.MetricsPath != "/test/metrics" {
		t.Errorf("Expected metrics path '/test/metrics', got '%s'", server.config.monitoring.MetricsPath)
	}
	if server.config.monitoring.MetricsInterval != 500*time.Millisecond {
		t.Errorf("Expected metrics interval 500ms, got %v", server.config.monitoring.MetricsInterval)
	}
	if server.config.monitoring.LogFile != "./test.log" {
		t.Errorf("Expected log file './test.log', got '%s'", server.config.monitoring.LogFile)
	}

}

func TestServerFromConfig_JSON_ProdConfig(t *testing.T) {
	// Test parsing prod_config.json
	file, err := os.Open("test_data/prod_config.json")
	if err != nil {
		t.Fatalf("Failed to open prod config file: %v", err)
	}
	defer file.Close()

	server, err := ServerFromConfig("prod_config.json", file)
	if err != nil {
		t.Fatalf("Failed to create server from JSON config: %v", err)
	}

	// Verify key server config values
	if server.config.Address != "0.0.0.0" {
		t.Errorf("Expected address '0.0.0.0', got '%s'", server.config.Address)
	}
	if server.config.Port != 8000 {
		t.Errorf("Expected port 8000, got %d", server.config.Port)
	}
	if server.config.MaxClients != 2000 {
		t.Errorf("Expected max clients 2000, got %d", server.config.MaxClients)
	}
	if server.config.timeout != 60*time.Second {
		t.Errorf("Expected timeout 60s, got %v", server.config.timeout)
	}

	// Verify key persistence config values
	if server.config.persistence.WALSyncInterval != 3*time.Second {
		t.Errorf("Expected WAL sync interval 3s, got %v", server.config.persistence.WALSyncInterval)
	}
	if server.config.persistence.WALPath != "./prod_wal.log" {
		t.Errorf("Expected WAL path './prod_wal.log', got '%s'", server.config.persistence.WALPath)
	}

}

func TestServerFromConfig_YAML_TestConfig(t *testing.T) {
	// Test parsing test_config.yaml
	file, err := os.Open("test_data/test_config.yaml")
	if err != nil {
		t.Fatalf("Failed to open test YAML config file: %v", err)
	}
	defer file.Close()

	server, err := ServerFromConfig("test_config.yaml", file)
	if err != nil {
		t.Fatalf("Failed to create server from YAML config: %v", err)
	}

	// Verify server config values
	if server.config.Address != "192.168.1.100" {
		t.Errorf("Expected address '192.168.1.100', got '%s'", server.config.Address)
	}
	if server.config.Port != 6500 {
		t.Errorf("Expected port 6500, got %d", server.config.Port)
	}
	if server.config.HealthCheckPort != 8500 {
		t.Errorf("Expected health check port 8500, got %d", server.config.HealthCheckPort)
	}
	if server.config.MaxClients != 750 {
		t.Errorf("Expected max clients 750, got %d", server.config.MaxClients)
	}
	if server.config.timeout != 35*time.Second {
		t.Errorf("Expected timeout 35s, got %v", server.config.timeout)
	}

	// Verify persistence config
	if server.config.persistence.WALSyncInterval != 1500*time.Millisecond {
		t.Errorf("Expected WAL sync interval 1.5s, got %v", server.config.persistence.WALSyncInterval)
	}
	if server.config.persistence.WALPath != "./dev_wal.log" {
		t.Errorf("Expected WAL path './dev_wal.log', got '%s'", server.config.persistence.WALPath)
	}

	// Verify monitoring config
	if server.config.monitoring.MetricsPort != 9292 {
		t.Errorf("Expected metrics port 9292, got %d", server.config.monitoring.MetricsPort)
	}
	if server.config.monitoring.MetricsInterval != 250*time.Millisecond {
		t.Errorf("Expected metrics interval 250ms, got %v", server.config.monitoring.MetricsInterval)
	}

}

func TestServerFromConfig_YAML_StagingConfig(t *testing.T) {
	// Test parsing staging_config.yml
	file, err := os.Open("test_data/staging_config.yml")
	if err != nil {
		t.Fatalf("Failed to open staging YAML config file: %v", err)
	}
	defer file.Close()

	server, err := ServerFromConfig("staging_config.yml", file)
	if err != nil {
		t.Fatalf("Failed to create server from YAML config: %v", err)
	}

	// Verify key server config values
	if server.config.Address != "10.0.0.1" {
		t.Errorf("Expected address '10.0.0.1', got '%s'", server.config.Address)
	}
	if server.config.Port != 6379 {
		t.Errorf("Expected port 6379, got %d", server.config.Port)
	}
	if server.config.MaxClients != 1500 {
		t.Errorf("Expected max clients 1500, got %d", server.config.MaxClients)
	}
	if server.config.timeout != 50*time.Second {
		t.Errorf("Expected timeout 50s, got %v", server.config.timeout)
	}

	// Verify persistence config
	if server.config.persistence.WALSyncInterval != 4*time.Second {
		t.Errorf("Expected WAL sync interval 4s, got %v", server.config.persistence.WALSyncInterval)
	}
	if server.config.persistence.WALMaxSize != 15728640 {
		t.Errorf("Expected WAL max size 15728640, got %d", server.config.persistence.WALMaxSize)
	}

	// Verify monitoring config
	if server.config.monitoring.MetricsInterval != 2*time.Second {
		t.Errorf("Expected metrics interval 2s, got %v", server.config.monitoring.MetricsInterval)
	}
	if server.config.monitoring.LogFile != "./staging.log" {
		t.Errorf("Expected log file './staging.log', got '%s'", server.config.monitoring.LogFile)
	}

	// Verify election config with 4 servers
}
