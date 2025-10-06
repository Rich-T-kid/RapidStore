package server

import (
	"testing"
	"time"
)

func TestServerLive(t *testing.T) {
	s := NewServer()
	go func() {
		s.Start()
	}()
	time.Sleep(50 * time.Millisecond)
	if !s.isLive {
		t.Errorf("expected server to be live after starting, but it is not")
	}
	s.Stop()
	time.Sleep(50 * time.Millisecond)
	if s.isLive {
		t.Errorf("expected server to be not live after stopping, but it is still live")
	}
}

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

		// Election configuration - individual fields
		WithElectionEnabled(true),
		WithZookeeperServers([]string{"zk1:2181", "zk2:2181", "zk3:2181"}),
		WithElectionPath("/custom/rapidstore/leader"),
		WithNodeID("test-node-123"),
		WithElectionTimeout(15*time.Second),
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

	// Test election configuration
	t.Run("ElectionConfig", func(t *testing.T) {
		if config.election == nil {
			t.Fatal("Expected election config to be initialized, got nil")
		}

		if config.election.live != true {
			t.Errorf("Expected election live to be true, got %t", config.election.live)
		}

		expectedServers := []string{"zk1:2181", "zk2:2181", "zk3:2181"}
		if len(config.election.ZookeeperServers) != len(expectedServers) {
			t.Errorf("Expected %d zookeeper servers, got %d", len(expectedServers), len(config.election.ZookeeperServers))
		}

		for i, expected := range expectedServers {
			if i >= len(config.election.ZookeeperServers) || config.election.ZookeeperServers[i] != expected {
				t.Errorf("Expected ZookeeperServers[%d] to be '%s', got '%s'", i, expected, config.election.ZookeeperServers[i])
			}
		}

		if config.election.ElectionPath != "/custom/rapidstore/leader" {
			t.Errorf("Expected ElectionPath to be '/custom/rapidstore/leader', got '%s'", config.election.ElectionPath)
		}

		if config.election.NodeID != "test-node-123" {
			t.Errorf("Expected NodeID to be 'test-node-123', got '%s'", config.election.NodeID)
		}

		if config.election.Timeout != 15*time.Second {
			t.Errorf("Expected election Timeout to be 15s, got %v", config.election.Timeout)
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
		live:             false,
		ZookeeperServers: []string{"struct-zk:2181"},
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

	t.Run("CompleteElectionStruct", func(t *testing.T) {
		if config.election != customElection {
			t.Error("Expected election config to be the exact same struct instance")
		}

		if config.election.live != false {
			t.Errorf("Expected election live to be false, got %t", config.election.live)
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

		if config.election == nil {
			t.Error("Expected default election config to be initialized")
		}
	})
}
