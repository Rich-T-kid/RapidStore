package server

import (
	memorystore "RapidStore/memoryStore"
	"errors"
	"fmt"
	"net"
	"time"
)

type serverOption = func(s *ServerConfig)

func NewServerConfig(...ServerConfig) *ServerConfig {
	return &ServerConfig{}
}

// Functional option constructors
func WithAddress(addr string) serverOption {
	return func(s *ServerConfig) {
		s.Address = addr
	}
}

func WithPort(port int) serverOption {
	return func(s *ServerConfig) {
		s.Port = port
	}
}

func WithMaxClients(max uint) serverOption {
	return func(s *ServerConfig) {
		s.MaxClients = max
	}
}

func WithTimeout(d time.Duration) serverOption {
	return func(s *ServerConfig) {
		s.timeout = d
	}
}

func WithIdleTimeout(d time.Duration) serverOption {
	return func(s *ServerConfig) {
		s.idleTimeout = d
	}
}

// Persistence configuration options
func WithPersistence(config *PersistenceConfig) serverOption {
	return func(s *ServerConfig) {
		s.persistence = config
	}
}

func WithWALSyncInterval(interval time.Duration) serverOption {
	return func(s *ServerConfig) {
		if s.persistence == nil {
			s.persistence = defaultPersistenceConfig()
		}
		s.persistence.WALSyncInterval = interval
	}
}

func WithWALPath(path string) serverOption {
	return func(s *ServerConfig) {
		if s.persistence == nil {
			s.persistence = defaultPersistenceConfig()
		}
		s.persistence.WALPath = path
	}
}

func WithWALMaxSize(size uint64) serverOption {
	return func(s *ServerConfig) {
		if s.persistence == nil {
			s.persistence = defaultPersistenceConfig()
		}
		s.persistence.WALMaxSize = size
	}
}

// Monitoring configuration options
func WithMonitoring(config *MonitoringConfig) serverOption {
	return func(s *ServerConfig) {
		s.monitoring = config
	}
}

func WithMetricsPort(port int) serverOption {
	return func(s *ServerConfig) {
		if s.monitoring == nil {
			s.monitoring = defaultMonitoringConfig()
		}
		s.monitoring.MetricsPort = port
	}
}

func WithMetricsPath(path string) serverOption {
	return func(s *ServerConfig) {
		if s.monitoring == nil {
			s.monitoring = defaultMonitoringConfig()
		}
		s.monitoring.MetricsPath = path
	}
}

func WithLogFile(logFile string) serverOption {
	return func(s *ServerConfig) {
		if s.monitoring == nil {
			s.monitoring = defaultMonitoringConfig()
		}
		s.monitoring.LogFile = logFile
	}
}

func WithHealthCheck(enabled bool, port int) serverOption {
	return func(s *ServerConfig) {
		if s.monitoring == nil {
			s.monitoring = defaultMonitoringConfig()
		}
		s.monitoring.HealthCheckEnabled = enabled
		s.monitoring.HealthCheckPort = port
	}
}

func WithHealthCheckEnabled(enabled bool) serverOption {
	return func(s *ServerConfig) {
		if s.monitoring == nil {
			s.monitoring = defaultMonitoringConfig()
		}
		s.monitoring.HealthCheckEnabled = enabled
	}
}

func WithHealthCheckPort(port int) serverOption {
	return func(s *ServerConfig) {
		if s.monitoring == nil {
			s.monitoring = defaultMonitoringConfig()
		}
		s.monitoring.HealthCheckPort = port
	}
}

// Election configuration options
func WithElection(config *ElectionConfig) serverOption {
	return func(s *ServerConfig) {
		s.election = config
	}
}

func WithElectionEnabled(enabled bool) serverOption {
	return func(s *ServerConfig) {
		if s.election == nil {
			s.election = defaultElectionConfig()
		}
		s.election.live = enabled
	}
}

func WithZookeeperServers(servers []string) serverOption {
	return func(s *ServerConfig) {
		if s.election == nil {
			s.election = defaultElectionConfig()
		}
		s.election.ZookeeperServers = servers
	}
}

func WithElectionPath(path string) serverOption {
	return func(s *ServerConfig) {
		if s.election == nil {
			s.election = defaultElectionConfig()
		}
		s.election.ElectionPath = path
	}
}

func WithNodeID(nodeID string) serverOption {
	return func(s *ServerConfig) {
		if s.election == nil {
			s.election = defaultElectionConfig()
		}
		s.election.NodeID = nodeID
	}
}

func WithElectionTimeout(timeout time.Duration) serverOption {
	return func(s *ServerConfig) {
		if s.election == nil {
			s.election = defaultElectionConfig()
		}
		s.election.Timeout = timeout
	}
}
func defeaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Address:         "0.0.0.0",
		Port:            6380,
		HealthCheckPort: 8080,
		MaxClients:      1000,
		timeout:         0,
		idleTimeout:     300 * time.Second,
		persistence:     defaultPersistenceConfig(),
		monitoring:      defaultMonitoringConfig(),
		election:        defaultElectionConfig(),
	}
}

type Server struct {
	config *ServerConfig
	// other fields like listener, handlers, etc.
	ramCache memorystore.Cache
	close    chan struct{}
	isLive   bool
}

func NewServer(options ...serverOption) *Server {
	// Start with default configuration
	config := defeaultServerConfig()

	// Apply all the functional options
	for _, option := range options {
		option(config)
	}

	return &Server{
		config:   config,
		ramCache: memorystore.NewCache(),
		close:    make(chan struct{}),
		isLive:   false,
	}
}
func (s *Server) Start() error {
	// Implementation to start the server
	s.ramCache = memorystore.NewCache()
	fmt.Printf("Cache initialized: %v\n", s.ramCache)
	fmt.Printf("Starting server on %s:%d\n", s.config.Address, s.config.Port)
	s.close = make(chan struct{})
	list, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.config.Address, s.config.Port))
	if err != nil {
		return fmt.Errorf("failed to start server: %v", err)
	}
	go func() {
		s.isLive = true
		<-s.close
		list.Close()
	}()
	for {
		conn, err := list.Accept()
		if err != nil {
			select {
			case <-s.close:
				fmt.Printf("Server stopped accepting new connections\n")
				return nil
			default:
				// Check if it's a network operation error (likely listener closed)
				if errors.Is(err, net.ErrClosed) {
					fmt.Printf("Connection was closed: Now leaving\n")
					return nil
				}
				fmt.Printf("Failed to accept connection: %v\n", err)
				continue
			}
		}
		go s.handleConnection(conn)
	}
}
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	// Handle client connection
	fmt.Printf("Accepted connection from %s\n", conn.RemoteAddr().String())
	// Here you would read commands from the connection and process them
}
func (s *Server) Stop() error {
	s.isLive = false
	s.close <- struct{}{}
	fmt.Printf("Stopping server on %s:%d\n", s.config.Address, s.config.Port)
	return nil
}
