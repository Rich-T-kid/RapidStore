package server

import (
	memorystore "RapidStore/memoryStore"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
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
	config          *ServerConfig
	productionStats ServerInfoMetaData
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
	s.close = make(chan struct{})
	list, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.config.Address, s.config.Port))
	if err != nil {
		return fmt.Errorf("failed to start server: %v", err)
	}
	fmt.Printf("Starting server on %s:%d\n", s.config.Address, s.config.Port)
	// background goroutine
	go func() {
		s.isLive = true
		// used to talk to other servers , (Leaders, followers) for healthchecks, replication logs, ect.
		go s.InterServerCommunications()
		// expose metrics via http endpoint
		go s.exportStats()
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
		s.productionStats.IncrementActiveConnections()
		go s.handleConnection(conn)
	}
}
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	defer s.productionStats.DecrementActiveConnections()
	// Handle client connection
	fmt.Printf("Accepted connection from %s\n", conn.RemoteAddr().String())

	// Keep reading commands until connection is closed
	for {
		buff := make([]byte, 1024)
		n, err := conn.Read(buff)
		if err != nil {
			fmt.Printf("Connection closed or error reading: %v\n", err)
			return
		}

		parts := strings.Split(strings.TrimSpace(string(buff[:n])), " ")
		base := parts[0]

		if base == "Close" {
			s.Stop()
			return // Close this connection when server stops
		} else if base == "PING" {
			conn.Write([]byte("PONG\n"))
		} else if strings.ToUpper(base) == "SET" && len(parts) >= 3 {
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			s.ramCache.SetKey(key, value)
		} else if strings.ToUpper(base) == "GET" && len(parts) == 2 {
			key := parts[1]
			val := s.ramCache.GetKey(key)
			if val == "" {
				conn.Write([]byte("(nil)\n"))
			} else {
				conn.Write([]byte(fmt.Sprintf("%s\n", val)))
			}
		} else {
			responseMsg := fmt.Sprintf("Echo: %s\n", strings.Join(parts, " "))
			conn.Write([]byte(responseMsg))
		}
	}
}
func (s *Server) Stop() error {
	s.isLive = false
	s.close <- struct{}{}
	fmt.Printf("Stopping server on %s:%d\n", s.config.Address, s.config.Port)
	return nil
}
func (s *Server) InterServerCommunications() {
	addr := fmt.Sprintf("%s:%d", s.config.Address, s.config.HealthCheckPort)
	fmt.Printf("Starting health check listener on %s\n", addr)
	healthListener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("Failed to start health check listener: %v\n", err)
		return
	}
	defer healthListener.Close()
	for {
		conn, err := healthListener.Accept()
		if err != nil {
			select {
			case <-s.close:
				fmt.Printf("Health check listener stopped\n")
				return
			default:
				if errors.Is(err, net.ErrClosed) {
					fmt.Printf("Health check connection was closed: Now leaving\n")
					return
				}
				fmt.Printf("Failed to accept health check connection: %v\n", err)
				continue
			}
		}
		go func(c net.Conn) {
			defer c.Close()
			status := "OK"
			if !s.isLive {
				status = "NOT OK"
			}
			c.Write([]byte(fmt.Sprintf("Health Status: %s\n", status)))
		}(conn)
	}

}

func (s *Server) exportStats() {
	http.HandleFunc(s.config.monitoring.MetricsPath, s.Metrics)
	go func() {
		var alreadyTried = false
		for s.isLive {
			endpoint := fmt.Sprintf(":%d", s.config.monitoring.MetricsPort)
			fmt.Printf("Starting metrics server on %s\n", endpoint)
			if err := http.ListenAndServe(endpoint, nil); err != nil {
				fmt.Printf("Error starting metrics server: %v\n", err)
			}
			time.Sleep(s.config.timeout) // Retry after a delay if it fails
			if alreadyTried {
				fmt.Printf("Metrics server failed to start after retry, giving up: \n")
				break // Avoid infinite retry loop
			}
			alreadyTried = true
		}
	}()
}
func (s *Server) Metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	gcStats, err := s.config.monitoring.collectGcStats()
	if err != nil {
		fmt.Printf("Error collecting GC stats: %v\n", err)
		return
	}
	var resp = map[string]interface{}{
		"timeStamp":  time.Now(),
		"serverInfo": s.productionStats,
		"gcStats":    *gcStats,
	}
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "Error generating JSON response", http.StatusInternalServerError)
		return
	}
	w.Write(jsonResp)
}
