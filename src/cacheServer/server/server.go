package server

import (
	memorystore "RapidStore/memoryStore"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var globalLogger *zap.Logger

func init() {
	// TODO: for now this is a basic setup, later we can make it configurable

	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: false,
		Encoding:    "json",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:       "timeStamp",
			LevelKey:      "level",
			MessageKey:    "message",
			CallerKey:     "source Code",
			StacktraceKey: "stacktrace",
			LineEnding:    zapcore.DefaultLineEnding,

			EncodeLevel: zapcore.CapitalLevelEncoder,
			EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
				enc.AppendString(t.Format("2006-01-02 15:04:05"))
			},
			EncodeDuration: zapcore.MillisDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
			EncodeName:     zapcore.FullNameEncoder,
		},
		OutputPaths:      []string{"stdout", "server.log"},
		ErrorOutputPaths: []string{"stderr"},
	}
	logger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	globalLogger = logger
}

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
		idleTimeout:     20 * time.Second,
		persistence:     defaultPersistenceConfig(),
		monitoring:      defaultMonitoringConfig(),
		election:        defaultElectionConfig(),
	}
}

type Server struct {
	config          *ServerConfig
	productionStats ServerInfoMetaData
	dynamicMessage  chan internalServerMSg
	// other fields like listener, handlers, etc.
	ramCache memorystore.Cache
	Wal      *WriteAheadLog
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

	var s = &Server{
		config:         config,
		ramCache:       memorystore.NewCache(),
		Wal:            newWAL(config.persistence.WALPath, uint32(bufferSize), config.persistence.WALSyncInterval),
		dynamicMessage: make(chan internalServerMSg, 1), // Buffered channel to avoid blocking
		close:          make(chan struct{}),
		isLive:         false,
	}
	err := s.initLeader()
	if err != nil {
		panic(fmt.Sprintf("Error initializing leader election: %v\n", err))
	}
	go s.InterServerCommunications(s.config.election.isLeader)
	connAddr := fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
	globalLogger.Info("Server init config:", zap.String("address", connAddr))
	// expose metrics via http endpoint
	go s.exportStats()
	return s
}
func (s *Server) Start() error {
	s.ramCache = memorystore.NewCache()
	s.close = make(chan struct{})
	list, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.config.Address, s.config.Port))
	if err != nil {
		return fmt.Errorf("failed to start server: %v", err)
	}
	connAddr := fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
	globalLogger.Info("Starting server on", zap.String("address", connAddr))
	// background goroutine
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
				globalLogger.Info("Server stopped accepting new connections")
				return nil
			default:
				// Check if it's a network operation error (likely listener closed)
				if errors.Is(err, net.ErrClosed) {
					globalLogger.Info("Connection was closed: Now leaving")
					return nil
				}
				globalLogger.Warn("Failed to accept connection", zap.Error(err))
				continue
			}
		}
		s.productionStats.IncrementActiveConnections()
		go s.handleConnection(conn)
	}
}

func (s *Server) Stop() error {
	s.isLive = false
	s.close <- struct{}{}
	s.config.election.zkConn.Close()
	globalLogger.Info("Stopping server on", zap.String("address", fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)))
	return nil
}
func (s *Server) InterServerCommunications(isHost bool) {

	// Start inter-server connection immediately instead of waiting for ticker
	globalLogger.Debug("Starting initial InterServer connection")
	s.startInterServerConnection(isHost)

}

func (s *Server) startInterServerConnection(isHost bool) chan struct{} {
	stopSignal := make(chan struct{})

	go func() {
		// For now, let's use 0.0.0.0 to bind to all interfaces
		// This should work if your router/firewall is configured correctly
		bindAddr := "0.0.0.0"

		addr := fmt.Sprintf("%s:%d", bindAddr, s.config.Port+1)
		globalLogger.Info("Inter Server communications is @ ", zap.String("address", addr))

		healthListener, err := net.Listen("tcp4", addr)
		if err != nil {
			globalLogger.Warn("Failed to start health check listener", zap.Error(err))
			return
		}
		defer healthListener.Close()

		if isHost {
			globalLogger.Info("Starting Inter Server communications as Leader")
		} else {
			globalLogger.Info("Starting Inter Server communications as Follower")
		}
		for {
			select {
			case <-stopSignal:
				globalLogger.Debug("Stopping InterServer connection")
				return
			default:
				// Set a timeout for Accept to make it non-blocking
				healthListener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))

				conn, err := healthListener.Accept()
				if err != nil {
					// Check if it's a timeout (normal) or real error
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						continue // Just a timeout, continue checking for stop signal
					}
					if errors.Is(err, net.ErrClosed) {
						return
					}
					globalLogger.Warn("(Inter Server Communications) Failed to accept connection", zap.Error(err))
					continue
				}

				// Handle the connection
				go func(c net.Conn) {
					defer c.Close()
					status := "OK"
					if !s.isLive {
						status = "NOT OK"
					}
					buff := make([]byte, 256)
					n, err := c.Read(buff)
					if err != nil {
						globalLogger.Warn("Error reading from connection", zap.Error(err))
						return
					}
					content := string(buff[:n])
					globalLogger.Info("Received message from InterServer comms", zap.String("content", content))
					c.Write([]byte(fmt.Sprintf("Health Status: %s\n", status)))
				}(conn)
			}
		}
	}()

	return stopSignal
}

func (s *Server) exportStats() {
	http.HandleFunc(s.config.monitoring.MetricsPath, s.Metrics)
	go func() {
		var alreadyTried = false
		for s.isLive {
			endpoint := fmt.Sprintf(":%d", s.config.monitoring.MetricsPort)
			globalLogger.Info("Starting metrics server on", zap.String("endpoint", endpoint))
			if err := http.ListenAndServe(endpoint, nil); err != nil {
				globalLogger.Warn("Error starting metrics server", zap.Error(err))
			}
			time.Sleep(s.config.timeout) // Retry after a delay if it fails
			if alreadyTried {
				globalLogger.Debug("Metrics server failed to start after retry, giving up")
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
		globalLogger.Error("Error collecting GC stats", zap.Error(err))
		return
	}
	var resp = map[string]interface{}{
		"timeStamp":  time.Now(),
		"serverInfo": s.productionStats,
		"gcStats":    *gcStats,
	}
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		globalLogger.Error("Error generating JSON response", zap.Error(err))
		http.Error(w, "Error generating JSON response", http.StatusInternalServerError)
		return
	}
	w.Write(jsonResp)
}
