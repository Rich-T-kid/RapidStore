package server

import (
	memorystore "RapidStore/memoryStore"
	"RapidStore/recovery"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var globalLogger *zap.Logger

func init() {

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

// NewConfigForTesting creates a server configuration without starting the server
// This is useful for testing configuration without the overhead of starting goroutines
func newConfigForTesting(options ...serverOption) *ServerConfig {
	// Start with default configuration
	config := defaultServerConfig()

	// Apply all the functional options
	for _, option := range options {
		option(config)
	}

	return config
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
func defaultServerConfig() *ServerConfig {
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
	mu       sync.RWMutex
	wal      *WriteAheadLog
	close    chan struct{}
	isLive   bool
}

func NewServerFromFile(configFile string) *Server {
	f, err := os.OpenFile(configFile, os.O_RDONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("Error reading config file: %v\n", err))
	}
	cfg, err := ServerFromConfig(configFile, f)
	if err != nil {
		panic(err)
	}
	return newServer(cfg)
}

func NewServer(options ...serverOption) *Server {
	// Start with default configuration
	config := defaultServerConfig()

	// Apply all the functional options
	for _, option := range options {
		option(config)
	}

	return newServer(config)
}
func newServer(cfg *ServerConfig) *Server {
	s := &Server{
		config:         cfg,
		ramCache:       memorystore.NewCache(),
		wal:            newWAL(cfg.persistence.WALPath, uint32(cfg.persistence.WALMaxSize), cfg.persistence.WALSyncInterval),
		dynamicMessage: make(chan internalServerMSg, 1),
		close:          make(chan struct{}),
		isLive:         false,
	}

	if err := s.initLeader(); err != nil {
		panic(fmt.Sprintf("error initializing leader election: %v", err))
	}
	// start early to avoid missing messages
	go s.interServerCommunications()
	go s.exportStats()

	connAddr := fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
	globalLogger.Info("Server initialized", zap.String("address", connAddr))
	return s
}
func (s *Server) Start() error {
	go s.watchControlC()
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
	go s.StoreStateLoop() // need to call this after s.live is set. if its not the leader it will just exit
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

// simple func for one purpose: watch for Control-C to gracefully shutdown
func (s *Server) watchControlC() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop() // Ensure the stop function is called to release resources.
	<-ctx.Done()
	s.Stop()

}

func (s *Server) Stop() error {
	s.isLive = false
	s.close <- struct{}{}
	s.config.election.zkConn.Close()
	globalLogger.Info("Stopping server on", zap.String("address", fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)))
	if s.config.election.isLeader {
		err := s.SaveState()
		if err != nil {
			globalLogger.Error("Error saving state during shutdown", zap.Error(err))
			return err
		}
	}
	globalLogger.Warn("Server stopped successfully")
	return nil
}

func (s *Server) exportStats() {
	// Create a new ServeMux for this server instance to avoid conflicts
	// application code wise this is unimportant but test are strcutred poorly right now, easiest fix
	mux := http.NewServeMux()
	mux.HandleFunc(s.config.monitoring.MetricsPath, s.metrics)

	go func() {
		var alreadyTried = false
		for s.isLive {
			endpoint := fmt.Sprintf(":%d", s.config.monitoring.MetricsPort)
			globalLogger.Info("Starting metrics server on", zap.String("endpoint", endpoint))
			if err := http.ListenAndServe(endpoint, mux); err != nil {
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
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
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
func (s *Server) Serialize() ([]byte, error) {
	content, err := s.ramCache.Serialize()
	if err != nil {
		return nil, err
	}
	var exampleState = map[string]interface{}{
		"internal_data":   base64.StdEncoding.EncodeToString(content),
		"lastSyncedIndex": s.wal.sequenceNumber,
		// not definate but this seems goods
	}
	return json.Marshal(exampleState)

}

// pull latest state from external storage and load into server
func (s *Server) DownloadLatestState() error {
	content, err := recovery.NewExternalStorage().DownloadState(recovery.StoragePath, recovery.CredPath)
	if err != nil {
		return err
	}
	type ExternalState struct {
		InternalData    string `json:"internal_data"`
		LastSyncedIndex uint64 `json:"lastSyncedIndex"`
	}
	var externalContent ExternalState
	if err := json.Unmarshal(content, &externalContent); err != nil {
		return err
	}
	globalLogger.Info("State representaion", zap.Any("state", externalContent))
	internalData := externalContent.InternalData

	cacheData, err := base64.StdEncoding.DecodeString(internalData)
	if err != nil {
		globalLogger.Info("error decoding base64 body of internal_data")
		return err
	}
	err = s.ramCache.Deserialize(cacheData)
	if err != nil {
		return err
	}
	s.wal.sequenceNumber = externalContent.LastSyncedIndex
	globalLogger.Info("Successfully loaded state from external storage", zap.Uint64("lastSyncedIndex", s.wal.sequenceNumber))
	return nil
}
func (s *Server) SaveState() error {

	// assume leader always has the latest data so write its state out to disk
	curContent, err := s.Serialize()
	if err != nil {
		globalLogger.Error("Error serializing state", zap.Error(err))
		return err
	}
	globalLogger.Warn("Saving State to Storage ", zap.Any("state", string(curContent)))
	return recovery.NewExternalStorage().SaveState(curContent, recovery.StoragePath, recovery.CredPath)
}
func (s *Server) StoreStateLoop() {
	ticker := time.NewTicker(15 * time.Second) // TODO: make configurable
	defer ticker.Stop()
	for s.isLive && s.config.election.isLeader {
		// only the leader should be saving state, this is always called after a new election, in the s.Start function it runs even if its not the leader
		// but then the if check outside the loop prevents it from doing anything
		select {
		case <-ticker.C:
			if err := s.SaveState(); err != nil {
				globalLogger.Error("Error saving state", zap.Error(err))
			} else {
				globalLogger.Info("State saved successfully")
			}
		case <-s.close:
			globalLogger.Info("Stopping state storage loop")
			return
		}
	}
}
