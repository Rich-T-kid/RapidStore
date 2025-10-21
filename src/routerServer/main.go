package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

func init() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		fmt.Printf("Error loading .env file: %v\n", err)
	}
	// Initialize logger
	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.DebugLevel), // Show debug messages
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
	l, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	logger = l
}

var (
	ErrNoLeader = fmt.Errorf("no leader available")
	logger      *zap.Logger
)

const (
	basePath     = "/rapidstore"
	leaderPath   = "/rapidstore/leader"
	followerPath = "/rapidstore/follower"
)

var writeOperations = map[string]bool{
	"SET":     true,
	"DEL":     true,
	"EXPIRE":  true,
	"INCR":    true,
	"DECR":    true,
	"APPEND":  true,
	"MSET":    true,
	"HSET":    true,
	"HDEL":    true,
	"LPUSH":   true,
	"RPUSH":   true,
	"LPOP":    true,
	"RPOP":    true,
	"SADD":    true,
	"SREM":    true,
	"ZADD":    true,
	"ZREMOVE": true,
}

type serverInfo struct {
	IP   string
	Port string
}
type routerServer struct {
	exposePort           int
	ryw                  bool
	rywCache             map[string]time.Time // IP -> last write time
	rywDuration          time.Duration
	leaderAddr           string
	leaderPort           string
	followers            []serverInfo
	rapidStoreUpdateChan <-chan zk.Event
	zooManager           *zk.Conn
	isLive               bool
	closer               func() // close the listener
	sync.RWMutex                // used to lock reads when the leader or followers are being updated
}
type configInfo struct {
	Port                 int  `json:"port" yaml:"port"`
	ReadYourWrites       bool `json:"read_your_writes" yaml:"read_your_writes"`
	ReadYourWritesWindow int  `json:"ryw_window" yaml:"ryw_window"` // in milliseconds
}

func (r *routerServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Keep handling requests until connection is closed
	for {
		buffer := make([]byte, KB*4)
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				logger.Debug("connection closed or error reading", zap.Error(err))
			}
			return
		}
		request := string(buffer[:n])
		parts := strings.Split(request, " ")

		switch strings.ToUpper(parts[0]) {
		case "ECHO":
			message := strings.TrimSpace(request[4:])
			response := fmt.Sprintf("%s\n", message)
			conn.Write([]byte(response))
		case "PING":
			conn.Write([]byte("PONG\n"))
		case "STOP":
			r.isLive = false
			go func() {
				time.Sleep(time.Millisecond * 500) // give it a second to send the shutdown message
				r.closer()
			}()
			conn.Write([]byte("Router server shutting down...\n"))
			return // Exit the loop and close connection for STOP command
		default:
			// Debug: Show RYW cache status
			for k, v := range r.rywCache {
				timeSinceWrite := time.Since(v)
				if timeSinceWrite > r.rywDuration {
					fmt.Printf("EXPIRED: %s - expired %v ago\n", k, timeSinceWrite-r.rywDuration)
				} else {
					timeRemaining := r.rywDuration - timeSinceWrite
					fmt.Printf("ACTIVE: %s - expires in %v\n", k, timeRemaining)
				}
			}
			// Determine routing logic based on command type and read-your-writes consistency
			command := strings.ToUpper(parts[0])
			routeToLeader := false

			if writeOperations[command] {
				// Always route writes to leader
				fmt.Printf("Routing write operation %s to leader\n", command)
				routeToLeader = true
			} else if r.ryw {
				// For reads: check if there was a recent write from this client
				lastWrite, exists := r.rywCache[conn.RemoteAddr().String()]
				if exists && time.Since(lastWrite) <= r.rywDuration {
					// Recent write detected, route read to leader for consistency
					fmt.Printf("Routing read operation %s to leader due to recent write\n", command)
					routeToLeader = true
				}
			}

			if routeToLeader {
				// Update cache with current timestamp for write operations
				fmt.Printf("routeToLeader is true for %s\n", buffer[:n])
				if writeOperations[command] {
					fmt.Printf("Updating RYW cache for %s\n", conn.RemoteAddr().String())
					r.rywCache[conn.RemoteAddr().String()] = time.Now()
				}
				// Route to leader
				r.RWMutex.RLock() // in case it gets updated while reading
				var addr = r.leaderAddr
				var port = r.leaderPort
				r.RWMutex.RUnlock()
				resp, err := r.requestCacheServer(addr, port, buffer[:n])
				if err != nil {
					conn.Write([]byte(fmt.Sprintf("Error routing to leader: %v\n", err)))
					continue // Continue to next request instead of closing connection
				}
				conn.Write(resp)
			} else {
				// Read operation -> route to random follower with leader fallback
				server, err := r.randomFollower()
				if err != nil {
					// fallback to leader if no followers available
					server = serverInfo{
						IP:   r.leaderAddr,
						Port: r.leaderPort,
					}
				}
				response, err := r.requestCacheServer(server.IP, server.Port, buffer[:n])
				if err != nil {
					conn.Write([]byte(fmt.Sprintf("Error routing to cache server: %v\n", err)))
					continue // Continue to next request instead of closing connection
				}
				conn.Write(response)
			}
		}
	}
}

func (r *routerServer) requestCacheServer(addr string, port string, payload []byte) ([]byte, error) {
	if addr == "" || port == "" {
		return nil, fmt.Errorf("invalid address or port for cache server (addr: '%s', port: '%s')", addr, port)
	}
	path := fmt.Sprintf("%s:%s", addr, port)
	conn, err := net.Dial("tcp", path)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to cache server at %s: %w", path, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second)) // Set a timeout for the operation

	_, err = conn.Write(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to cache server at %s: %w", path, err)
	}

	response := make([]byte, 4096*4) // Adjust buffer
	n, err := conn.Read(response)
	if err != nil {
		if err == io.EOF {
			return response[:n], nil
		}
		return nil, fmt.Errorf("failed to read response from cache server at %s: %w", path, err)
	}

	return response[:n], nil
}

func (r *routerServer) listenForChanges() {
	for r.isLive {
		event, ok := <-r.rapidStoreUpdateChan
		if !ok {
			logger.Info("RapidStore watch channel closed")
			return
		}
		switch event.Type {
		case zk.EventNodeChildrenChanged:
			logger.Info("rapidstore node changed, checking leader and followers...", zap.Any("event", event))

			// Get all children of /rapidstore
			children, _, err := r.zooManager.Children("/rapidstore")
			if err != nil {
				logger.Error("Failed to get rapidstore children", zap.Error(err))
				break
			}

			// Check if leader path exists
			leaderExists := false
			followerExists := false

			for _, child := range children {
				if child == "leader" {
					leaderExists = true
				}
				if child == "follower" {
					followerExists = true
				}
			}

			// Handle leader changes
			if leaderExists {
				logger.Info("Leader path exists, refreshing leader info")
				err := r.refreshLeader()
				if err != nil {
					logger.Error("Error refreshing leader info", zap.Error(err))
				}
			} else {
				logger.Info("Leader path does not currently exist,")
			}

			// Handle follower changes
			if followerExists {
				logger.Info("Follower path exists, refreshing follower list")
				err := r.refreshFollowers()
				if err != nil {
					logger.Error("Error refreshing follower info", zap.Error(err))
				}
			} else {
				logger.Info("Follower path does not currently exist")
			}
			// set up new watch
			_, _, r.rapidStoreUpdateChan, err = r.zooManager.ChildrenW(basePath)
			if err != nil {
				logger.Error("Failed to re-establish rapidstore watch", zap.Error(err))
				return
			}
		}
	}
}
func (r *routerServer) randomFollower() (serverInfo, error) {
	size := len(r.followers)
	if size == 0 {
		return serverInfo{}, fmt.Errorf("no followers available")
	}
	index := time.Now().UnixNano() % int64(size)
	return r.followers[index], nil

}
func (r *routerServer) refreshLeader() error {
	leaderInfo, err := getLeader(r.zooManager, leaderPath)
	if err != nil {
		if err == ErrNoLeader {
			logger.Info("No leader currently elected")
			r.Lock()
			r.leaderAddr = ""
			r.leaderPort = ""
			r.Unlock()
			logger.Debug("upddated leader to empty values", zap.String("addr", r.leaderAddr), zap.String("port", r.leaderPort))
			return nil
		}
		return err
	}
	r.Lock()
	r.leaderAddr = leaderInfo.IP
	r.leaderPort = leaderInfo.Port
	r.Unlock()
	logger.Debug("upddated leader", zap.String("addr", r.leaderAddr), zap.String("port", r.leaderPort))

	return nil
}
func (r *routerServer) refreshFollowers() error {
	followers, err := getFollowers(r.zooManager, followerPath)
	if err != nil {
		return fmt.Errorf("failed to get followers: %w", err)
	}
	r.Lock()
	r.followers = followers
	r.Unlock()
	logger.Debug("updated followers", zap.Any("followers", r.followers))

	return nil
}
func getLeader(zkConn *zk.Conn, leaderPath string) (*serverInfo, error) {
	// Check if leader path exists
	exist, _, err := zkConn.Exists(leaderPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check if leader path exists: %w", err)
	}
	if !exist {
		return nil, ErrNoLeader
	}

	// Get leader data
	data, _, err := zkConn.Get(leaderPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get leader data: %w", err)
	}

	leaderInfo := string(data)
	logger.Debug("Fetched leader info from Zookeeper", zap.String("leaderInfo", leaderInfo))

	// Parse the IP and port
	parts := strings.Split(leaderInfo, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid leader info format, expected IP:port, got %s", leaderInfo)
	}

	addr := parts[0]
	port := parts[1]
	logger.Debug("Parsed leader info", zap.String("IP", addr), zap.String("Port", port))

	return &serverInfo{
		IP:   addr,
		Port: port,
	}, nil
}
func getFollowers(zkConn *zk.Conn, followerPath string) ([]serverInfo, error) {
	var followers []serverInfo

	// Ensure follower path exists first
	exist, _, err := zkConn.Exists(followerPath)
	if err != nil {
		return followers, fmt.Errorf("failed to check if follower path exists: %w", err)
	}
	if !exist {
		_, err = zkConn.Create(followerPath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return followers, fmt.Errorf("failed to create follower path: %w", err)
		}
	}

	// Get all follower children nodes
	children, _, err := zkConn.Children(followerPath)
	if err != nil {
		return followers, fmt.Errorf("failed to get follower children: %w", err)
	}

	// Read data from each follower node
	for _, child := range children {
		fullPath := fmt.Sprintf("%s/%s", followerPath, child)
		data, _, err := zkConn.Get(fullPath)
		if err != nil {
			// Log error but continue with other followers
			logger.Error("Failed to get data for follower", zap.String("child", child), zap.Error(err))
			continue
		}

		followerAddr := string(data)
		if followerAddr != "" {
			parts := strings.Split(followerAddr, ":")
			if len(parts) != 2 {
				logger.Error("Invalid follower address format", zap.String("child", child), zap.String("followerAddr", followerAddr))
				continue
			}
			followers = append(followers, serverInfo{
				IP:   parts[0],
				Port: parts[1],
			})
			//
		}
	}

	return followers, nil
}
func newRouter(c configInfo) *routerServer {
	var zooKeeperAddr = "35.222.157.12:2181"
	zkConn, _, err := zk.Connect([]string{zooKeeperAddr}, time.Second*2)
	if err != nil {
		fmt.Printf("Error connecting to Zookeeper: %v\n", err)
		panic(err)
	}
	// Ensure rapidStore path exists
	exist, _, err := zkConn.Exists(basePath)
	if err != nil {
		panic(err)
	}
	if !exist {
		panic(fmt.Errorf("%s does not yet exist \n cannot route message to cache servers if no cache servers are currently active", basePath)) // if this dir doesnt already exist it guarentees theres no leader in which case init is wrong
	}
	servInfo, err := getLeader(zkConn, leaderPath)
	if err != nil {
		panic(err)
	}
	addr := servInfo.IP
	port := servInfo.Port
	logger.Debug("Initial leader info", zap.String("IP", addr), zap.String("Port", port))

	// follower data
	followers, err := getFollowers(zkConn, followerPath)
	if err != nil {
		panic(fmt.Errorf("failed to get followers: %w", err))
	}
	logger.Debug("Initial followers", zap.Any("followers", followers))

	_, _, rapidStoreW, err := zkConn.ChildrenW(basePath)
	if err != nil {
		panic(fmt.Errorf("failed to watch rapidstore changes: %w", err))
	}

	return &routerServer{
		exposePort:           c.Port,
		ryw:                  c.ReadYourWrites,
		rywDuration:          time.Duration(c.ReadYourWritesWindow) * time.Millisecond,
		rywCache:             make(map[string]time.Time),
		leaderAddr:           addr,
		leaderPort:           port,
		followers:            followers,
		rapidStoreUpdateChan: rapidStoreW,
		zooManager:           zkConn,
	}
}
func (r *routerServer) Start() {
	path := fmt.Sprintf(":%d", r.exposePort)
	list, err := net.Listen("tcp", path)
	if err != nil {
		logger.Error("error creating connection", zap.String("port", path), zap.Error(err))
		return
	}
	logger.Debug("Successfully created listener", zap.String("port", path))
	defer list.Close()
	r.isLive = true
	go r.listenForChanges()
	r.closer = endListener(list)

	for r.isLive {
		conn, err := list.Accept()
		if err != nil {
			if strings.HasSuffix(err.Error(), "use of closed network connection") {
				break // Exit the loop if the listener is close
			}
			logger.Error("encountered error reading connection", zap.Error(err))
			break
		}
		// Handle each connection in a new goroutine && recover from panics so one bad connection doesnt crash the server
		go func() {
			handlePanics(func() { r.handleConnection(conn) })
		}()
	}

}
func endListener(l net.Listener) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.Close()
		})
	}
}

func main() {
	if len(os.Args) > 1 {
		r := newRouter(readConfigFile(os.Args[1]))
		r.Start()
	} else {
		r := newRouter(configInfo{
			Port:           6579,
			ReadYourWrites: true,
		})
		r.Start()
	}
	logger.Info("Router server shutting down...")
}

func getExternalIP() (string, error) {
	services := []string{
		"https://api.ipify.org",
		"https://icanhazip.com",
		"https://ipecho.net/plain",
		"https://myexternalip.com/raw",
	}

	for _, service := range services {
		resp, err := http.Get(service)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			continue
		}

		ip := strings.TrimSpace(string(body))
		if net.ParseIP(ip) != nil {
			return ip, nil
		}
	}

	return "", fmt.Errorf("failed to get external IP from any service")
}

func handlePanics(fn func()) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("Recovered:", err)
		}
	}()
	fn()
}
func readConfigFile(configFile string) configInfo {
	suffix := strings.ToLower(configFile[strings.LastIndex(configFile, ".")+1:])
	if suffix != "json" && suffix != "yml" && suffix != "yaml" {
		panic(fmt.Sprintf("Unsupported config file format: %s\n", suffix))
	}
	f, err := os.OpenFile(configFile, os.O_RDONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("Error reading config file: %v\n", err))
	}
	switch suffix {
	case "json":
		return readJsonConfig(f)
	case "yml", "yaml":
		return readYAMLConfig(f)
	default:
		panic(fmt.Sprintf("Unsupported config file format: %s\n", suffix))
	}
}
func readYAMLConfig(r io.Reader) configInfo {
	var config configInfo
	decoder := yaml.NewDecoder(r)
	err := decoder.Decode(&config)
	if err != nil {
		panic(fmt.Errorf("error decoding YAML config: %w", err))
	}
	return config
}
func readJsonConfig(r io.Reader) configInfo {
	var config configInfo
	decoder := json.NewDecoder(r)
	err := decoder.Decode(&config)
	if err != nil {
		panic(fmt.Errorf("error decoding JSON config: %w", err))
	}
	return config
}
