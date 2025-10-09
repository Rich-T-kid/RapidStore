package main

import (
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
)

func init() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		fmt.Printf("Error loading .env file: %v\n", err)
	}
	// Initialize logger
	logger, _ = zap.NewProduction()
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

type serverInfo struct {
	IP   string
	Port string
}
type routerServer struct {
	exposePort         int
	leaderAddr         string
	leaderPort         string
	followers          []serverInfo
	leaderUpdateChan   <-chan zk.Event // listen for updates/deletes from /leader chan
	followerUpdateChan <-chan zk.Event // listen for updates/deletes from /follower chan
	zooManager         *zk.Conn
	isLive             bool
	closer             func() // close the listener
	sync.RWMutex              // used to lock reads when the leader or followers are being updated
}

func (r *routerServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("error reading from connection ", err)
		return
	}
	request := string(buffer[:n])
	if strings.HasPrefix(strings.ToUpper(request), "ECHO") {
		message := strings.TrimSpace(request[4:])
		response := fmt.Sprintf("%s\n", message)
		conn.Write([]byte(response))
	}
	if strings.HasPrefix(strings.ToUpper(request), "SET") { // write operation -> leader
		resp, err := r.requestCacheServer(r.leaderAddr, r.leaderPort, buffer[:n])
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("Error routing to leader: %v\n", err)))
			return
		}
		conn.Write(resp)
	}
	if strings.HasPrefix(strings.ToUpper(request), "GET") {
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
			return
		}
		conn.Write(response)
	}
	if strings.HasPrefix(strings.ToUpper(request), "STOP") {
		r.isLive = false
		go func() {
			time.Sleep(time.Second * 1) // give it a second to send the shutdown message
			r.closer()
		}()
		conn.Write([]byte("Router server shutting down...\n"))

	}
}

// TODO: this needs to be updated when we develop the protocol more
func (r *routerServer) requestCacheServer(addr string, port string, payload []byte) ([]byte, error) {
	if addr == "" || port == "" {
		return nil, fmt.Errorf("invalid address or port for cache server")
	}
	path := fmt.Sprintf("%s:%s", addr, port)
	conn, err := net.Dial("tcp", path)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to cache server at %s: %w", path, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second)) // Set a timeout for the operation

	_, err = conn.Write(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to cache server at %s: %w", path, err)
	}

	response := make([]byte, 4096*4) // Adjust buffer size as needed
	n, err := conn.Read(response)
	if err != nil {
		if err == io.EOF {
			return response[:n], nil // Return what was read before EOF
		}
		return nil, fmt.Errorf("failed to read response from cache server at %s: %w", path, err)
	}

	return response[:n], nil
}

// TODO: race condition with a follower that gets promoted to leader is still presnt in the follower list. which is fine lowkey since it will just handler read
func (r *routerServer) listenForLeaderChanges() {
	for r.isLive {
		select {
		case event, ok := <-r.leaderUpdateChan:
			if !ok {
				fmt.Println("Leader watch channel closed")
				return
			}
			fmt.Printf("Leader change detected: %v\n", event)
			// Re-fetch leader info
			switch event.Type {
			case zk.EventNodeDeleted, zk.EventNodeCreated, zk.EventNodeDataChanged:
				fmt.Println("Leader node deleted, waiting for new leader...")
				time.Sleep(3 * time.Second) // brief pause to allow new leader to be elected
				err := r.refreshLeader()
				if err != nil {
					fmt.Printf("Error refreshing leader info: %v\n", err)
				}
			}
		case event, ok := <-r.followerUpdateChan:
			if !ok { // this should never happen but for now just so we dont get pointer deref errors
				fmt.Println("Follower watch channel closed")
				return
			}
			switch event.Type {
			case zk.EventNodeChildrenChanged, zk.EventNodeCreated, zk.EventNodeDeleted:
				fmt.Println("Follower nodes changed, refreshing follower list...")
			}
			r.refreshFollowers()
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
	_, _, leaderW, err := r.zooManager.GetW(leaderPath)
	if err != nil {
		return err
	}
	r.leaderUpdateChan = leaderW

	leaderInfo, err := getLeader(r.zooManager, leaderPath)
	if err != nil {
		if err == ErrNoLeader {
			fmt.Printf("no leader currently elected\n")
			r.Lock()
			r.leaderAddr = ""
			r.leaderPort = ""
			r.Unlock()
			fmt.Printf("Updated leader to: %s:%s\n", r.leaderAddr, r.leaderPort)
			return nil
		}
		return err
	}
	r.Lock()
	r.leaderAddr = leaderInfo.IP
	r.leaderPort = leaderInfo.Port
	r.Unlock()
	fmt.Printf("Updated leader to: %s:%s\n", r.leaderAddr, r.leaderPort)

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
	fmt.Printf("Updated followers: %v\n", r.followers)

	_, _, followerW, err := r.zooManager.ChildrenW(followerPath)
	if err != nil {
		return fmt.Errorf("failed to watch follower changes: %w", err)

	}
	r.followerUpdateChan = followerW
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
	fmt.Printf("Current leader: %s\n", leaderInfo)

	// Parse the IP and port
	parts := strings.Split(leaderInfo, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid leader info format, expected IP:port, got %s", leaderInfo)
	}

	addr := parts[0]
	port := parts[1]
	fmt.Printf("Leader IP: %s, Port: %s\n", addr, port)

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
			fmt.Printf("Failed to get data for follower %s: %v\n", child, err)
			continue
		}

		followerAddr := string(data)
		if followerAddr != "" {
			parts := strings.Split(followerAddr, ":")
			if len(parts) != 2 {
				fmt.Printf("Invalid follower address format for %s: %s\n", child, followerAddr)
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
func newRouter() (*routerServer, error) {
	var zooKeeperAddr = os.Getenv("ZOOKEEPERADDR")
	fmt.Printf("Using Zookeeper address: %s\n", zooKeeperAddr)
	zkConn, _, err := zk.Connect([]string{zooKeeperAddr}, time.Second*30)
	if err != nil {
		return nil, err
	}
	// Ensure rapidStore path exists
	exist, _, err := zkConn.Exists(basePath)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, fmt.Errorf("%s does not yet exist \n cannot route message to cache servers if no cache servers are currently active", basePath) // if this dir doesnt already exist it guarentees theres no leader in which case init is wrong
	}
	servInfo, err := getLeader(zkConn, leaderPath)
	if err != nil {
		return nil, err
	}
	addr := servInfo.IP
	port := servInfo.Port
	fmt.Printf("Leader IP: %s, Port: %s\n", addr, port)

	_, _, leaderW, err := zkConn.GetW(leaderPath)
	if err != nil {
		return nil, err
	}
	// follower data
	followers, err := getFollowers(zkConn, followerPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get followers: %w", err)
	}
	fmt.Printf("Current followers: %v\n", followers)

	_, _, followerW, err := zkConn.ChildrenW(followerPath)
	if err != nil {
		return nil, fmt.Errorf("failed to watch follower changes: %w", err)
	}

	return &routerServer{
		exposePort:         8888,
		leaderAddr:         addr,
		leaderPort:         port,
		followers:          followers,
		leaderUpdateChan:   leaderW,
		followerUpdateChan: followerW,
		zooManager:         zkConn,
	}, nil
}
func endListener(l net.Listener) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.Close()
		})
	}
}

// TODO: Read from zookeeper for the followers IP and port (Done)
// TODO: parse basic GET & SET commmands and route them to correct place (Write -> leader, Read -> any follower or leader) (Done)
// TODO: Finish implementing ListenForChanges (leader changes) & (follower changes) and update internal state
// TODO: Replace all printfs with proper logging (zap)

// Not in the scope of this ticket right now
// TODO: Think about ways to handle connection retries (exponential backoff?)
func main() {
	r, err := newRouter()
	if err != nil {
		panic(err)
	}
	path := fmt.Sprintf(":%d", r.exposePort)
	list, err := net.Listen("tcp", path)
	if err != nil {
		fmt.Printf("error creating connection at port %s, %b", path, err)
		return
	}
	fmt.Printf("Router server listening on port %d\n", r.exposePort)
	defer list.Close()

	r.isLive = true
	go r.listenForLeaderChanges()
	r.closer = endListener(list)

	for r.isLive {
		conn, err := list.Accept()
		if err != nil {
			if strings.HasSuffix(err.Error(), "use of closed network connection") {
				break // Exit the loop if the listener is closed
			}
			fmt.Printf("encountered error reading connection %v \n", err)
			continue
		}
		go r.handleConnection(conn)
	}
	fmt.Printf("Router server is no longer live | shutting down...\n")
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
