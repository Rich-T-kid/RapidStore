package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/joho/godotenv"
)

func init() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		fmt.Printf("Error loading .env file: %v\n", err)
	}
}

const (
	basePath   = "/rapidstore"
	leaderPath = "/rapidstore/leader"
)

type routerServer struct {
	exposePort       int
	leaderAddr       string
	leaderPort       int
	leaderUpdateChan <-chan zk.Event // listen for updates/deletes from /leader chan
	zooManager       *zk.Conn
	isLive           bool
	closer           func() // close the listener
	sync.RWMutex
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
	if strings.HasPrefix(strings.ToUpper(request), "STOP") {
		r.isLive = false
		go func() {
			time.Sleep(time.Second * 1)
			r.closer()
		}()
		conn.Write([]byte("Router server shutting down...\n"))

	}
}
func (r *routerServer) listenForLeaderChanges() {
	for r.isLive {
		fmt.Printf("Listening for leader changes...\n")
		time.Sleep(time.Second * 2)
	}
}
func newRouter() (*routerServer, error) {
	var zooKeeperAddr = os.Getenv("ZOOKEEPERADDR")
	fmt.Printf("Using Zookeeper address: %s\n", zooKeeperAddr)
	zkConn, _, err := zk.Connect([]string{zooKeeperAddr}, time.Second*5)
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
	exist, _, err = zkConn.Exists(leaderPath)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, fmt.Errorf("%s does not yet exist, cannot route writes to leader if there is no leader", leaderPath)
	}
	data, _, err := zkConn.Get(leaderPath)
	if err != nil {
		return nil, err
	}
	leaderInfo := string(data)
	fmt.Printf("Current leader: %s\n", leaderInfo)

	// If you want to parse the IP and port
	parts := strings.Split(leaderInfo, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid leader info format, expected IP:port, got %s", leaderInfo)
	}
	addr := parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid port format in leader info, got %s", parts[1])
	}
	fmt.Printf("Leader IP: %s, Port: %d\n", addr, port)

	_, _, leaderW, err := zkConn.GetW(leaderPath)
	if err != nil {
		return nil, err
	}

	return &routerServer{
		exposePort:       8888,
		leaderUpdateChan: leaderW,
		zooManager:       zkConn,
		leaderAddr:       addr,
		leaderPort:       port,
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

// TODO: Read from zookeeper for the followers IP and port
// TODO: parse basic GET & SET commmands and route them to correct place (Write -> leader, Read -> any follower or leader)
// TODO: Finish implementing ListenForChanges (leader changes) & (follower changes) and update internal state
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
