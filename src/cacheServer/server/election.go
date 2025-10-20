package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
	"go.uber.org/zap"
)

const (
	basePath     = "/rapidstore"
	leaderPath   = "/rapidstore/leader"
	followerPath = "/rapidstore/follower"
	electionPath = "/rapidstore/election"
	nodePrefix   = "/rapidstore/election/node_"
)

// read from zoo keeper and such
func (s *Server) initLeader() error {
	zkConn, _, err := zk.Connect(s.config.election.ZookeeperServers, s.config.election.connTimeout)
	if err != nil {
		return err
	}
	// keep the connection going for the lifetime of the server
	s.config.election.zkConn = zkConn
	// Ensure rapidStore path exists
	exist, _, err := zkConn.Exists("/rapidstore")
	if err != nil {
		return err
	}
	if !exist {
		_, err = zkConn.Create(basePath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return fmt.Errorf("failed to create /rapidstore: %w", err)
		}
	}
	// Ensure election path exists
	exist, _, err = zkConn.Exists(electionPath)
	if err != nil {
		return err
	}
	if !exist {
		_, err = zkConn.Create(electionPath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return fmt.Errorf("failed to create %s: %w", electionPath, err)
		}
	}

	// Create ephemeral sequential node
	path, err := zkConn.Create(nodePrefix, nil, zk.FlagEphemeralSequential, zk.WorldACL(zk.PermAll))
	if err != nil {
		return fmt.Errorf("failed to create node: %w", err)
	}
	globalLogger.Debug("Created node at path", zap.String("path", path))

	// Retry getting children with a small delay to handle timing issues
	var children []string
	for i := 0; i < 3; i++ {
		children, _, err = zkConn.Children(electionPath)
		if err != nil {
			return fmt.Errorf("failed to get children: %w", err)
		}

		if len(children) > 0 {
			break
		}

		globalLogger.Debug("No children found, retrying...", zap.Int("attempt", i+1))
		time.Sleep(100 * time.Millisecond)
	}

	if len(children) == 0 {
		return fmt.Errorf("no children found after retries")
	}

	sort.Strings(children)
	fmt.Printf("%s children: %v\n", electionPath, children)

	electedLeader := children[0]
	var pos = -1
	for i := 0; i < len(children); i++ {
		if strings.HasSuffix(path, children[i]) {
			pos = i
		}
	}
	if pos == -1 {
		return errors.New(" invalid state encountered ")
	}
	globalLogger.Debug("Node: ", zap.String("path", path), zap.Int("position", pos))
	// Extract just the node name from our created path for comparison
	// path is like "/rapidstore/election/node_0000000001"
	// We need just "node_0000000001" to compare with children[0]
	pathParts := strings.Split(path, "/")
	myNodeName := pathParts[len(pathParts)-1]
	s.config.election.NodeID = myNodeName
	globalLogger.Debug("Node ID set", zap.String("nodeID", myNodeName), zap.String("electedLeader", electedLeader))

	nodeAddr, err := getExternalIP()
	if err != nil {
		return fmt.Errorf("failed to get external IP: %w", err)
	}

	if myNodeName == electedLeader {
		globalLogger.Info("You were elected leader")
		s.config.election.isLeader = true
		// create /leader dir (if not exist) and place IP:port of self in it
		leaderData := []byte(fmt.Sprintf("%s:%d", nodeAddr, s.config.Port))
		exist, _, err = zkConn.Exists(leaderPath)
		if err != nil {
			return fmt.Errorf("failed to check if leader path exists: %w", err)
		}
		if exist {
			// delete it since we are the new leader
			_, err = zkConn.Set(leaderPath, leaderData, -1)
			if err != nil {
				return fmt.Errorf("failed to update leader path: %w", err)
			}
		} else {
			_, err = zkConn.Create(leaderPath, leaderData, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
			if err != nil {
				return fmt.Errorf("failed to create leader path: %w", err)
			}
		}
		s.config.election.followerInfo = NewFollowers(zkConn, followerPath)
		globalLogger.Info("Current followers", zap.Int("count", len(s.config.election.followerInfo)))
		for _, f := range s.config.election.followerInfo {
			globalLogger.Info("Follower", zap.String("address", f.address), zap.String("port", f.port))
		}
		go s.watchFollowers()
		go s.attemptSyncFollowers()
	} else {
		globalLogger.Debug("You are a follower", zap.String("leader", electedLeader))
		s.config.election.isLeader = false
		// Register as follower - create ephemeral sequential node under /followers
		followerData := []byte(fmt.Sprintf("%s:%d", nodeAddr, s.config.Port))

		// Ensure follower path exists first
		exist, _, err := zkConn.Exists(followerPath)
		if err != nil {
			return fmt.Errorf("failed to check if follower path exists: %w", err)
		}
		if !exist {
			_, err = zkConn.Create(followerPath, nil, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				return fmt.Errorf("failed to create follower path: %w", err)
			}
		}

		// Create ephemeral sequential node for this follower
		followerNodePath := fmt.Sprintf("%s/follower_", followerPath)
		followerNode, err := zkConn.Create(followerNodePath, followerData, zk.FlagEphemeralSequential, zk.WorldACL(zk.PermAll))
		if err != nil {
			return fmt.Errorf("failed to create follower node: %w", err)
		}

		globalLogger.Debug("Registered as follower", zap.String("followerNode", followerNode))
		if pos-1 >= 0 && pos-1 < len(children) {
			// watch the node just before us
			_, stat, predW, err := zkConn.GetW(electionPath + "/" + children[pos-1])
			if err != nil {
				if err == zk.ErrNoNode {
					return fmt.Errorf("node %s no longer exists, err %v", children[pos-1], err)
				}
				return fmt.Errorf("failed to set watch on predecessor node: %w", err)
			}
			// set leader info
			lconf, err := newLeader(zkConn)
			if err != nil {
				return fmt.Errorf("failed to set up leader connection in newElection: %w", err)
			}

			s.config.election.updateLeaderInfo(lconf.Address, lconf.Port, lconf.c)
			// watch the leader for events
			electedLeaderPath := fmt.Sprintf("%s/%s", electionPath, electedLeader)
			globalLogger.Debug("Elected leader path", zap.String("path", electedLeaderPath))

			_, _, leaderW, err := zkConn.GetW(electedLeaderPath)
			if err != nil {
				if err == zk.ErrNoNode {
					return fmt.Errorf("leader node %s no longer exists, err %v", electedLeader, err)
				}
				return fmt.Errorf("failed to set watch on leader node: %w", err)
			}
			globalLogger.Debug("Watching predecessor node", zap.String("predecessor", children[pos-1]), zap.Int32("version", stat.Version))
			s.config.election.zkPredecessorEvents = predW
			s.config.election.zkLeaderEvents = leaderW
			go s.watchZoo()
			// for now just return error but in the future need to do retrys TODO:
			err = s.attemptSync() // grab latest updates from leader
			if err != nil {
				return err
			}

		}
	}

	return nil
}

func (s *Server) newElection() error {
	globalLogger.Info("Starting new election process")

	zkConn := s.config.election.zkConn
	if zkConn == nil {
		panic("ZooKeeper connection is nil")
	}

	// Get all children and sort to find leader
	children, _, err := zkConn.Children(electionPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to get election children: %v", err))
	}

	if len(children) == 0 {
		panic("No children found in election")
	}

	sort.Strings(children)
	electedLeader := children[0]
	myNodeName := s.config.election.NodeID

	globalLogger.Info("Leader elected", zap.String("leader", electedLeader), zap.String("myNode", myNodeName))

	nodeAddr, err := getExternalIP()
	if err != nil {
		return fmt.Errorf("failed to get external IP: %w", err)
	}
	if myNodeName == electedLeader {
		//TODO: set up connection to followers
		globalLogger.Info("I am the new leader!")
		s.config.election.isLeader = true

		nodeData := fmt.Sprintf("%s:%d", nodeAddr, s.config.Port)
		// Remove this node from followers since it's now the leader
		err := s.removeFollowerNode(nodeData, followerPath)
		if err != nil {
			globalLogger.Error("error removing follower node", zap.Error(err))
			// continue, not a huge deal if we cant remove it
		}
		// Update leader path
		leaderData := []byte(fmt.Sprintf("%s:%d", nodeAddr, s.config.Port))
		exist, _, err := zkConn.Exists(leaderPath)
		if err != nil {
			panic(fmt.Sprintf("Failed to check leader path: %v", err))
		}

		if exist {
			_, err = zkConn.Set(leaderPath, leaderData, -1)
		} else {
			_, err = zkConn.Create(leaderPath, leaderData, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		}
		if err != nil {
			panic(fmt.Sprintf("Failed to update leader path: %v", err))
		}
		s.config.election.followerInfo = NewFollowers(zkConn, followerPath)
		globalLogger.Info("Current followers", zap.Int("count", len(s.config.election.followerInfo)))
		for _, f := range s.config.election.followerInfo {
			globalLogger.Info("Follower", zap.String("address", f.address), zap.String("port", f.port))
		}
		go s.watchFollowers()

	} else {
		globalLogger.Info("I am a follower!", zap.String("leader", electedLeader), zap.String("myNode", myNodeName))
		s.config.election.isLeader = false
		// get leader info and store

		lconf, err := newLeader(zkConn)
		if err != nil {
			return fmt.Errorf("failed to set up leader connection in newElection: %w", err)
		}

		s.config.election.updateLeaderInfo(lconf.Address, lconf.Port, lconf.c)
		s.dynamicMessage <- restartLeaderStream

		// Find my index and watch predecessor
		myIndex := -1
		for i, child := range children {
			if child == myNodeName {
				myIndex = i
				break
			}
		}
		if myIndex == -1 {
			panic(fmt.Sprintf("Could not find my node %s", myNodeName))
		}

		// Watch predecessor if not first
		if myIndex > 0 {
			predecessorPath := fmt.Sprintf("%s/%s", electionPath, children[myIndex-1])
			_, _, eventChan, err := zkConn.ExistsW(predecessorPath)
			if err != nil {
				panic(fmt.Sprintf("Failed to watch predecessor: %v", err))
			}
			s.config.election.zkPredecessorEvents = eventChan
		}

		// Watch leader
		_, _, leaderEventChan, err := zkConn.ExistsW(leaderPath)
		if err != nil {
			panic(fmt.Sprintf("Failed to watch leader: %v", err))
		}
		s.config.election.zkLeaderEvents = leaderEventChan
	}

	err = s.attemptSync() // grab latest updates from leader
	if err != nil {
		return err
	}
	return nil
}

// make a server log chan that logs are sent to. write this out there
func (s *Server) watchZoo() {
	// leader node doesnt have a predecessor to watch
	for {
		globalLogger.Debug("In watchZoo loop", zap.Bool("isLive", s.isLive), zap.Bool("isLeader", s.config.election.isLeader), zap.Float64("timeout duration seconds", s.config.election.Timeout.Seconds()))
		time.Sleep(s.config.election.Timeout)
		if !s.isLive || s.config.election.isLeader {
			break
		}
		// TODO: if we ever want to watch the leader for what ever reason
		select {
		case event := <-s.config.election.zkPredecessorEvents:
			globalLogger.Debug("Received event from predecessor", zap.String("eventType", event.Type.String()), zap.String("eventTypeRaw", fmt.Sprintf("%v", event.Type)))
			switch event.Type {
			case zk.EventNodeDeleted:
				globalLogger.Info("Predecessor deleted - starting new election")
				err := s.newElection()
				if err != nil {
					globalLogger.Error("error handling new election", zap.Error(err))
				}
			case zk.EventNotWatching:
				globalLogger.Info("Predecessor watch stopped - restarting watch")
				// Need to restart the watch
			default:
				globalLogger.Debug("Unhandled predecessor event type", zap.String("eventType", event.Type.String()), zap.String("eventTypeRaw", fmt.Sprintf("%v", event.Type)))
			}
		case event := <-s.config.election.zkLeaderEvents:
			globalLogger.Debug("Received event from leader", zap.String("eventType", event.Type.String()), zap.String("eventTypeRaw", fmt.Sprintf("%v", event.Type)))
			switch event.Type {
			case zk.EventNodeDeleted:
				globalLogger.Info("Leader deleted - starting new election")
				err := s.newElection()
				if err != nil {
					globalLogger.Error("error handling new election", zap.Error(err))
				}
			case zk.EventNodeDataChanged:
				globalLogger.Info("Leader data changed - updating leader info")
				// just re-read the leader info
				data, _, err := s.config.election.zkConn.Get(leaderPath)
				if err != nil {
					globalLogger.Error("error reading leader data", zap.Error(err))
					return
				}

				// Parse the data (format is "IP:PORT")
				leaderInfo := string(data)
				parts := strings.Split(leaderInfo, ":")
				if len(parts) != 2 {
					globalLogger.Debug("Invalid leader data format", zap.String("leaderInfo", leaderInfo))
					return
				}

				addr := parts[0]
				port := parts[1]
				globalLogger.Debug("Leader updated", zap.String("addr", addr), zap.String("port", port))
				s.config.election.updateLeaderInfo(addr, port)
				s.dynamicMessage <- restartLeaderStream
			case zk.EventNotWatching:
				globalLogger.Info("Leader watch stopped - restarting watch")
				// Need to restart the watch
			default:
				globalLogger.Debug("Unhandled leader event type", zap.String("eventType", event.Type.String()), zap.String("eventTypeRaw", fmt.Sprintf("%v", event.Type)))
			}
		}
	}
}

// only the leader watches followers
func (s *Server) watchFollowers() {
	ticker := time.NewTicker(time.Second * 3)

	_, _, followerW, _ := s.config.election.zkConn.ChildrenW(followerPath)
	for {
		select {
		case event := <-followerW:
			globalLogger.Debug("Received event from followers", zap.String("eventType", event.Type.String()), zap.String("eventTypeRaw", fmt.Sprintf("%v", event.Type)))
			switch event.Type {
			case zk.EventNodeChildrenChanged:
				globalLogger.Info("Follower list changed - updating followers")
				s.config.election.followerInfo = NewFollowers(s.config.election.zkConn, followerPath)
				globalLogger.Info("Current followers", zap.Int("count", len(s.config.election.followerInfo)))
				for _, f := range s.config.election.followerInfo {
					globalLogger.Info("Follower", zap.String("address", f.address), zap.String("port", f.port))
				}
			case zk.EventNotWatching:
				globalLogger.Info("Follower watch stopped - restarting watch")
				// Need to restart the watch
			default:
				globalLogger.Debug("Unhandled follower event type", zap.String("eventType", event.Type.String()), zap.String("eventTypeRaw", fmt.Sprintf("%v", event.Type)))
			}
			_, _, followerChan, _ := s.config.election.zkConn.ChildrenW(followerPath)
			followerW = followerChan // keep this going forever
		case <-ticker.C:
			if !s.isLive || !s.config.election.isLeader {
				ticker.Stop()
				return
			}
			// just a keep alive to check if we should still be watching
			globalLogger.Debug("Watching followers...", zap.Int("followerCount", len(s.config.election.followerInfo)))
		}
	}
}

func (s *Server) removeFollowerNode(nodeData string, path string) error {
	zkConn := s.config.election.zkConn
	if zkConn == nil {
		return errors.New("ZooKeeper connection is nil")
	}

	children, _, err := zkConn.Children(path)
	if err != nil {
		return fmt.Errorf("failed to get follower children: %w", err)
	}

	for _, child := range children {
		childPath := fmt.Sprintf("%s/%s", path, child)
		data, _, err := zkConn.Get(childPath)
		if err != nil {
			if err == zk.ErrNoNode {
				continue // Node might have been deleted already
			}
			return fmt.Errorf("failed to get follower node data: %w", err)
		}

		if string(data) == nodeData {
			err = zkConn.Delete(childPath, -1)
			if err != nil {
				if err == zk.ErrNoNode {
					return nil // Node already deleted
				}
				return fmt.Errorf("failed to delete follower node: %w", err)
			}
			globalLogger.Info("Removed follower node", zap.String("nodePath", childPath))
			return nil
		}
	}

	return fmt.Errorf("follower node with data %s not found", nodeData)
}
func (s *Server) attemptSyncFollowers() {
	if !s.config.election.isLeader {
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.close:
			globalLogger.Debug("Stopping follower sync attempts")
			return
		case <-ticker.C:
			for i := range s.config.election.followerInfo {
				go func(f *followerInfo) {
					// Only attempt connection if we don't have one
					if f.c == nil {
						c, err := net.Dial("tcp", fmt.Sprintf("%s:%s", f.address, f.port))
						if err != nil {
							globalLogger.Warn("Failed to connect to follower",
								zap.String("address", f.address),
								zap.String("port", f.port),
								zap.Error(err))
							return
						}

						globalLogger.Info("Successfully connected to follower",
							zap.String("address", f.address),
							zap.String("port", f.port))
						f.c = c
					}
				}(&s.config.election.followerInfo[i])
			}
		}
	}
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
			// TODO: Update this later
			return "0.0.0.0", nil
		}
	}

	return "", fmt.Errorf("failed to get external IP from any service")
}
func NewFollowers(zkConn *zk.Conn, path string) []followerInfo {
	var resultFollowerInfo []followerInfo
	// check if theres a follower path
	exist, _, err := zkConn.Exists(path)
	if err != nil {
		globalLogger.Error("error checking follower path", zap.Error(err))
		return nil // no follower path, nothing to do
	}
	if exist {
		followers, _, err := zkConn.Children(path)
		if err != nil {
			globalLogger.Error("error getting follower children", zap.Error(err))
			return nil
		}
		for _, f := range followers {
			// Get the data for this follower node
			followerNodePath := fmt.Sprintf("%s/%s", followerPath, f)
			data, _, err := zkConn.Get(followerNodePath)
			if err != nil {
				globalLogger.Error("failed to get follower data", zap.String("follower", f), zap.Error(err))
				continue
			}

			// Parse IP:PORT format
			parts := strings.Split(string(data), ":")
			if len(parts) != 2 {
				globalLogger.Error("invalid follower data format", zap.String("data", string(data)))
				continue
			}

			// interserver comms happens on port + 1
			// so if follower is on 8000 we connect to it on 8001
			// this is to keep client and interserver comms separate
			ip := parts[0]
			strPort := parts[1]
			intPort, err := strconv.Atoi(strPort)
			if err != nil {
				globalLogger.Error("invalid port in follower data", zap.String("port", strPort), zap.Error(err))
				continue
			}
			intPort++
			port := strconv.Itoa(intPort)

			globalLogger.Info("Found follower", zap.String("ip", ip), zap.String("port", port))
			conn, err := NewConnectionToNode(ip, port)
			if err != nil {
				globalLogger.Info("Failed to connect to follower", zap.String("ip", ip), zap.String("port", port), zap.Error(err))
			}
			if conn != nil {
				globalLogger.Info("Successfully connected to follower", zap.String("ip", ip), zap.String("port", port))
			} else {
				globalLogger.Info("Failed to connect to follower after 3 attempts", zap.String("ip", ip), zap.String("port", port))
			}
			resultFollowerInfo = append(resultFollowerInfo, followerInfo{
				address: ip,
				port:    port,
				c:       conn,
			})
		}
	}
	return resultFollowerInfo
}
func newLeader(zkConn *zk.Conn) (leaderInfo, error) {
	leaderData, _, err := zkConn.Get(leaderPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to get leader data: %v", err))
	}

	// Parse the data (format is "IP:PORT")
	leaderInfostr := string(leaderData)
	parts := strings.Split(leaderInfostr, ":")
	if len(parts) != 2 {
		panic(fmt.Sprintf("Invalid leader data format: %s", leaderInfostr))
	}

	addr := parts[0]
	strPort := parts[1]
	intPort, err := strconv.Atoi(strPort)
	if err != nil {
		globalLogger.Error("invalid port in follower data", zap.String("port", strPort), zap.Error(err))
		return leaderInfo{}, err
	}
	intPort++
	port := strconv.Itoa(intPort)
	c, err := NewConnectionToNode(addr, port)
	if err != nil {
		return leaderInfo{}, fmt.Errorf("failed to connect to leader at %s:%s: %w", addr, port, err)
	}
	return leaderInfo{
		Address: addr,
		Port:    port,
		c:       c,
	}, nil
}
func NewConnectionToNode(Addr string, Port string) (net.Conn, error) {
	address := fmt.Sprintf("%s:%s", Addr, Port)
	conn, err := net.Dial("tcp4", address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to leader at %s: %w", address, err)
	}
	return conn, nil
}
