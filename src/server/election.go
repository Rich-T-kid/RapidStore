package server

import (
	"errors"
	"fmt"
	"sort"
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
	zkConn, _, err := zk.Connect(s.config.election.ZookeeperServers, s.config.election.Timeout)
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
	fmt.Printf("node: %s is at position %d\n", path, pos)
	// Extract just the node name from our created path for comparison
	// path is like "/rapidstore/election/node_0000000001"
	// We need just "node_0000000001" to compare with children[0]
	pathParts := strings.Split(path, "/")
	myNodeName := pathParts[len(pathParts)-1]
	s.config.election.NodeID = myNodeName
	globalLogger.Debug("Node ID set", zap.String("nodeID", myNodeName), zap.String("electedLeader", electedLeader))

	if myNodeName == electedLeader {
		globalLogger.Info("You were elected leader")
		s.config.election.isLeader = true
		// create /leader dir (if not exist) and place IP:port of self in it
		leaderData := []byte(fmt.Sprintf("%s:%d", s.config.Address, s.config.Port))
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
		// nodes are ephemeral so no need to delete
	} else {
		globalLogger.Debug("You are a follower", zap.String("leader", electedLeader))
		s.config.election.isLeader = false
		// Register as follower - create ephemeral sequential node under /followers
		followerData := []byte(fmt.Sprintf("%s:%d", s.config.Address, s.config.Port))

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

	if myNodeName == electedLeader {
		globalLogger.Info("I am the new leader!")
		s.config.election.isLeader = true

		// Update leader path
		leaderData := []byte(fmt.Sprintf("%s:%d", s.config.Address, s.config.Port))
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

	} else {
		globalLogger.Info("I am a follower!", zap.String("leader", electedLeader), zap.String("myNode", myNodeName))
		s.config.election.isLeader = false

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

	return nil
}

// make a server log chan that logs are sent to. write this out there
func (s *Server) watchZoo() {
	// leader node doesnt have a predecessor to watch
	for {
		globalLogger.Debug("In watchZoo loop", zap.Bool("isLive", s.isLive), zap.Bool("isLeader", s.config.election.isLeader))
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

		default:
			globalLogger.Info("No events received, continuing...")
		}
	}
}
