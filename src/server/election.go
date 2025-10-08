package server

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
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
	zkConn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second)
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
	fmt.Printf("Created node at path: %s\n", path)

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

		fmt.Printf("No children found, retrying... (attempt %d/5)\n", i+1)
		time.Sleep(100 * time.Millisecond)
	}

	if len(children) == 0 {
		return fmt.Errorf("no children found after retries")
	}

	sort.Strings(children)
	fmt.Printf("%s children: %v\n", electionPath, children)

	electedLeader := children[0]

	// Extract just the node name from our created path for comparison
	// path is like "/rapidstore/election/node_0000000001"
	// We need just "node_0000000001" to compare with children[0]
	pathParts := strings.Split(path, "/")
	myNodeName := pathParts[len(pathParts)-1]

	fmt.Printf("My node name: %s, Leader node name: %s\n", myNodeName, electedLeader)

	if myNodeName == electedLeader {
		fmt.Printf("You were elected leader\n")
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
		fmt.Printf("You are a follower. Leader is: %s\n", electedLeader)
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

		fmt.Printf("Registered as follower at: %s\n", followerNode)
	}

	return nil
}

func (s *Server) NewElection() error {
	// Implementation to start a new election
	return nil
}
