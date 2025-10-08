# RapidStore Leader Election Guide

## What is Leader Election?

In a distributed system with multiple servers, **only one server should be the "leader"** at any time. The leader handles critical operations like coordinating writes, managing cluster state, and making decisions for the entire system. All other servers are "followers" that receive instructions from the leader.

**Why do we need this?** Without a single leader, multiple servers might try to make conflicting decisions simultaneously, leading to data corruption or inconsistent state.

## How ZooKeeper Enables Leader Election

**ZooKeeper** is a coordination service that helps distributed applications agree on things. Think of it as a shared bulletin board where all servers can post messages and watch for changes.

Key ZooKeeper concepts we use:
- **Ephemeral Sequential Nodes**: Temporary numbered entries that disappear when a server disconnects
- **Watchers**: Notifications when something changes
- **Atomic Operations**: Changes that either completely succeed or completely fail

## RapidStore Leader Election Sequence

### 1. JOIN PHASE
```
Server starts up → Connects to ZooKeeper cluster
```

**What happens:**
- Server establishes connection to ZooKeeper (typically 3-5 ZooKeeper servers for reliability)
- Creates persistent directory structure if it doesn't exist:
  - `/rapidstore/election/` (where election happens)
  - `/rapidstore/leader/` (stores current leader info)
  - `/rapidstore/follower/` (tracks active followers)

**Key point:** Multiple servers can join simultaneously - this is normal and expected.

### 2. CREATE NODE PHASE
```
Server joins election → Creates ephemeral sequential node
```

**What happens:**
- Server creates a node like `/rapidstore/election/node_0000000001`
- ZooKeeper automatically assigns sequential numbers (000000001, 000000002, etc.)
- Node is "ephemeral" = automatically deleted if server disconnects
- Node stores server's IP:PORT for identification

**Example:**
```
/rapidstore/election/
  ├── node_0000000001  (Server A: 192.168.1.10:6380)
  ├── node_0000000002  (Server B: 192.168.1.11:6380)
  └── node_0000000003  (Server C: 192.168.1.12:6380)
```

### 3. ELECT PHASE
```
All servers examine nodes → Lowest number becomes leader
```

**What happens:**
- Each server lists all nodes in `/rapidstore/election/`
- Sorts nodes by number (lexicographical sort: node_0000000001 comes first)
- Server with the **lowest number** becomes the leader
- All other servers become followers

**Why this works:**
- All servers see the same sorted list (ZooKeeper guarantees consistency)
- Only one server can have the lowest number
- If multiple servers join simultaneously, ZooKeeper's sequential numbering prevents ties

**Example outcome:**
- Server A (node_0000000001) LEADER
- Server B (node_0000000002) Follower
- Server C (node_0000000003) Follower

### 4. PROMOTE PHASE
```
Leader updates cluster state → Followers set up monitoring
```

**Leader actions:**
- Creates/updates `/rapidstore/leader` with its IP:PORT
- Starts accepting write requests and making cluster decisions
- Begins leader-specific operations (data coordination, etc.)

**Follower actions:**
- Watch the node immediately before them in the sequence (predecessor watching)
  - Server B watches node_0000000001 (the leader)
  - Server C watches node_0000000002 (Server B)
- Watch `/rapidstore/leader` for leader changes
- Register themselves in `/rapidstore/follower/`
- Redirect write requests to the leader

**Why predecessor watching?** This creates an efficient chain reaction when failures occur, rather than all followers constantly watching the leader.

### 5. CLEANUP PHASE
```
Failed servers automatically removed → New election triggered
```

**Normal operation:**
- Leader processes requests, followers stay synchronized
- All servers maintain heartbeats with ZooKeeper

**When a server fails:**
- ZooKeeper detects disconnection (network timeout, server crash, etc.)
- Automatically deletes the failed server's ephemeral node
- Watchers notify the next server in the chain
- New election automatically triggered

**Example failure scenario:**
1. Server B (node_0000000002) crashes
2. ZooKeeper deletes node_0000000002
3. Server C was watching Server B gets deletion notification
4. Server C triggers new election examines remaining nodes
5. Server A still has lowest number remains leader
6. Server C now watches Server A directly

## Reconnect and Recovery Behavior

### Network Partitions
**Problem:** Server temporarily loses connection to ZooKeeper
**Solution:**
- ZooKeeper session has timeout period (default: 4 seconds in our system)
- If server reconnects within timeout keeps its election node
- If timeout expires node deleted, server must rejoin election

### Split-Brain Prevention
**Problem:** Network partition could create multiple leaders
**Solution:**
- ZooKeeper cluster requires majority (quorum) to operate
- With 3 ZooKeeper servers need 2 connected to make decisions
- Partition with minority of ZooKeeper servers cannot elect leaders
- Only one partition can have a functioning ZooKeeper only one leader possible

### Leader Failure Recovery
**Automatic re-election process:**
1. Leader crashes ephemeral node deleted
2. All followers detect leader deletion via watchers
3. Remaining servers re-examine election nodes
4. Server with new lowest number becomes leader
5. New leader updates `/rapidstore/leader` path
6. Followers redirect to new leader

**Timing:** Typically completes in 1-5 seconds depending on network conditions

### Follower Recovery
**Server rejoining after failure:**
1. Reconnects to ZooKeeper
2. Creates new election node (gets new sequential number)
3. Examines existing nodes to determine current leader
4. Sets up appropriate watches based on its position
5. Synchronizes with current leader to catch up on missed changes

## Key Guarantees

1. **Exactly One Leader:** ZooKeeper's consistency guarantees ensure only one server can have the lowest sequential number
2. **Automatic Failover:** Leader failures trigger immediate re-election without human intervention
3. **No Split-Brain:** ZooKeeper's quorum requirement prevents multiple concurrent leaders
4. **Self-Healing:** Failed servers can rejoin and automatically find their place in the hierarchy

## Monitoring and Observability

**Health Checks:**
- Each server exposes health endpoint showing leader/follower status
- ZooKeeper paths can be monitored to see cluster topology
- Election events are logged for debugging

**Example monitoring queries:**
- List all participants: `ls /rapidstore/election`
- Check current leader: `get /rapidstore/leader`
- See active followers: `ls /rapidstore/follower`

This system provides robust, automatic leader election suitable for production distributed systems with minimal configuration required.
