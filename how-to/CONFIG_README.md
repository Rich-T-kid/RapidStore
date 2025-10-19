# RapidStore Configuration Guide

RapidStore supports configuration through both JSON and YAML files. This guide explains the purpose and available configuration options for setting up your RapidStore server.

## Configuration File Formats

RapidStore automatically detects the configuration format based on the file extension:
- `.json` files are parsed as JSON
- `.yaml` and `.yml` files are parsed as YAML

## Configuration Structure

The configuration is organized into four main sections:

### 1. Server Configuration (`serverConfig`)

Core server settings that control basic network and connection behavior.

```json
"serverConfig": {
    "address": "0.0.0.0",
    "port": 6379,
    "health_check_port": 8080,
    "max_clients": 1000,
    "timeout": "30s",
    "idle_timeout": "300s"
}
```

**Fields:**
- `address` (string): IP address the server binds to
  - `"0.0.0.0"` - Listen on all interfaces
  - `"127.0.0.1"` - Listen only on localhost
  - `"192.168.1.100"` - Listen on specific IP
- `port` (integer): Main server port for client connections (default: 6379)
- `health_check_port` (integer): Port for health check endpoint (default: 8080)
- `max_clients` (integer): Maximum number of concurrent client connections (default: 1000)
- `timeout` (duration string): Client connection timeout (e.g., "30s", "5m")
- `idle_timeout` (duration string): How long to keep idle connections open (e.g., "300s", "10m")

### 2. Persistence Configuration (`PersistenceConfig`)

Settings for Write-Ahead Logging (WAL), data persistence, and replication consistency.

```json
"PersistenceConfig": {
    "consistancy_type": "Strong",
    "replication_timeout": "2s",
    "wal_sync_interval": "1s",
    "wal_path": "./wal.log",
    "wal_max_size": 10485760
}
```

**Fields:**
- `consistancy_type` (string): Data consistency level for replication
  - `"Strong"` - All replicas must acknowledge writes (highest consistency, lower performance)
  - `"Quorum"` - Majority of replicas must acknowledge writes (balanced consistency/performance)  
  - `"Eventual"` - Write to leader, replicas sync eventually (highest performance, eventual consistency)
  - `"Minimal"` - Write to leader only (fastest, least consistent)
- `replication_timeout` (duration string): Maximum time to wait for replica acknowledgments (e.g., "2s", "500ms")
  - Lower values = faster timeout detection, may cause false timeouts under load
  - Higher values = more tolerance for slow networks, slower failure detection
- `wal_sync_interval` (duration string): How often to sync WAL to disk (e.g., "1s", "500ms")
  - Lower values = better durability, higher I/O overhead
  - Higher values = better performance, potential data loss on crash
- `wal_path` (string): File system path where WAL file is stored
- `wal_max_size` (integer): Maximum WAL file size in bytes before rotation
  - Example: `10485760` = 10MB, `20971520` = 20MB

### 3. Monitoring Configuration (`MoniteringConfig`)

Settings for metrics collection, monitoring endpoints, and logging.

```json
"MoniteringConfig": {
    "metrics_port": 9090,
    "metrics_path": "/metrics",
    "metrics_interval": "750ms",
    "log_file": "./rapidstore.log"
}
```

**Fields:**
- `metrics_port` (integer): Port for Prometheus-style metrics endpoint (default: 9090)
- `metrics_path` (string): HTTP path for metrics endpoint (default: "/metrics")
- `metrics_interval` (duration string): How often to collect/update metrics
  - Lower values = more responsive monitoring, higher CPU usage
  - Higher values = less overhead, less frequent updates
- `log_file` (string): Path to application log file

### 4. Election Configuration (`ElectionConfig`)

Settings for leader election, distributed coordination using Apache Zookeeper, and cluster heartbeat behavior.

```json
"ElectionConfig": {
    "zookeeper_servers": ["localhost:2181"],
    "election_path": "/rapidstore/leader",
    "node_id": "node-1",
    "election_timeout": "5s",
    "heartbeat_interval": "1s"
}
```

**Fields:**
- `zookeeper_servers` (array of strings): List of Zookeeper server addresses
  - Format: `["host1:port1", "host2:port2", ...]`
  - Multiple servers provide failover capability
- `election_path` (string): Zookeeper path used for leader election
  - Should be unique per RapidStore cluster
  - Example: `"/production/rapidstore/leader"`
- `node_id` (string): Unique identifier for this server instance
  - Must be unique across all nodes in the cluster
  - Used for logging and cluster identification
- `election_timeout` (duration string): Timeout for leader election process
  - Lower values = faster leader election, more sensitive to network delays
  - Higher values = more stable in network partitions, slower failover
- `heartbeat_interval` (duration string): How often the leader sends heartbeats to followers
  - Lower values = faster failure detection, more network traffic
  - Higher values = less network overhead, slower failure detection

## Example Configurations

### Development Configuration (JSON)
```json
{
    "serverConfig": {
        "address": "127.0.0.1",
        "port": 6379,
        "health_check_port": 8080,
        "max_clients": 100,
        "timeout": "30s",
        "idle_timeout": "300s"
    },
    "PersistenceConfig": {
        "consistancy_type": "Eventual",
        "replication_timeout": "2s",
        "wal_sync_interval": "1s",
        "wal_path": "./dev_wal.log",
        "wal_max_size": 5242880
    },
    "MoniteringConfig": {
        "metrics_port": 9090,
        "metrics_path": "/metrics",
        "metrics_interval": "1s",
        "log_file": "./dev.log"
    },
    "ElectionConfig": {
        "zookeeper_servers": ["localhost:2181"],
        "election_path": "/dev/rapidstore/leader",
        "node_id": "dev-node-1",
        "election_timeout": "5s",
        "heartbeat_interval": "1s"
    }
}
```

### Production Configuration (YAML)
```yaml
serverConfig:
  address: "0.0.0.0"
  port: 6379
  health_check_port: 8080
  max_clients: 5000
  timeout: "60s"
  idle_timeout: "900s"

PersistenceConfig:
  consistancy_type: "Strong"
  replication_timeout: "1s"
  wal_sync_interval: "500ms"
  wal_path: "/var/lib/rapidstore/wal.log"
  wal_max_size: 104857600  # 100MB

MoniteringConfig:
  metrics_port: 9090
  metrics_path: "/metrics"
  metrics_interval: "500ms"
  log_file: "/var/log/rapidstore/rapidstore.log"

ElectionConfig:
  zookeeper_servers:
    - "zk1.prod.example.com:2181"
    - "zk2.prod.example.com:2181"
    - "zk3.prod.example.com:2181"
  election_path: "/production/rapidstore/leader"
  node_id: "prod-node-01"
  election_timeout: "10s"
  heartbeat_interval: "2s"
```

## Consistency Types Explained

The `consistancy_type` field in `PersistenceConfig` controls how RapidStore handles data replication across the cluster. Each type offers different trade-offs between consistency, availability, and performance:

### Strong Consistency
- **Description**: All replica nodes must acknowledge writes before the operation is considered complete
- **Use Case**: Financial systems, critical data where consistency is paramount
- **Trade-offs**: 
  - ✅ Highest data consistency guarantee
  - ✅ No risk of reading stale data
  - ❌ Slower write performance
  - ❌ Lower availability if replica nodes are down
- **Example**: Banking transactions, inventory management

### Quorum Consistency  
- **Description**: Majority of replica nodes (>50%) must acknowledge writes
- **Use Case**: Balanced systems requiring good consistency with acceptable performance
- **Trade-offs**:
  - ✅ Good consistency guarantees
  - ✅ Better performance than Strong consistency
  - ✅ Can tolerate some replica failures
  - ❌ Possibility of brief inconsistency during network partitions
- **Example**: User profiles, configuration data, e-commerce catalogs

### Eventual Consistency
- **Description**: Writes go to leader immediately, replicas are updated asynchronously
- **Use Case**: High-throughput systems where temporary inconsistency is acceptable
- **Trade-offs**:
  - ✅ High write performance
  - ✅ High availability
  - ✅ Handles network partitions well
  - ❌ Temporary data inconsistency possible
  - ❌ Risk of reading stale data from replicas
- **Example**: Social media feeds, analytics data, caching layers

### Minimal Consistency
- **Description**: Writes only to leader node, no replica synchronization
- **Use Case**: Maximum performance scenarios, development/testing
- **Trade-offs**:
  - ✅ Fastest write performance
  - ✅ No dependency on replica nodes
  - ❌ No data redundancy
  - ❌ Data loss risk if leader fails
  - ❌ Not recommended for production
- **Example**: Development environments, temporary caches, performance testing

## Duration Format

Duration values use Go's duration format:
- `"1s"` = 1 second
- `"500ms"` = 500 milliseconds
- `"5m"` = 5 minutes
- `"1h30m"` = 1 hour 30 minutes
- `"2h45m30s"` = 2 hours 45 minutes 30 seconds

## Usage

RapidStore supports multiple ways to specify configuration:

### Command Line Configuration File
To start RapidStore with a configuration file:

```bash
# Using JSON config
rapidstore --config=/path/to/config.json

# Using YAML config  
rapidstore --config=/path/to/config.yaml

# Using relative paths
rapidstore --config=./configs/development.json
rapidstore --config=../shared/production.yaml
```

### Command Line Arguments
You can also override specific configuration values via command line flags:

```bash
# Override port and address
rapidstore --config=config.json --port=8080 --address=0.0.0.0

# Override consistency type
rapidstore --config=config.yaml --consistency=Strong

# Override WAL settings
rapidstore --config=config.json --wal-path=/custom/wal.log --wal-sync-interval=2s
```

### Configuration Precedence
Configuration values are applied in the following order (later values override earlier ones):

1. **Default values** (built into the application)
2. **Configuration file** (JSON or YAML)
3. **Command line flags** (highest priority)

### Automatic Defaults
If no configuration file is specified, RapidStore will use sensible defaults:

```bash
# Uses all default values
rapidstore

# Uses defaults with CLI overrides
rapidstore --port=7000 --consistency=Eventual
```

The server will automatically detect the file format and parse accordingly.

## Configuration Validation

RapidStore validates all configuration values at startup:
- Network ports must be valid (1-65535)
- Duration strings must be parseable
- File paths are checked for writeability
- Zookeeper servers are validated for connectivity

Invalid configurations will cause the server to exit with an error message explaining the issue.
