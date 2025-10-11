package server

import (
	"fmt"
	"net"
	"strings"
	"time"

	"go.uber.org/zap"
)

type baseCommands string

const (
	// server commands
	ServerPrefix baseCommands = "SS" // RapidStore command prefix
	PING         baseCommands = "PING"
	ECHO         baseCommands = "ECHO"
	Close        baseCommands = "CLOSE"
	// key commands
	SET    baseCommands = "SET"
	GET    baseCommands = "GET"
	GetAll baseCommands = "GETALL"
	Del    baseCommands = "DEL"
	TTL    baseCommands = "TTL"
	Exists baseCommands = "EXISTS"
	Type   baseCommands = "TYPE"
	// Data structure commands
	// Hash commands
	HSet    baseCommands = "HSET"
	HGet    baseCommands = "HGET"
	HGetAll baseCommands = "HGETALL"
	HDel    baseCommands = "HDEL"
	HExists baseCommands = "HEXISTS"
	// List commands
	LPush  baseCommands = "LPUSH"
	RPush  baseCommands = "RPUSH"
	LPop   baseCommands = "LPOP"
	RPop   baseCommands = "RPOP"
	LRange baseCommands = "LRANGE"
	// Set commands
	SAdd      baseCommands = "SADD"
	SMembers  baseCommands = "SMEMBERS"
	SRem      baseCommands = "SREM"
	SIsMember baseCommands = "SISMEMBER"
	SCARD     baseCommands = "SCARD"
	// Sorted Set commands
	ZAdd      baseCommands = "ZADD"
	Zrange    baseCommands = "ZRANGE"
	Zrevrange baseCommands = "ZREVRANGE"
	Zrank     baseCommands = "ZRANK"
	Zscore    baseCommands = "ZSCORE"
)
const (
	Successfull = "Successfull"
	Failed      = "Failed"
)

// Handle individual client connection
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	defer s.productionStats.DecrementActiveConnections()
	// Handle client connection
	globalLogger.Info("Accepted connection from", zap.String("remoteAddr", conn.RemoteAddr().String()))

	// Keep reading commands until connection is closed
	for {
		buff := make([]byte, 1024)
		n, err := conn.Read(buff)
		if err != nil {
			globalLogger.Warn("(cache Server) Connection closed or error reading", zap.Error(err))
			return
		}
		//TODO: space in command can cause errors. fix this
		parts := strings.Split(strings.TrimSpace(string(buff[:n])), " ")
		base := parts[0]

		switch strings.ToUpper(base) {
		case string(SET):
			globalLogger.Info("SET command received", zap.String("command", string(buff[:n])))
			if len(parts) < 3 {
				globalLogger.Warn("Invalid SET command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid SET command format\n"))
				continue
			}
			key := parts[1]
			value := parts[2]
			// check if ttl is provided

			if len(parts) < 4 {
				s.ramCache.SetKey(key, value)
				conn.Write([]byte(Successfull + "\n"))
				continue
			}
			// parse ttl
			var ttl int
			_, err := fmt.Sscanf(parts[3], "%d", &ttl)
			if err != nil || ttl < 0 {
				globalLogger.Warn("Invalid TTL value", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid TTL value\n"))
				continue
			}
			s.ramCache.SetKey(key, value, time.Second*time.Duration(ttl))
			conn.Write([]byte(Successfull + "\n"))
		case string(GET):
			globalLogger.Info("GET command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid GET command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid GET command format\n"))
				continue
			}
			key := parts[1]
			v := s.ramCache.GetKey(key)
			conn.Write([]byte(fmt.Sprintf("%v\n", v)))
		case string(GetAll):
			globalLogger.Info("GETALL command received", zap.String("command", string(buff[:n])))
			keys := s.ramCache.Keys()
			conn.Write([]byte(strings.Join(keys, ",") + "\n"))
		case string(Del):
			globalLogger.Info("DEL command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid DEL command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid DEL command format\n"))
				continue
			}
			key := parts[1]
			s.ramCache.DeleteKey(key)
			conn.Write([]byte(Successfull + "\n"))
		case string(TTL):
			globalLogger.Info("TTL command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid TTL command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid TTL command format\n"))
				continue
			}
			key := parts[1]
			ttl, err := s.ramCache.TTLKey(key)
			if err != nil {
				conn.Write([]byte("Error: " + err.Error() + "\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("%d\n", ttl)))
		case string(Exists):
			globalLogger.Info("EXISTS command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid EXISTS command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid EXISTS command format\n"))
				continue
			}
			key := parts[1]
			exists := s.ramCache.ExistsKey(key)
			if exists {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
		case string(Type):
			globalLogger.Info("TYPE command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid TYPE command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid TYPE command format\n"))
				continue
			}
			key := parts[1]
			typ := s.ramCache.Type(key)
			conn.Write([]byte(fmt.Sprintf("%s\n", typ)))
		case string(HSet):
			globalLogger.Info("HSET command received", zap.String("command", string(buff[:n])))
		case string(HGet):
			globalLogger.Info("HGET command received", zap.String("command", string(buff[:n])))
		case string(HGetAll):
			globalLogger.Info("HGETALL command received", zap.String("command", string(buff[:n])))
		case string(HDel):
			globalLogger.Info("HDEL command received", zap.String("command", string(buff[:n])))
		case string(HExists):
			globalLogger.Info("HEXISTS command received", zap.String("command", string(buff[:n])))
		case string(LPush):
			globalLogger.Info("LPUSH command received", zap.String("command", string(buff[:n])))
		case string(RPush):
			globalLogger.Info("RPUSH command received", zap.String("command", string(buff[:n])))
		case string(LPop):
			globalLogger.Info("LPOP command received", zap.String("command", string(buff[:n])))
		case string(RPop):
			globalLogger.Info("RPOP command received", zap.String("command", string(buff[:n])))

		case string(LRange):
			globalLogger.Info("LRANGE command received", zap.String("command", string(buff[:n])))
		case string(SAdd):
			globalLogger.Info("SADD command received", zap.String("command", string(buff[:n])))
		case string(SMembers):
			globalLogger.Info("SMEMBERS command received", zap.String("command", string(buff[:n])))
		case string(SRem):
			globalLogger.Info("SREM command received", zap.String("command", string(buff[:n])))
		case string(SIsMember):
			globalLogger.Info("SISMEMBER command received", zap.String("command", string(buff[:n])))
		case string(SCARD):
			globalLogger.Info("SCARD command received", zap.String("command", string(buff[:n])))
		case string(ZAdd):
			globalLogger.Info("ZADD command received", zap.String("command", string(buff[:n])))
		case string(Zrange):
			globalLogger.Info("ZRANGE command received", zap.String("command", string(buff[:n])))
		case string(Zrevrange):
			globalLogger.Info("ZREVRANGE command received", zap.String("command", string(buff[:n])))
		case string(Zrank):
			globalLogger.Info("ZRANK command received", zap.String("command", string(buff[:n])))
		case string(Zscore):
			globalLogger.Info("ZSCORE command received", zap.String("command", string(buff[:n])))
		case string(ServerPrefix):
			// there valid cases Ping, Echo, Close
			if len(parts) < 2 {
				globalLogger.Warn("Invalid command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid command format\n"))
				continue
			}
			subCommand := strings.ToUpper(parts[1])
			switch subCommand {
			case string(PING):
				globalLogger.Info("PING command received", zap.String("command", string(buff[:n])))
				conn.Write([]byte("PONG\n"))
			case string(ECHO):
				globalLogger.Info("ECHO command received", zap.String("command", string(buff[:n])))
				// Echo back the message
				if len(parts) < 3 {
					conn.Write([]byte("Error: ECHO command requires a message\n"))
					continue
				}
				message := strings.Join(parts[2:], " ")
				conn.Write([]byte(message + "\n"))
			case string(Close):
				globalLogger.Info("CLOSE command received, closing connection", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Connection closing...\n"))
				s.Stop()
				return
			default:
				globalLogger.Warn("Unknown server sub-command", zap.String("subCommand", subCommand))
				conn.Write([]byte("Error: Unknown server sub-command\n"))
			}
		default:
			globalLogger.Warn("Unknown command", zap.String("command", base))
			conn.Write([]byte("Error: Unknown command\n"))
			continue
		}

	}
}
