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
	ZAdd     baseCommands = "ZADD"
	Zremove  baseCommands = "ZREMOVE"
	Zrange   baseCommands = "ZRANGE"
	Zrank    baseCommands = "ZRANK"
	Zrevrank baseCommands = "ZREVRANK"
	Zscore   baseCommands = "ZSCORE"
)
const (
	// Change to exist/not found TODO:
	Successfull = "Successfull"
	Failed      = "Failed"
)

var (
	// Define a set of valid commands for quick lookup
	validCommands = newValidCMD()
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
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid SET command format \n correct usage: %s\n", validCommands.Set)))
				continue
			}
			key := parts[1]
			// TODO: need to parse this as the actuall type. right now its just a string
			// reflection?
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
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid GET command format \n correct usage: %s\n", validCommands.Get)))
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
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid DEL command format \n correct usage: %s\n", validCommands.Del)))
				continue
			}
			key := parts[1]
			s.ramCache.DeleteKey(key)
			conn.Write([]byte(Successfull + "\n"))
		case string(TTL):
			globalLogger.Info("TTL command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid TTL command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid TTL command format \n correct usage: %s\n", validCommands.TTL)))
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
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid EXISTS command format \n correct usage: %s\n", validCommands.Exists)))
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
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid TYPE command format \n correct usage: %s\n", validCommands.Type)))
				continue
			}
			key := parts[1]
			typ := s.ramCache.Type(key)
			conn.Write([]byte(fmt.Sprintf("%s\n", typ)))
		case string(HSet):
			globalLogger.Info("HSET command received", zap.String("command", string(buff[:n])))
			if len(parts) < 4 {
				globalLogger.Warn("Invalid HSET command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid HSET command format \n correct usage: %s\n", validCommands.HSet)))
				continue
			}
			key := parts[1]
			field := parts[2]
			value := parts[3] // same issue as before, need to get the real data type here for now its fine TODO:
			if len(parts) < 5 {
				s.ramCache.HSet(key, field, value)
				conn.Write([]byte(Successfull + "\n"))
				continue
			}
			var ttl int
			_, err := fmt.Sscanf(parts[4], "%d", &ttl)
			if err != nil || ttl < 0 {
				globalLogger.Warn("Invalid TTL value", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid TTL value\n"))
				continue
			}
			s.ramCache.HSet(key, field, value, time.Second*time.Duration(ttl))
			conn.Write([]byte(Successfull + "\n"))
		case string(HGet):
			globalLogger.Info("HGET command received", zap.String("command", string(buff[:n])))
			if len(parts) != 3 {
				globalLogger.Warn("Invalid HGET command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid HGET command format \n correct usage: %s\n", validCommands.HGet)))
				continue
			}
			key := parts[1]
			field := parts[2]
			v, err := s.ramCache.HGet(key, field)
			if err != nil {
				conn.Write([]byte("Failed HGet operation: " + err.Error() + "\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("%v\n", v)))
		case string(HGetAll):
			globalLogger.Info("HGETALL command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid HGETALL command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid HGETALL command format \n correct usage: %s\n", validCommands.HGetAll)))
				continue
			}
			key := parts[1]
			v := s.ramCache.HGetAll(key)
			conn.Write([]byte(fmt.Sprintf("%v\n", v)))
		case string(HDel):
			globalLogger.Info("HDEL command received", zap.String("command", string(buff[:n])))
			if len(parts) != 3 {
				globalLogger.Warn("Invalid HDEL command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid HDEL command format \n correct usage: %s\n", validCommands.HDel)))
				continue
			}
			key := parts[1]
			field := parts[2]
			s.ramCache.HDel(key, field)
			conn.Write([]byte(Successfull + "\n"))
		case string(HExists):
			globalLogger.Info("HEXISTS command received", zap.String("command", string(buff[:n])))
			if len(parts) != 3 {
				globalLogger.Warn("Invalid HExists command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid HExists command format \n correct usage: %s\n", validCommands.HExists)))
				continue
			}
			key := parts[1]
			field := parts[2]
			exists := s.ramCache.HExists(key, field)
			if exists {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
		case string(LPush):
			globalLogger.Info("LPUSH command received", zap.String("command", string(buff[:n])))
			if len(parts) < 3 {
				globalLogger.Warn("Invalid LPUSH command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid LPUSH command format \n correct usage: %s\n", validCommands.LPush)))
				continue
			}
			key := parts[1]
			values := parts[2]
			s.ramCache.LPush(key, values)
			conn.Write([]byte(Successfull + "\n"))
		case string(RPush):
			globalLogger.Info("RPUSH command received", zap.String("command", string(buff[:n])))
			if len(parts) < 3 {
				globalLogger.Warn("Invalid RPUSH command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid RPUSH command format \n correct usage: %s\n", validCommands.RPush)))
				continue
			}
			key := parts[1]
			values := parts[2]
			s.ramCache.RPush(key, values)
			conn.Write([]byte(Successfull + "\n"))
		case string(LPop):
			globalLogger.Info("LPOP command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid LPOP command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid LPOP command format \n correct usage: %s\n", validCommands.LPop)))
				continue
			}
			key := parts[1]
			v, err := s.ramCache.LPop(key)
			if err != nil {
				conn.Write([]byte("Failed LPop operation: " + err.Error() + "\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("%v\n", v)))
		case string(RPop):
			globalLogger.Info("RPOP command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid RPOP command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid RPOP command format \n correct usage: %s\n", validCommands.RPop)))
				continue
			}
			key := parts[1]
			v, err := s.ramCache.RPop(key)
			if err != nil {
				conn.Write([]byte("Failed RPop operation: " + err.Error() + "\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("%v\n", v)))

		case string(LRange):
			globalLogger.Info("LRANGE command received", zap.String("command", string(buff[:n])))
			if len(parts) != 4 {
				globalLogger.Warn("Invalid LRANGE command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid LRANGE command format \n correct usage: %s\n", validCommands.LRange)))
				continue
			}
			key := parts[1]
			var start, end int
			_, err1 := fmt.Sscanf(parts[2], "%d", &start)
			_, err2 := fmt.Sscanf(parts[3], "%d", &end)
			if err1 != nil || err2 != nil {
				globalLogger.Warn("Invalid LRANGE indices", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid LRANGE indices\n"))
				continue
			}
			v, err := s.ramCache.LRange(key, start, end)
			if err != nil {
				conn.Write([]byte("Failed LRange operation: " + err.Error() + "\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("%v\n", v)))
		case string(SAdd):
			globalLogger.Info("SADD command received", zap.String("command", string(buff[:n])))
			if len(parts) < 3 {
				globalLogger.Warn("Invalid SADD command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid SADD command format \n correct usage: %s\n", validCommands.SAdd)))
				continue
			}
			key := parts[1]
			members := parts[2]
			s.ramCache.SAdd(key, members)
			conn.Write([]byte(Successfull + "\n"))
		case string(SMembers):
			globalLogger.Info("SMEMBERS command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid SMEMBERS command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid SMEMBERS command format \n correct usage: %s\n", validCommands.SMembers)))
				continue
			}
			key := parts[1]
			members := s.ramCache.SMembers(key)
			conn.Write([]byte(fmt.Sprintf("%v\n", members)))
		case string(SRem):
			globalLogger.Info("SREM command received", zap.String("command", string(buff[:n])))
			if len(parts) < 3 {
				globalLogger.Warn("Invalid SREM command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid SREM command format \n correct usage: %s\n", validCommands.SRem)))
				continue
			}
			key := parts[1]
			members := parts[2]
			s.ramCache.SRem(key, members)
			conn.Write([]byte(Successfull + "\n"))
		case string(SIsMember):
			globalLogger.Info("SISMEMBER command received", zap.String("command", string(buff[:n])))
			if len(parts) != 3 {
				globalLogger.Warn("Invalid SISMEMBER command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid SISMEMBER command format \n correct usage: %s\n", validCommands.SIsMember)))
				continue
			}
			key := parts[1]
			member := parts[2]
			isMember := s.ramCache.SIsMember(key, member)
			if isMember {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
		case string(SCARD):
			globalLogger.Info("SCARD command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid SCARD command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid SCARD command format \n correct usage: %s\n", validCommands.SCARD)))
				continue
			}
			key := parts[1]
			card := s.ramCache.SCard(key)
			conn.Write([]byte(fmt.Sprintf("%d\n", card)))
		case string(ZAdd):
			globalLogger.Info("ZADD command received", zap.String("command", string(buff[:n])))
			if len(parts) < 4 {
				globalLogger.Warn("Invalid ZADD command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid ZADD command format \n correct usage: %s\n", validCommands.ZAdd)))
				continue
			}
			key := parts[1]
			var score float64
			_, err := fmt.Sscanf(parts[2], "%f", &score)
			if err != nil {
				globalLogger.Warn("Invalid ZADD score value", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid ZADD score value (%v) \n correct usage: %s\n", parts[2], validCommands.ZAdd)))
				continue
			}
			member := parts[3]
			s.ramCache.ZAdd(key, score, member)
			conn.Write([]byte(Successfull + "\n"))
		case string(Zremove):
			globalLogger.Info("ZREMOVE command received", zap.String("command", string(buff[:n])))
			if len(parts) != 3 {
				globalLogger.Warn("Invalid ZREMOVE command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid ZREMOVE command format \n correct usage: %s\n", validCommands.Zremove)))
				continue
			}
			key := parts[1]
			member := parts[2]
			err := s.ramCache.ZRemove(key, member)
			if err != nil {
				conn.Write([]byte("Failed ZRemove operation: " + err.Error() + "\n"))
				continue
			}
			conn.Write([]byte(Successfull + "\n"))
		case string(Zrange):
			globalLogger.Info("ZRANGE command received", zap.String("command", string(buff[:n])))
			if len(parts) < 4 {
				globalLogger.Warn("Invalid ZRANGE command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid ZRANGE command format \n correct usage: %s\n", validCommands.Zrange)))
				continue
			}
			key := parts[1]
			var start, stop int
			_, err1 := fmt.Sscanf(parts[2], "%d", &start)
			_, err2 := fmt.Sscanf(parts[3], "%d", &stop)
			if err1 != nil || err2 != nil {
				globalLogger.Warn("Invalid ZRANGE indices", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid ZRANGE indices\n"))
				continue
			}
			withScores := false
			if len(parts) == 5 && strings.ToUpper(parts[4]) == "WITHSCORES" {
				withScores = true
			}
			result := s.ramCache.ZRange(key, start, stop, withScores)
			conn.Write([]byte(fmt.Sprintf("%v\n", result)))
		case string(Zrank):
			globalLogger.Info("ZRANK command received", zap.String("command", string(buff[:n])))
			if len(parts) != 3 {
				globalLogger.Warn("Invalid ZRANK command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid ZRANK command format \n correct usage: %s\n", validCommands.Zrank)))
				continue
			}
			key := parts[1]
			member := parts[2]
			rank, exist := s.ramCache.ZRank(key, member)
			if !exist {
				conn.Write([]byte("Error: Member does not exist\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("%d\n", rank)))
		case string(Zrevrank):
			globalLogger.Info("ZREVRANK command received", zap.String("command", string(buff[:n])))
			if len(parts) != 3 {
				globalLogger.Warn("Invalid ZREVRANK command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid ZREVRANK command format \n correct usage: %s\n", validCommands.Zrevrank)))
				continue
			}
			key := parts[1]
			member := parts[2]
			rank, exist := s.ramCache.ZRevRank(key, member)
			if !exist {
				conn.Write([]byte("Error: Member does not exist\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("%d\n", rank)))
		case string(Zscore):
			globalLogger.Info("ZSCORE command received", zap.String("command", string(buff[:n])))
			if len(parts) != 3 {
				globalLogger.Warn("Invalid ZSCORE command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid ZSCORE command format \n correct usage: %s\n", validCommands.Zscore)))
				continue
			}
			key := parts[1]
			member := parts[2]
			score, exist := s.ramCache.ZScore(key, member)
			if !exist {
				conn.Write([]byte("Error: Member does not exist\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("%f\n", score)))
		case string(ServerPrefix):
			// there valid cases Ping, Echo, Close
			if len(parts) < 2 {
				globalLogger.Warn("Invalid command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid command format\n correct usage: SS [Server Command] | Example: SS PING\n"))
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

type correctCommandFormat struct {
	//TODO:
	Ping  string
	Echo  string
	Close string

	Set    string
	Get    string
	GetAll string
	Del    string
	TTL    string
	Exists string
	Type   string

	HSet    string
	HGet    string
	HGetAll string
	HDel    string
	HExists string

	LPush  string
	RPush  string
	LPop   string
	RPop   string
	LRange string

	SAdd      string
	SMembers  string
	SRem      string
	SIsMember string
	SCARD     string

	ZAdd     string
	Zremove  string
	Zrange   string
	Zrank    string
	Zrevrank string
	Zscore   string
}

func newValidCMD() correctCommandFormat {
	return correctCommandFormat{
		Ping:  "SS PING",
		Echo:  "SS ECHO message",
		Close: "SS CLOSE",

		Set:    "SET key value [ttl]",
		Get:    "GET key",
		GetAll: "GETALL",
		Del:    "DEL key",
		TTL:    "TTL key",
		Exists: "EXISTS key",
		Type:   "TYPE key",

		HSet:    "HSET key field value [ttl]",
		HGet:    "HGET key field",
		HGetAll: "HGETALL key",
		HDel:    "HDEL key field",
		HExists: "HEXISTS key field",

		LPush:  "LPUSH key value",
		RPush:  "RPUSH key value",
		LPop:   "LPOP key",
		RPop:   "RPOP key",
		LRange: "LRANGE key start end",

		SAdd:      "SADD key member",
		SMembers:  "SMEMBERS key",
		SRem:      "SREM key member",
		SIsMember: "SISMEMBER key member",
		SCARD:     "SCARD key",

		ZAdd:     "ZADD key score member",
		Zremove:  "ZREMOVE key member",
		Zrange:   "ZRANGE key start stop [WithScores]",
		Zrank:    "ZRANK key member",
		Zrevrank: "ZREVRANK key member",
		Zscore:   "ZSCORE key member",
	}
}
