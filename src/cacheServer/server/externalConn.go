package server

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

var (
	neverExpireIntRepresentation = -1
	neverExpires                 = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
)

type baseCommands string

// TODO split this into cmd and Inter server cmd
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
	Expire baseCommands = "EXPIRE"
	TTL    baseCommands = "TTL"
	Exists baseCommands = "EXISTS"
	Type   baseCommands = "TYPE"
	Incr   baseCommands = "INCR"
	Decr   baseCommands = "DECR"
	Append baseCommands = "APPEND"
	Mset   baseCommands = "MSET"
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
	SCard     baseCommands = "SCARD"
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
			value := trueType(parts[2])
			// check if ttl is provided
			// if not provided set default value
			var ttl = neverExpireIntRepresentation
			if len(parts) >= 4 {
				_, err := fmt.Sscanf(parts[3], "%d", &ttl)
				if err != nil || ttl < 0 {
					globalLogger.Warn("Invalid TTL value", zap.String("command", string(buff[:n])))
					conn.Write([]byte("Error: Invalid TTL value\n"))
					continue
				}
			}
			content := NewSetEntry(key, value, time.Second*time.Duration(ttl))
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			// after appending to wal, write out to
			if ttl == -1 {
				s.ramCache.SetKey(key, value, time.Until(neverExpires))
			} else {
				s.ramCache.SetKey(key, value, time.Second*time.Duration(ttl))
			}
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
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
			content := NewDeleteKeyEntry(key)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			s.ramCache.DeleteKey(key)
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
		// ToDO update this so that internally handles time.Time for expireation due to latency
		case string(Expire):
			globalLogger.Info("EXPIRE command received", zap.String("command", string(buff[:n])))
			if len(parts) != 3 {
				globalLogger.Warn("Invalid EXPIRE command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid EXPIRE command format \n correct usage: %s\n", validCommands.Expire)))
				continue
			}
			key := parts[1]
			var ttl = neverExpireIntRepresentation
			timeToLive := parts[2]
			_, err := fmt.Sscanf(timeToLive, "%d", &ttl)
			if err != nil || ttl < 0 {
				globalLogger.Warn("Invalid TTL value", zap.String("command", string(buff[:n])))
				conn.Write([]byte("Error: Invalid TTL value\n"))
				continue
			}
			content := NewExpireKey(key, time.Second*time.Duration(ttl))
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			if ttl == -1 {
				s.ramCache.ExpireKey(key, time.Until(neverExpires))
			} else {
				s.ramCache.ExpireKey(key, time.Second*time.Duration(ttl))
			}
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
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
		case string(Incr):
			globalLogger.Info("INCR command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid INCR command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid INCR command format \n correct usage: %s\n", validCommands.Incr)))
				continue
			}
			key := parts[1]
			content := NewIncrement(key)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			newv, err := s.ramCache.Increment(key)
			if err != nil {
				conn.Write([]byte("Error: " + err.Error() + "\n"))
				continue
			}
			if s.propogateChange(content) {
				conn.Write([]byte(fmt.Sprintf("%d\n", newv)))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
		case string(Decr):
			globalLogger.Info("DECR command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid DECR command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid DECR command format \n correct usage: %s\n", validCommands.Decr)))
				continue
			}
			key := parts[1]
			content := NewDecrement(key)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			newv, err := s.ramCache.Decrement(key)
			if err != nil {
				conn.Write([]byte("Error: " + err.Error() + "\n"))
				continue
			}
			if s.propogateChange(content) {
				conn.Write([]byte(fmt.Sprintf("%d\n", newv)))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
		case string(Append):
			globalLogger.Info("APPEND command received", zap.String("command", string(buff[:n])))
			if len(parts) < 3 {
				globalLogger.Warn("Invalid APPEND command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid APPEND command format \n correct usage: %s\n", validCommands.Append)))
				continue
			}
			key := parts[1]
			// Join all parts from index 2 onwards to handle values with spaces
			suffix := strings.Join(parts[2:], " ")
			content := NewAppend(key, suffix)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			err := s.ramCache.Append(key, suffix)
			if err != nil {
				conn.Write([]byte("Error: " + err.Error() + "\n"))
				continue
			}
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
		case string(Mset):
			globalLogger.Info("MSET command received", zap.String("command", string(buff[:n])))
			//TODO: this is a bit more complicated since its a varadic function
			toMap := decodePairs(parts[1:])
			content := NewMset(toMap)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			s.ramCache.MSet(toMap)
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
		case string(HSet):
			globalLogger.Info("HSET command received", zap.String("command", string(buff[:n])))
			if len(parts) < 4 {
				globalLogger.Warn("Invalid HSET command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid HSET command format \n correct usage: %s\n", validCommands.HSet)))
				continue
			}
			key := parts[1]
			field := parts[2]
			value := trueType(parts[3]) // same issue as before, need to get the real data type here for now its fine TODO:

			var ttl = neverExpireIntRepresentation
			if len(parts) >= 5 {
				_, err := fmt.Sscanf(parts[4], "%d", &ttl)
				if err != nil || ttl < 0 {
					globalLogger.Warn("Invalid TTL value", zap.String("command", string(buff[:n])))
					conn.Write([]byte("Error: Invalid TTL value\n"))
					continue
				}
			}
			content := NewHSet(key, field, value, time.Second*time.Duration(ttl))
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			if ttl == -1 {
				s.ramCache.HSet(key, field, value, time.Until(neverExpires))
			} else {
				s.ramCache.HSet(key, field, value, time.Second*time.Duration(ttl))
			}
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
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
			content := NewHDel(key, field)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			s.ramCache.HDel(key, field)
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
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
			values := trueType(parts[2])
			content := NewLPush(key, values)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			s.ramCache.LPush(key, values)
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
		case string(RPush):
			globalLogger.Info("RPUSH command received", zap.String("command", string(buff[:n])))
			if len(parts) < 3 {
				globalLogger.Warn("Invalid RPUSH command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid RPUSH command format \n correct usage: %s\n", validCommands.RPush)))
				continue
			}
			key := parts[1]
			values := trueType(parts[2])
			content := NewRPush(key, values)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			s.ramCache.RPush(key, values)
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
		case string(LPop):
			globalLogger.Info("LPOP command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid LPOP command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid LPOP command format \n correct usage: %s\n", validCommands.LPop)))
				continue
			}
			key := parts[1]
			content := NewLPop(key)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			v, err := s.ramCache.LPop(key)
			if err != nil {
				conn.Write([]byte("Failed LPop operation: " + err.Error() + "\n"))
				continue
			}
			if s.propogateChange(content) {
				conn.Write([]byte(fmt.Sprintf("%v\n", v)))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
		case string(RPop):
			globalLogger.Info("RPOP command received", zap.String("command", string(buff[:n])))
			if len(parts) != 2 {
				globalLogger.Warn("Invalid RPOP command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid RPOP command format \n correct usage: %s\n", validCommands.RPop)))
				continue
			}
			key := parts[1]
			content := NewRPop(key)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			v, err := s.ramCache.RPop(key)
			if err != nil {
				conn.Write([]byte("Failed RPop operation: " + err.Error() + "\n"))
				continue
			}
			if s.propogateChange(content) {
				conn.Write([]byte(fmt.Sprintf("%v\n", v)))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}

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
			members := trueType(parts[2])
			content := NewSAdd(key, members)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			s.ramCache.SAdd(key, members)
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
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
			content := NewSRem(key, members)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			s.ramCache.SRem(key, members)
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
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
		case string(SCard):
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
			content := NewZAdd(key, score, member)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			s.ramCache.ZAdd(key, score, member)
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
		case string(Zremove):
			globalLogger.Info("ZREMOVE command received", zap.String("command", string(buff[:n])))
			if len(parts) != 3 {
				globalLogger.Warn("Invalid ZREMOVE command format", zap.String("command", string(buff[:n])))
				conn.Write([]byte(fmt.Sprintf(" Error: Invalid ZREMOVE command format \n correct usage: %s\n", validCommands.Zremove)))
				continue
			}
			key := parts[1]
			member := parts[2]
			content := NewZRemove(key, member)
			if err := s.Wal.Append(content); err != nil {
				globalLogger.Error("WAL Append error", zap.Error(err))
				conn.Write([]byte(Failed + "\n"))
				continue
			}
			err := s.ramCache.ZRemove(key, member)
			if err != nil {
				conn.Write([]byte("Failed ZRemove operation: " + err.Error() + "\n"))
				continue
			}
			if s.propogateChange(content) {
				conn.Write([]byte(Successfull + "\n"))
			} else {
				conn.Write([]byte(Failed + "\n"))
			}
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

// this will only be used for non-leader nodes, so we only need to handle write commands
// SET , Del , Expire, INCR, DECR, APPEND, MSET, HSET, HDEL, LPush, RPush, LPop, RPop, SAdd, SRem, ZAdd, ZRemove
func (s *Server) updateState(entry entryLog) error {
	globalLogger.Debug("Replaying log entry", zap.String("entry", string(entry)))
	parts := strings.Split(string(entry), " ")
	switch parts[0] {
	case string(SET):
		if len(parts) < 3 {
			return fmt.Errorf("invalid SET entry format: %s, correct format %s", string(entry), validCommands.Set)
		}
		key := parts[1]
		value := trueType(parts[2])
		ttl := parts[3]
		var tllSeconds int
		_, err := fmt.Sscanf(ttl, "%d", &tllSeconds)
		if err != nil {
			globalLogger.Debug("Invalid TTL value in log", zap.String("log", string(entry)))
			return fmt.Errorf("invalid TTL value in log: %s", ttl)
		}

		// Only pass TTL if it's greater than 0 (0 means no expiration)
		if tllSeconds > 0 {
			s.ramCache.SetKey(key, value, time.Second*time.Duration(tllSeconds))
		} else {
			s.ramCache.SetKey(key, value, time.Until(neverExpires))
		}
	case string(Del):
		if len(parts) != 2 {
			return fmt.Errorf("invalid DEL entry format: %s, correct format %s", string(entry), validCommands.Del)
		}
		key := parts[1]

		s.ramCache.DeleteKey(key)
	case string(Expire):
		if len(parts) != 3 {
			return fmt.Errorf("invalid EXPIRE entry format: %s, correct format %s", string(entry), validCommands.Expire)
		}
		key := parts[1]
		var seconds int
		_, err := fmt.Sscanf(parts[2], "%d", &seconds)
		if err != nil {
			return fmt.Errorf("invalid expire time in log: %s", parts[2])
		}
		s.ramCache.ExpireKey(key, time.Second*time.Duration(seconds))
	case string(Incr):
		if len(parts) != 2 {
			return fmt.Errorf("invalid INCR entry format: %s, correct format %s", string(entry), validCommands.Incr)
		}
		key := parts[1]
		s.ramCache.Increment(key)
	case string(Decr):
		if len(parts) != 2 {
			return fmt.Errorf("invalid DECR entry format: %s, correct format %s", string(entry), validCommands.Decr)
		}
		key := parts[1]
		s.ramCache.Decrement(key)
	case string(Append):
		if len(parts) < 3 {
			return fmt.Errorf("invalid APPEND entry format: %s, correct format %s", string(entry), validCommands.Append)
		}

		// Use SplitN to handle suffixes with spaces correctly
		parts := strings.SplitN(string(entry), " ", 3)
		if len(parts) < 3 {
			return fmt.Errorf("invalid APPEND entry format: %s", string(entry))
		}
		key := parts[1]
		suffix := parts[2]
		fmt.Printf("err from append %v\n ", s.ramCache.Append(key, suffix))
		fmt.Printf("result of appending %s to %s: %v\n", suffix, key, s.ramCache.GetKey(key))
	case string(Mset):
		if len(parts) < 2 {
			return fmt.Errorf("invalid MSET entry format: %s, correct format %s", string(entry), validCommands.Mset)
		}
		pairs := strings.Split(parts[1], "/")
		for i := range pairs {
			subPairs := strings.SplitN(pairs[i], "-", 2)
			fmt.Printf("key: %s, value: %s\n", subPairs[0], subPairs[1])
			pairs[i] = strings.TrimSpace(pairs[i])
			s.ramCache.SetKey(subPairs[0], subPairs[1])
		}
	case string(HSet):
		if len(parts) < 4 {
			return fmt.Errorf("invalid HSET entry format: %s, correct format %s", string(entry), validCommands.HSet)
		}
		key := parts[1]
		field := parts[2]
		value := trueType(parts[3]) // TODO: need to parse the actual type here
		var seconds int
		_, err := fmt.Sscanf(parts[4], "%d", &seconds)
		if err != nil {
			return fmt.Errorf("invalid TTL value in log: %s", parts[4])
		}
		if seconds == -1 {
			s.ramCache.HSet(key, field, value, time.Until(neverExpires))
			return nil
		}
		s.ramCache.HSet(key, field, value, time.Second*time.Duration(seconds))
	case string(HDel):
		if len(parts) != 3 {
			return fmt.Errorf("invalid HDEL entry format: %s, correct format %s", string(entry), validCommands.HDel)
		}
		key := parts[1]
		field := parts[2]
		s.ramCache.HDel(key, field)
	case string(LPush):
		if len(parts) < 3 {
			return fmt.Errorf("invalid LPUSH entry format: %s, correct format %s", string(entry), validCommands.LPush)
		}
		key := parts[1]
		value := parts[2] // TODO: need to parse the actual type here
		s.ramCache.LPush(key, value)
	case string(RPush):
		if len(parts) < 3 {
			return fmt.Errorf("invalid RPUSH entry format: %s, correct format %s", string(entry), validCommands.RPush)
		}

		key := parts[1]
		value := parts[2] // TODO: need to parse the actual type here
		s.ramCache.RPush(key, value)
	case string(LPop):
		if len(parts) != 2 {
			return fmt.Errorf("invalid LPOP entry format: %s, correct format %s", string(entry), validCommands.LPop)
		}

		key := parts[1]
		s.ramCache.LPop(key)
	case string(RPop):
		if len(parts) != 2 {
			return fmt.Errorf("invalid RPOP entry format: %s, correct format %s", string(entry), validCommands.RPop)
		}
		key := parts[1]
		s.ramCache.RPop(key)
	case string(SAdd):
		if len(parts) < 3 {
			return fmt.Errorf("invalid SADD entry format: %s, correct format %s", string(entry), validCommands.SAdd)
		}
		key := parts[1]
		member := parts[2] // TODO: need to parse the actual type here
		s.ramCache.SAdd(key, member)
	case string(SRem):
		if len(parts) < 3 {
			return fmt.Errorf("invalid SREM entry format: %s, correct format %s", string(entry), validCommands.SRem)
		}
		key := parts[1]
		member := parts[2] // TODO: need to parse the actual type here
		s.ramCache.SRem(key, member)
	case string(ZAdd):
		if len(parts) < 4 {
			return fmt.Errorf("invalid ZADD entry format: %s, correct format %s", string(entry), validCommands.ZAdd)
		}

		key := parts[1]
		var score float64
		_, err := fmt.Sscanf(parts[2], "%f", &score)
		if err != nil {
			return fmt.Errorf("invalid score in log: %s", parts[2])
		}
		member := parts[3]
		s.ramCache.ZAdd(key, score, member)
	case string(Zremove):
		if len(parts) != 3 {
			return fmt.Errorf("invalid ZREMOVE entry format: %s, correct format %s", string(entry), validCommands.Zremove)
		}
		key := parts[1]
		member := parts[2]
		s.ramCache.ZRemove(key, member)
	default:
		return fmt.Errorf("unknown command in log: %s", parts[0])
	}
	return nil
}

func (s *Server) propogateChange(message []byte) bool {
	switch s.config.persistence.ConsistancyType {
	case string(Strong):
		syncExternal(s.config.election.followerInfo, message, uint(len(s.config.election.followerInfo)), s.config.persistence.ReplicationTimeout)
	case string(Quorum):
		neededAcks := (uint(len(s.config.election.followerInfo)) / 2) + 1
		return syncExternal(s.config.election.followerInfo, message, neededAcks, s.config.persistence.ReplicationTimeout)
	case string(Eventual): // eventual -> send to 1
		return syncExternal(s.config.election.followerInfo, message, 1, s.config.persistence.ReplicationTimeout)
	default:
		// leader only case
		return true
	}
	return true
}

// dest : ip:port of followers nodes
// message : message to be sent
// concensus : number of nodes that need to ack the message
// timeout : time to wait for acks
// returns true if concensus is reached, false otherwise
func syncExternal(dest []followerInfo, message []byte, consensus uint, timeout time.Duration) bool {
	fmt.Printf("writing %v to followers %v, Need %d acks to be considered succesfull within %v seconds\n", string(message), dest, consensus, timeout.Seconds())
	if len(dest) == 0 {
		return true
	}
	if consensus > uint(len(dest)) {
		consensus = uint(len(dest))
	}
	var curAck uint32
	done := make(chan struct{}, len(dest))
	for _, nodeElement := range dest {
		go func(node followerInfo) {
			defer func() { done <- struct{}{} }()
			buff := make([]byte, 256)
			if _, err := node.c.Write(message); err != nil {
				fmt.Printf("failed to write to %s:%s %v\n", node.address, node.port, err)
				return
			}

			_ = node.c.SetReadDeadline(time.Now().Add(timeout - 1*time.Second))

			n, err := node.c.Read(buff)
			if err != nil {
				fmt.Printf("read error from %s:%s %v\n", node.address, node.port, err)
				return
			}
			nodeResponse := buff[:n]
			fmt.Println("Follower replied: ", string(nodeResponse))
			if strings.Contains(string(nodeResponse), "Successful") {
				fmt.Println("follower succesfully received the message")
				atomic.AddUint32(&curAck, 1)
			}
		}(nodeElement)
	}

	// Wait until either enough acks or timeout
	expire := time.After(timeout)
	for {
		select {
		case <-done:
			if atomic.LoadUint32(&curAck) >= uint32(consensus) {
				return true
			}
			// if all replies are back but not enough acks, fail fast
			if int(atomic.LoadUint32(&curAck))+len(done) >= len(dest) {
				return false
			}
		case <-expire:
			globalLogger.Warn("Consensus timeout reached", zap.Uint32("currentAcks", atomic.LoadUint32(&curAck)), zap.Uint("neededAcks", consensus))
			return atomic.LoadUint32(&curAck) >= uint32(consensus)
		}
	}
}
func (s *Server) InterServerCommunications() {

	// Start inter-server connection immediately instead of waiting for ticker
	globalLogger.Debug("Starting initial InterServer connection")
	s.startInterServerConnection()

}

func (s *Server) startInterServerConnection() chan struct{} {
	stopSignal := make(chan struct{})

	go func() {

		bindAddr := "0.0.0.0"

		addr := fmt.Sprintf("%s:%d", bindAddr, s.config.Port+1)
		globalLogger.Info("Inter Server communications is @ ", zap.String("address", addr))

		healthListener, err := net.Listen("tcp4", addr)
		if err != nil {
			globalLogger.Warn("Failed to start health check listener", zap.Error(err))
			return
		}
		defer healthListener.Close()

		if s.config.election.isLeader {
			globalLogger.Info("Starting Inter Server communications as Leader")
		} else {
			globalLogger.Info("Starting Inter Server communications as Follower")
		}
		for {
			select {
			case <-stopSignal:
				globalLogger.Debug("Stopping InterServer connection")
				return
			default:
				// Set a timeout for Accept to make it non-blocking
				healthListener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))

				conn, err := healthListener.Accept()
				if err != nil {
					// Check if it's a timeout (normal) or real error
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						continue // Just a timeout, continue checking for stop signal
					}
					if errors.Is(err, net.ErrClosed) {
						return
					}
					globalLogger.Warn("(Inter Server Communications) Failed to accept connection", zap.Error(err))
					continue
				}

				// Handle the connection
				go func(c net.Conn) {
					defer c.Close()
					status := "OK"
					if !s.isLive {
						status = "NOT OK"
					}
					buff := make([]byte, 256)
					n, err := c.Read(buff)
					if err != nil {
						globalLogger.Warn("Error reading from connection", zap.Error(err))
						return
					}
					content := string(buff[:n])
					switch content {
					case string(PING):
						globalLogger.Info("(1) Received PING message from InterServer comms", zap.String("content", content))
						c.Write([]byte("PONG\n"))
					case string(ECHO):
						globalLogger.Info("(1) Received ECHO message from InterServer comms", zap.String("content", content))
						c.Write([]byte(fmt.Sprintf("Health Status: %s\n", status)))
					default:
						globalLogger.Info("(2) Received  message from InterServer comms", zap.String("content", content))
						if e := s.updateState(entryLog(content)); e != nil {
							globalLogger.Error("Failed to update state from inter-server message", zap.Error(e))
							c.Write([]byte("Failed\n"))
							return
						}
						c.Write([]byte("Successful\n"))
					}
				}(conn)
			}
		}
	}()

	return stopSignal
}

// key-value
func decodePairs(pairs []string) map[string]any {
	var toMap = make(map[string]any)
	for i := range pairs {
		pairs[i] = strings.TrimSpace(pairs[i])
		pieces := strings.SplitN(pairs[i], "-", 2)
		if len(pieces) != 2 {
			continue
		}
		key := pieces[0]
		value := pieces[1] // TODO: need to parse the actual type here
		toMap[key] = value
	}
	return toMap
}

func trueType(s string) interface{} {
	// Check if string is quoted - if so, remove quotes and return as string
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1] // remove quotes and return as string
	}
	// Try boolean first
	if s == "true" {
		return true
	}
	if s == "false" {
		return false
	}

	// Try int64
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}

	// Try float64
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}

	// Default to string
	return s
}

type correctCommandFormat struct {
	Ping  string
	Echo  string
	Close string

	Set    string
	Get    string
	GetAll string
	Del    string
	Expire string
	TTL    string
	Exists string
	Type   string
	Incr   string
	Decr   string
	Append string
	Mset   string

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
		Expire: "EXPIRE key [ttl]",
		TTL:    "TTL key",
		Exists: "EXISTS key",
		Type:   "TYPE key",
		Incr:   "INCR key",
		Decr:   "DECR key",
		Append: "APPEND key suffix",
		Mset:   "MSET key1-value1 key2-value2 ...",

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
