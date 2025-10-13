package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

var (
	// singleton instance of WriteAheadLog
	wal         *WriteAheadLog
	bufferSize  = 4096 * 4 // 16KB buffer
	syncPeriod  = 250 * time.Millisecond
	magicNumber = uint32(0xD9B4BEF9)
	checksumTB  = crc32.MakeTable(crc32.IEEE)
)

// entryLog represents a single log entry in the WAL
type entryLog []byte

// TODO: make this wrap the Reader/Writer interface

// entrys are size|data
// size is uint16
// data is []byte of size
type WriteAheadLog struct {
	filePath       string
	file           *os.File
	maxTime        time.Duration
	buffer         *bytes.Buffer
	lastFlush      time.Time
	sequenceNumber uint64
	ctx            context.Context
	cnl            context.CancelFunc
	lock           sync.RWMutex
}

func GetWAL() *WriteAheadLog {
	if wal == nil {
		sync.OnceFunc(func() {
			wal = newWAL("wal.log", uint32(bufferSize), syncPeriod) // 16KB buffer, flush every 5 seconds
		})()
	}
	return wal
}

// Write now all entries to disk immediately, mabey we could buffer them in the future?
func newWAL(filePath string, maxSize uint32, maxDuration time.Duration) *WriteAheadLog {
	ctx, cnl := context.WithCancel(context.Background())
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("Failed to open WAL file: %v", err))
	}
	buffer := bytes.NewBuffer(make([]byte, 0, maxSize))
	w := &WriteAheadLog{
		filePath:       filePath,
		file:           f,
		maxTime:        maxDuration,
		lastFlush:      time.Now(),
		buffer:         buffer,
		sequenceNumber: 0,
		ctx:            ctx,
		cnl:            cnl,
		lock:           sync.RWMutex{},
	}
	wal = w
	go w.autoSync()
	return wal
}

// locking is up to the caller
func (wal *WriteAheadLog) sync() error {
	_, err := wal.buffer.WriteTo(wal.file)
	wal.lastFlush = time.Now()
	return err
}
func (wal *WriteAheadLog) autoSync() {
	ticker := time.NewTicker(wal.maxTime)
	defer ticker.Stop()
	for {
		select {
		case <-wal.ctx.Done():
			return
		case <-ticker.C:
			wal.lock.Lock()
			if time.Since(wal.lastFlush) >= wal.maxTime {
				wal.sync()
			}
			wal.lock.Unlock()
		}
	}
}

// flush buffers, close files
func (wal *WriteAheadLog) Close() error {
	wal.sync()
	wal.cnl()
	if err := wal.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %v", err)
	}
	globalLogger.Debug("WAL closed successfully", zap.Uint64("sequence Number", wal.sequenceNumber))
	return nil
}

// add checksums
func (wal *WriteAheadLog) Append(entry entryLog) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()
	entrySize := uint32(len(entry))
	// If adding this entry would exceed buffer capacity, flush first
	if wal.buffer.Len()+4+len(entry) > wal.buffer.Cap() {
		wal.sync()
	}
	if err := binary.Write(wal.buffer, binary.BigEndian, magicNumber); err != nil {
		return fmt.Errorf("failed to write magic number: %v", err)
	}
	if err := binary.Write(wal.buffer, binary.BigEndian, entrySize); err != nil {
		return fmt.Errorf("failed to write entry size: %v", err)
	}
	if err := binary.Write(wal.buffer, binary.BigEndian, entry); err != nil {
		return fmt.Errorf("failed to write entry data: %v", err)
	}
	checkSum := crc32.Checksum(entry, checksumTB)
	if err := binary.Write(wal.buffer, binary.BigEndian, checkSum); err != nil {
		return fmt.Errorf("failed to write checksum: %v", err)
	}
	atomic.AddUint64(&wal.sequenceNumber, 1)
	return nil

}

type walEntry struct {
	MagicNumber uint32
	EntrySize   uint32
	Entry       entryLog
	Checksum    uint32
}

func (wal *WriteAheadLog) ReadWal(r io.Reader) <-chan walEntry {
	result := make(chan walEntry)

	go func() {
		defer close(result) // Always close the channel when done

		for {
			beginBuffer := make([]byte, 8) // magicNumber & len
			n, err := r.Read(beginBuffer)
			if err != nil {
				// EOF or other read error, stop reading
				break
			}

			storedMagic := binary.BigEndian.Uint32(beginBuffer[:4])
			if n != 8 || storedMagic != magicNumber {
				// Invalid magic number or incomplete read
				break
			}

			length := binary.BigEndian.Uint32(beginBuffer[4:])
			logEntry := make([]byte, length)
			n, err = r.Read(logEntry)
			if err != nil {
				// Read error, skip this entry
				continue
			}
			logEntry = logEntry[:n]

			checkSum := make([]byte, 4)
			_, err = r.Read(checkSum)
			if err != nil {
				// Failed to read checksum, skip
				continue
			}

			storedCK := binary.BigEndian.Uint32(checkSum)
			computedCK := crc32.Checksum(logEntry, checksumTB)
			if storedCK != computedCK {
				// Checksum mismatch, skip this entry
				continue
			}

			// Send valid entry to channel
			result <- walEntry{
				MagicNumber: storedMagic,
				EntrySize:   length,
				Entry:       logEntry,
				Checksum:    storedCK,
			}
		}
	}()

	return result
}

func NewSetEntry(key string, value interface{}, t time.Duration) entryLog {
	v := fmt.Sprintf("SET %s %v [%v]", key, value, t)
	return []byte(v)
}
func NewGetEntry(key string) entryLog {
	v := fmt.Sprintf("GET %s", key)
	return []byte(v)
}

func NewDeleteKeyEntry(key string) entryLog {
	v := fmt.Sprintf("DEL %s", key)
	return []byte(v)
}

func NewExpireKey(key string, t time.Duration) entryLog {
	v := fmt.Sprintf("EXPIRE %s", key)
	return []byte(v)
}
func NewExistKey(key string) entryLog {
	v := fmt.Sprintf("EXISTS %s", key)
	return []byte(v)
}

func NewType(key string) entryLog {
	v := fmt.Sprintf("TYPE %s", key)
	return []byte(v)
}

func NewIncrement(key string) entryLog {
	v := fmt.Sprintf("INCR %s", key)
	return []byte(v)
}
func NewDecrement(key string) entryLog {
	v := fmt.Sprintf("DECR %s", key)
	return []byte(v)
}
func NewAppend(key, suffix string) entryLog {
	v := fmt.Sprintf("Append %s %s", key, suffix)
	return []byte(v)

}
func NewMset(pairs map[string]any) entryLog {
	contigPairs := ""
	for k, v := range pairs {
		pair := fmt.Sprintf("%s-%v", k, v)
		contigPairs += pair + "/"
	}
	v := fmt.Sprintf("MSet %s", contigPairs[:len(contigPairs)-1])
	fmt.Printf("Generated MSet log: %s\n", v)
	return []byte(v)
}

// HashTableManager entry log constructors
func NewHSet(key, field string, value any, duration ...time.Duration) entryLog {
	v := fmt.Sprintf("HSET %s %s %v [%v]", key, field, value, duration)
	return []byte(v)
}

func NewHGet(key, field string) entryLog {
	v := fmt.Sprintf("HGET %s %s", key, field)
	return []byte(v)
}

func NewHGetAll(key string) entryLog {
	v := fmt.Sprintf("HGetAll %s", key)
	return []byte(v)
}

func NewHDel(key, field string) entryLog {
	v := fmt.Sprintf("HDel %s %s", key, field)
	return []byte(v)
}

func NewHExists(key, field string) entryLog {
	v := fmt.Sprintf("HExists %s %s", key, field)
	return []byte(v)
}

// ListManager entry log constructors
func NewLPush(key string, value any) entryLog {
	v := fmt.Sprintf("LPush %s %v", key, value)
	return []byte(v)
}

func NewRPush(key string, value any) entryLog {
	v := fmt.Sprintf("RPush %s %v", key, value)
	return []byte(v)
}

func NewLPnilop(key string) entryLog {
	v := fmt.Sprintf("LPOP %s", key)
	return []byte(v)
}

func NewRPop(key string) entryLog {
	v := fmt.Sprintf("RPOP %s", key)
	return []byte(v)
}

func NewLRange(key string, start, stop int) entryLog {
	v := fmt.Sprintf("LRange %s", key)
	return []byte(v)
}

func NewSize(key string) entryLog {
	v := fmt.Sprintf("LRange %s", key)
	return []byte(v)
}

// SetManager entry log constructors
func NewSAdd(key string, member any) entryLog {
	v := fmt.Sprintf("SAdd %s %v", key, member)
	return []byte(v)
}

func NewSMembers(key string) entryLog {
	v := fmt.Sprintf("SMembers %s", key)
	return []byte(v)
}

func NewSRem(key string, member any) entryLog {
	v := fmt.Sprintf("SRem %s %v", key, member)
	return []byte(v)
}

func NewSIsMember(key string, member any) entryLog {
	v := fmt.Sprintf("SIsMember %s %v", key, member)
	return []byte(v)
}

func NewSCard(key string) entryLog {
	v := fmt.Sprintf("SCard %s", key)
	return []byte(v)
}

// SortedSetManager entry log constructors
func NewZAdd(key string, score float64, member string) entryLog {
	v := fmt.Sprintf("ZAdd %s %v %s", key, score, member)
	return []byte(v)
}

func NewZRemove(key string, member string) entryLog {
	v := fmt.Sprintf("ZRem %s %s", key, member)
	return []byte(v)
}

func NewZRange(key string, start, stop int, withScores bool) entryLog {
	v := fmt.Sprintf("ZRange %s %d %d %v", key, start, stop, withScores)
	return []byte(v)
}

func NewZRank(key string, member string) entryLog {
	v := fmt.Sprintf("ZRank %s %s", key, member)
	return []byte(v)
}

func NewZRevRank(key string, member string) entryLog {
	v := fmt.Sprintf("ZRevRank %s %s", key, member)
	return []byte(v)
}

func NewZScore(key string, member string) entryLog {
	v := fmt.Sprintf("ZScore %s %s", key, member)
	return []byte(v)
}
