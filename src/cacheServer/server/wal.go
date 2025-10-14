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

// TODO: graceful shutdown, use a chan

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

// mostly for testing
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
	seconds := int64(t.Seconds())
	v := fmt.Sprintf("%s %s %v %d", SET, key, value, seconds)
	return []byte(v)
}
func NewGetEntry(key string) entryLog {
	v := fmt.Sprintf("%s %s", GET, key)
	return []byte(v)
}
func NewGetAllEntry(key string) entryLog {
	v := fmt.Sprintf("%s %s", GetAll, key)
	return []byte(v)
}

func NewDeleteKeyEntry(key string) entryLog {
	v := fmt.Sprintf("%s %s", Del, key)
	return []byte(v)
}

func NewExpireKey(key string, t time.Duration) entryLog {
	seconds := int64(t.Seconds())
	v := fmt.Sprintf("%s %s %d", Expire, key, seconds)
	return []byte(v)
}
func NewExistKey(key string) entryLog {
	v := fmt.Sprintf("%s %s", Exists, key)
	return []byte(v)
}

func NewType(key string) entryLog {
	v := fmt.Sprintf("%s %s", Type, key)
	return []byte(v)
}

func NewIncrement(key string) entryLog {
	v := fmt.Sprintf("%s %s", Incr, key)
	return []byte(v)
}
func NewDecrement(key string) entryLog {
	v := fmt.Sprintf("%s %s", Decr, key)
	return []byte(v)
}
func NewAppend(key, suffix string) entryLog {
	v := fmt.Sprintf("%s %s %s", Append, key, suffix)
	return []byte(v)

}
func NewMset(pairs map[string]any) entryLog {
	contigPairs := ""
	for k, v := range pairs {
		pair := fmt.Sprintf("%s-%v", k, v)
		contigPairs += pair + "/"
	}
	v := fmt.Sprintf("%s %s", Mset, contigPairs[:len(contigPairs)-1])
	return []byte(v)
}

// HashTableManager entry log constructors
func NewHSet(key, field string, value any, duration time.Duration) entryLog {
	seconds := int64(duration.Seconds())
	v := fmt.Sprintf("%s %s %s %v %d", HSet, key, field, value, seconds)
	return []byte(v)
}

func NewHGet(key, field string) entryLog {
	v := fmt.Sprintf("%s %s %s", HGet, key, field)
	return []byte(v)
}

func NewHGetAll(key string) entryLog {
	v := fmt.Sprintf("%s %s", HGetAll, key)
	return []byte(v)
}

func NewHDel(key, field string) entryLog {
	v := fmt.Sprintf("%s %s %s", HDel, key, field)
	return []byte(v)
}

func NewHExists(key, field string) entryLog {
	v := fmt.Sprintf("%s %s %s", HExists, key, field)
	return []byte(v)
}

// ListManager entry log constructors
func NewLPush(key string, value any) entryLog {
	v := fmt.Sprintf("%s %s %v", LPush, key, value)
	return []byte(v)
}

func NewRPush(key string, value any) entryLog {
	v := fmt.Sprintf("%s %s %v", RPush, key, value)
	return []byte(v)
}

func NewLPnilop(key string) entryLog {
	v := fmt.Sprintf("%s %s", LPush, key)
	return []byte(v)
}

func NewRPop(key string) entryLog {
	v := fmt.Sprintf("%s %s", RPop, key)
	return []byte(v)
}

func NewLRange(key string, start, stop int) entryLog {
	v := fmt.Sprintf("%s %s", LRange, key)
	return []byte(v)
}

// SetManager entry log constructors
func NewSAdd(key string, member any) entryLog {
	v := fmt.Sprintf("%s %s %v", SAdd, key, member)
	return []byte(v)
}

func NewSMembers(key string) entryLog {
	v := fmt.Sprintf("%s %s", SMembers, key)
	return []byte(v)
}

func NewSRem(key string, member any) entryLog {
	v := fmt.Sprintf("%s %s %v", SRem, key, member)
	return []byte(v)
}

func NewSIsMember(key string, member any) entryLog {
	v := fmt.Sprintf("%s %s %v", SIsMember, key, member)
	return []byte(v)
}

func NewSCard(key string) entryLog {
	v := fmt.Sprintf("%s %s", SCard, key)
	return []byte(v)
}

// SortedSetManager entry log constructors
func NewZAdd(key string, score float64, member string) entryLog {
	v := fmt.Sprintf("%s %s %v %s", ZAdd, key, score, member)
	return []byte(v)
}

func NewZRemove(key string, member string) entryLog {
	v := fmt.Sprintf("%s %s %s", Zremove, key, member)
	return []byte(v)
}

func NewZRange(key string, start, stop int, withScores bool) entryLog {
	v := fmt.Sprintf("%s %s %d %d %v", Zrange, key, start, stop, withScores)
	return []byte(v)
}

func NewZRank(key string, member string) entryLog {
	v := fmt.Sprintf("%s %s %s", Zrank, key, member)
	return []byte(v)
}

func NewZRevRank(key string, member string) entryLog {
	v := fmt.Sprintf("%s %s %s", Zrevrank, key, member)
	return []byte(v)
}

func NewZScore(key string, member string) entryLog {
	v := fmt.Sprintf("%s %s %s", Zscore, key, member)
	return []byte(v)
}
