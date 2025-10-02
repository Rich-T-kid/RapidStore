package persistance

import (
	"context"
	"sync"
	"time"
)

/*
Write first
make sure its written to disk
apply to in memory data structure
*/
type WAL interface {
	Write(entry []byte) error
	Read(offset int64) ([]byte, error)
	maxSize(uint64)    // max size of in memory buffer before fsync flush to disk
	maxTime(time.Time) // max time interval before fsync flush to disk
}
type WriteAheadLog struct {
	filePath            string
	buffer              []byte
	maxSize             uint64
	maxTime             time.Time
	maxFileSize         uint64
	maxSegmentSize      uint64
	currentSegmentIndex uint64
	lastFlush           time.Time
	shouldFlush         bool
	sequenceNumber      uint64
	ctx                 context.Context
	cnl                 context.CancelFunc
	lock                sync.RWMutex
	// other fields as necessary
}
