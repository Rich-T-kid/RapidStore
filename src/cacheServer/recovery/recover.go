package recovery

import (
	"context"
	"io"
	"sync"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// write state to s3
// read state from s3

// Recovery process:
/*
Implement a persistence layer that allows the leader to periodically or manually dump its in-memory state to a file. This dump will act as a snapshot of the most recent authoritative state. When a new server starts, it should be able to restore from this file and reach the same state as the previous leader before resuming normal operation.

Only the current leader performs state dumps to prevent divergence
Serialize in-memory data structures to a compact file format
Include WAL offset or sequence number for incremental recovery
On startup, allow a node to load state from the latest dump file (mabey store in s3?)

Leader successfully saves its full cache state to disk on command or interval

A new server can restore and resume with identical data and WAL position

No data loss or corruption after simulated crash and restart


TODO: dont forget to set WAL (position/ID/byte offset) back to zero
*/
var (
	bucketName  = "rapid-store-bucket-storage"
	StoragePath = "internal_state_dump" // s3 path to store the state dump
	CredPath    = "recovery/key.json"
)

type ExternalStorage struct{}

var ExtStorage = &ExternalStorage{}

var once sync.Once

func NewExternalStorage() *ExternalStorage {
	once.Do(func() {
		ExtStorage = &ExternalStorage{}
	})
	return ExtStorage
}

func (es *ExternalStorage) SaveState(d []byte, path string, credPath string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(credPath))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	wc := client.Bucket(bucketName).Object(path).NewWriter(ctx)
	if _, err = wc.Write(d); err != nil {
		wc.Close() // ensure proper cleanup even on partial writes
		return err
	}
	return wc.Close()
}
func (es *ExternalStorage) DownloadState(path, credPath string) ([]byte, error) {
	ctx := context.Background()

	client, err := storage.NewClient(ctx, option.WithCredentialsFile(credPath))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	rc, err := client.Bucket(bucketName).Object(path).NewReader(ctx)
	if err != nil {
		panic(err)
	}
	defer rc.Close()
	return io.ReadAll(rc)

}
