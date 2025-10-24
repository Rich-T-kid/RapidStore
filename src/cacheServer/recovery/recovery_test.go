package recovery

import (
	"fmt"
	"os"
	"testing"
	"time"
)

const (
	testCredPath = "key.json"
	testDataPath = "testdata/go_test.json"
	testDataDir  = "testdata"
)

func TestSave(t *testing.T) {
	es := &ExternalStorage{}
	data := []byte("test state data (This is updated)")
	err := es.SaveState(data, testDataPath, testCredPath)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}
func TestDownload(t *testing.T) {
	es := &ExternalStorage{}
	v, err := es.DownloadState(testDataPath, testCredPath)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	fmt.Printf("Downloaded Data: %s\n", string(v))
}

func TestIntegration(t *testing.T) {
	es := &ExternalStorage{}
	path := fmt.Sprintf("testdata/go_test_%d.json", time.Now().UnixNano())

	originalData := []byte("test state data for integration")
	err := es.SaveState(originalData, path, testCredPath)
	if err != nil {
		t.Fatalf("SaveState error: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	downloadedData, err := es.DownloadState(path, testCredPath)
	if err != nil {
		t.Fatalf("DownloadState error: %v", err)
	}

	if string(downloadedData) != string(originalData) {
		t.Fatalf("Data mismatch: expected %q, got %q", originalData, downloadedData)
	}
}
func TestIntegration_Basic(t *testing.T) {
	es := &ExternalStorage{}
	path := fmt.Sprintf("%s/go_test_%d.json", testDataDir, time.Now().UnixNano())
	data := []byte("test state data for integration")

	if err := es.SaveState(data, path, testCredPath); err != nil {
		t.Fatalf("SaveState error: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	downloaded, err := es.DownloadState(path, testCredPath)
	if err != nil {
		t.Fatalf("DownloadState error: %v", err)
	}

	if string(downloaded) != string(data) {
		t.Fatalf("Data mismatch: expected %q, got %q", data, downloaded)
	}
}

// Test multiple unique writes and reads to verify propagation
func TestIntegration_MultipleFiles(t *testing.T) {
	es := &ExternalStorage{}
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("%s/multi_test_%d_%d.json", testDataDir, i, time.Now().UnixNano())
		data := []byte(fmt.Sprintf("multi write test %d", i))

		if err := es.SaveState(data, path, testCredPath); err != nil {
			t.Fatalf("SaveState error: %v", err)
		}

		time.Sleep(200 * time.Millisecond)

		read, err := es.DownloadState(path, testCredPath)
		if err != nil {
			t.Fatalf("DownloadState error: %v", err)
		}
		if string(read) != string(data) {
			t.Fatalf("File %d mismatch: expected %q, got %q", i, data, read)
		}
	}
}

func TestIntegration_OverwriteSameFile(t *testing.T) {
	es := &ExternalStorage{}
	path := fmt.Sprintf("%s/overwrite_test.json", testDataDir)

	first := []byte("first version of data")
	second := []byte("second version of data")

	if err := es.SaveState(first, path, testCredPath); err != nil {
		t.Fatalf("SaveState error: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	if err := es.SaveState(second, path, testCredPath); err != nil {
		t.Fatalf("SaveState error (overwrite): %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	read, err := es.DownloadState(path, testCredPath)
	if err != nil {
		t.Fatalf("DownloadState error: %v", err)
	}
	if string(read) != string(second) {
		t.Fatalf("Expected overwritten content %q, got %q", second, read)
	}
}

func TestIntegration_LargePayload(t *testing.T) {
	es := &ExternalStorage{}
	path := fmt.Sprintf("%s/large_payload_%d.json", testDataDir, time.Now().UnixNano())

	// Create a 2 MB payload
	data := make([]byte, 2*1024*1024)
	for i := range data {
		data[i] = byte('A' + (i % 26))
	}

	if err := es.SaveState(data, path, testCredPath); err != nil {
		t.Fatalf("SaveState error (large payload): %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	read, err := es.DownloadState(path, testCredPath)
	if err != nil {
		t.Fatalf("DownloadState error: %v", err)
	}
	if len(read) != len(data) {
		t.Fatalf("Expected size %d, got %d", len(data), len(read))
	}
}

func TestIntegration_TempFileRoundtrip(t *testing.T) {
	es := &ExternalStorage{}
	path := fmt.Sprintf("%s/temp_roundtrip_%d.json", testDataDir, time.Now().UnixNano())
	tempFile := "temp.json"

	data := []byte(`{"status":"ok","message":"roundtrip test"}`)
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	defer os.Remove(tempFile)

	content, _ := os.ReadFile(tempFile)
	if err := es.SaveState(content, path, testCredPath); err != nil {
		t.Fatalf("SaveState error: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	read, err := es.DownloadState(path, testCredPath)
	if err != nil {
		t.Fatalf("DownloadState error: %v", err)
	}
	if string(read) != string(content) {
		t.Fatalf("Roundtrip mismatch: expected %q, got %q", content, read)
	}
}
