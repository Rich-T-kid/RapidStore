package server

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"
)

// TestServerEchoPingFeatures tests the ECHO and PING functionality
func TestServerEchoPingFeatures(t *testing.T) {
	// Initialize server
	server := NewServer(
		WithPort(6382), // Use different port to avoid conflicts
	)

	// Start server in goroutine
	go func() {
		server.Start()
	}()

	// Wait for server to start
	time.Sleep(50 * time.Millisecond)

	// Connect to server
	conn, err := net.Dial("tcp", "0.0.0.0:6382")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Test PING command
	t.Run("PING command", func(t *testing.T) {
		_, err := fmt.Fprintf(conn, "SS PING\n")
		if err != nil {
			t.Fatalf("Failed to send PING command: %v", err)
		}

		response, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read PING response: %v", err)
		}

		expected := "PONG\n"
		if response != expected {
			t.Errorf("Expected '%s', got '%s'", expected, response)
		}
	})
	t.Run("ECHO command", func(t *testing.T) {
		message := "Hello, World!"
		_, err := fmt.Fprintf(conn, "SS ECHO %s\n", message)
		if err != nil {
			t.Fatalf("Failed to send ECHO command: %v", err)
		}

		response, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read ECHO response: %v", err)
		}

		expected := fmt.Sprintf("%s\n", message)
		if response != expected {
			t.Errorf("Expected '%s', got '%s'", expected, response)
		}
	})

	// Stop server
	fmt.Fprintf(conn, "Close\n")

	// Wait for server to stop
	time.Sleep(50 * time.Millisecond)
}
