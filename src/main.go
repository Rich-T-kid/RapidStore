package main

import (
	"RapidStore/server"
	"fmt"
	"time"
)

func main() {
	s := server.NewServer()
	go func() {
		time.Sleep(3 * time.Second)
		fmt.Printf("going to stop the server now\n")
		s.Stop()
	}()
	s.Start()
	fmt.Printf("done running the server\n")
}
