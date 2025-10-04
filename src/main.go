package main

import (
	"RapidStore/server"
	"fmt"
)

func main() {
	s := server.NewServer()
	s.Start()
	fmt.Printf("done running the server\n")
}
