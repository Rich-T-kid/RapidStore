package main

import (
	"RapidStore/server"
	"fmt"
	"os"
	"time"
)

/*
 */
func main() {
	fmt.Printf("========================================================\n")
	fmt.Printf("RapidStore Cache Server Starting Up - %v\n", time.Now().Format(time.RFC1123))
	fmt.Printf("========================================================\n")
	if len(os.Args) == 2 {
		s := server.NewServerFromFile(os.Args[1])
		fmt.Printf("server Start Status: %v\n", s.Start())
	} else {
		s := server.NewServer()
		fmt.Printf("server Start Status: %v\n", s.Start())
	}
}
