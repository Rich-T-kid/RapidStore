package main

import (
	"RapidStore/server"
	"fmt"
	"os"
)

/*
 */
func main() {
	if len(os.Args) == 2 {
		s := server.NewServerFromFile(os.Args[1])
		fmt.Printf("server Start Status: %v\n", s.Start())
	} else {
		s := server.NewServer()
		fmt.Printf("server Start Status: %v\n", s.Start())
	}
}
