package main

import (
	"RapidStore/server"
	"os"
)

/*
 */
func main() {
	if len(os.Args) == 2 {
		s := server.NewServerFromFile(os.Args[1])
		s.Start()
	} else {
		s := server.NewServer()
		s.Start()
	}
}
