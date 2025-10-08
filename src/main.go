package main

import (
	"RapidStore/server"
	"fmt"
)

func main() {
	fmt.Printf("%v\n", server.NewServer().Start())
}
