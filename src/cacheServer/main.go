package main

import (
	"RapidStore/server"
	"fmt"
)

func main() {
	fmt.Println(server.NewServer().Start())
}
