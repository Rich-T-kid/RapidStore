package main

import (
	"RapidStore/server"
	"fmt"
	//"go.uber.org/zap"
)

func main() {
	fmt.Println(server.NewServer().Start())
}
