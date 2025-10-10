package main

import (
	"RapidStore/server"
	"fmt"
)

var unusedport = 9321

func main() {
	fmt.Println(server.NewServer(
		server.WithPort(unusedport + 51),
	).Start())
}
