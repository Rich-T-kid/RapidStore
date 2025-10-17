package main

import "RapidStore/server"

var basePort = 6300

func main() {
	server.NewServer(
		server.WithPort(basePort + 40),
	).Start()
}
