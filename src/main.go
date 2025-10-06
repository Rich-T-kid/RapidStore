package main

import (
	"RapidStore/server"
	"fmt"
	"os"
)

func main() {
	f, err := os.Open("../config.yaml")
	if err != nil {
		fmt.Printf("Error reading config file: %v\n", err)
		return
	}
	s, err := server.ServerFromConfig("config.yaml", f)
	if err != nil {
		fmt.Printf("Error parsing config file: %v\n", err)
		return
	}
	fmt.Printf("Config: %+v\n", s.ExposeConfig())
}
