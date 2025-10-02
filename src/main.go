package main

import (
	memorystore "RapidStore/memoryStore"
	"fmt"
)

func main() {
	c := memorystore.NewCache()
	fmt.Println(c)
}
