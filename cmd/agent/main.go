package main

import (
	"flag"
	"fmt"
	"strconv"
	"ifs"
)

func main() {

	args := flag.Args()

	if len(args) != 2 {
		fmt.Errorf("usage: agent address port")
	}

	address := args[0]
	port, err := strconv.ParseInt(args[1], 10, 64)

	if err != nil {
		fmt.Errorf("port should be integer")
	}

	ifs.StartAgent(address, int(port))
}
