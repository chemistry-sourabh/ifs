package main

import (
	"fmt"
	"strconv"
	"ifs"
	"os"
	log "github.com/sirupsen/logrus"
)

func main() {

	args := os.Args[1:]


	if len(args) != 2 {
		fmt.Errorf("usage: agent address port")
		os.Exit(1)
	}

	address := args[0]
	port, err := strconv.ParseInt(args[1], 10, 64)

	if err != nil {
		fmt.Errorf("port should be integer")
	}


	log.SetLevel(log.DebugLevel)

	ifs.StartAgent(address, int(port))
}
