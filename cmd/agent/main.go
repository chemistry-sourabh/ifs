package main

import (
	"fmt"
	"ifs"
	"os"
)

func main() {

	cfgPath := "./agent.json"
	args := os.Args[1:]

	if len(args) > 1 {
		fmt.Printf("usage: agent [config]")
		os.Exit(1)
	} else if len(args) == 1 {
		cfgPath = args[0]
	}

	cfg := ifs.AgentConfig{}
	cfg.Load(cfgPath)

	ifs.SetupLogger(cfg.Log)
	ifs.StartAgent(cfg.Address, cfg.Port)
}
