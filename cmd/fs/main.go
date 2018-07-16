package main

import (
	"ifs"
	"fmt"
	"os"
)

import _ "go.uber.org/automaxprocs"

func main() {

	cfgPath := "./fs.json"

	args := os.Args[1:]

	if len(args) > 1 {
		fmt.Printf("usage: ifs [config]")
		os.Exit(1)
	} else if len(args) == 1 {
		cfgPath = args[0]
	}

	cfg := &ifs.FsConfig{}
	err := cfg.Load(cfgPath)

	if err != nil {
		fmt.Printf("got error: %s",err)
	}

	ifs.SetupLogger(cfg.Log)
	ifs.MountRemoteRoots(cfg)
}
