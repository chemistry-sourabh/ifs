package main

import (
	"ifs"
	"fmt"
	"os"
)


func main() {

	cfgPath := "./fs.json"

	args := os.Args[1:]

	if len(args) > 1 {
		fmt.Errorf("usage: ifs [config]")
		os.Exit(1)
	} else if len(args) == 1 {
		cfgPath = args[0]
	}

	cfg := &ifs.Config{}
	err := cfg.Load(cfgPath)

	if err != nil {
		fmt.Errorf("got error: %s",err)
	}

	ifs.SetupLogger(cfg)
	ifs.MountRemoteRoots(cfg)
}
