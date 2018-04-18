package main

import (
	"ifs"
	"flag"
	"fmt"
)


func main() {

	cfgPath := "./fs/json"

	args := flag.Args()

	if len(args) > 1 {
		fmt.Errorf("usage: ifs [config]")
	} else if len(args) == 1 {
		cfgPath = args[0]
	}

	cfg := &ifs.Config{}
	cfg.Load(cfgPath)

	ifs.SetupLogger(cfg)
	ifs.MountRemoteRoots(cfg)
}
