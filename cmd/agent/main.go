/*
Copyright 2018 Sourabh Bollapragada

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"github.com/chemistry-sourabh/ifs"
	"os"
)

import _ "go.uber.org/automaxprocs"

func main() {

	cfgPath := "./agent.yaml"
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
