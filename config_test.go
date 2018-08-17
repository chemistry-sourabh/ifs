// +build unit

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

package ifs_test

import (
	"encoding/json"
	"github.com/chemistry-sourabh/ifs"
	"io/ioutil"
	"os"
	"testing"
)

const configLocation = "testConfig"

func TestConfig_LoadSuccess(t *testing.T) {

	// Setup
	initialCfg := ifs.FsConfig{
		MountPoint: "/tmp",
		RemoteRoots: []*ifs.RemoteRoot{
			{
				Hostname: "localhost",
				Port:     11211,
				Paths:    []string{"/tmp", "/tmp/test"},
			},
		},
	}

	data, _ := json.Marshal(initialCfg)
	ioutil.WriteFile(configLocation, data, 0666)

	// Test
	cfg := ifs.FsConfig{}
	cfg.Load(configLocation)

	Compare(t, initialCfg, cfg)

	// Cleanup
	os.Remove(configLocation)
}

func TestConfig_LoadFailure(t *testing.T) {

	cfg := ifs.FsConfig{}
	err := cfg.Load(configLocation)

	Err(t, err)
}

func TestRemoteRoot_StringArray(t *testing.T) {

	rr := &ifs.RemoteRoot{
		Hostname: "localhost",
		Port:     11211,
		Paths:    []string{"/tmp/hello", "/tmp/bye"},
	}

	paths := rr.StringArray()

	result := []string{"localhost:11211@/tmp/hello", "localhost:11211@/tmp/bye"}

	Compare(t, paths, result)
}
