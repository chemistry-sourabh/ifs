// +build unit

package unit

import (
	"testing"
	"io/ioutil"
	"encoding/json"
	"os"
	"ifs"
	"ifs/test"
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

	test.Compare(t, initialCfg, cfg)

	// Cleanup
	os.Remove(configLocation)
}

func TestConfig_LoadFailure(t *testing.T) {

	cfg := ifs.FsConfig{}
	err := cfg.Load(configLocation)

	test.Error(t, err)
}

func TestRemoteRoot_StringArray(t *testing.T) {

	rr := &ifs.RemoteRoot{
		Hostname: "localhost",
		Port:     11211,
		Paths:    []string{"/tmp/hello", "/tmp/bye"},
	}

	paths := rr.StringArray()

	result := []string{"localhost:11211@/tmp/hello", "localhost:11211@/tmp/bye"}

	test.Compare(t, paths, result)
}
