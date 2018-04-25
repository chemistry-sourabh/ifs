package ifs

import (
	"testing"
	"io/ioutil"
	"encoding/json"
	"github.com/google/go-cmp/cmp"
	"os"
)

const configLocation = "testConfig"


func TestConfig_LoadSuccess(t *testing.T) {

	// Setup
	initialCfg := FsConfig{
		MountPoint: "/tmp",
		RemoteRoot: &RemoteRoot{
			Address: "localhost:11211",
			Paths: []string{"/tmp", "/tmp/test"},
		},
	}

	data, _ := json.Marshal(initialCfg)
	ioutil.WriteFile(configLocation, data, 0666)

	// Test
	cfg := FsConfig{}
	cfg.Load(configLocation)

	if !cmp.Equal(initialCfg, cfg) {
		PrintTestError(t,"cfg not matching", cfg, initialCfg)
	}


	// Cleanup
	os.Remove(configLocation)
}

func TestConfig_LoadFailure(t *testing.T) {

	cfg := FsConfig{}
	err := cfg.Load(configLocation)

	if err == nil {
		t.Error("err is nil")
	}
}

func TestRemoteRoot_StringArray(t *testing.T) {

	rr := &RemoteRoot{
		Address: "localhost:11211",
		Paths: []string{"/tmp/hello", "/tmp/bye"},
	}

	paths := rr.StringArray()

	result := []string{"localhost:11211@/tmp/hello", "localhost:11211@/tmp/bye"}

	if !cmp.Equal(paths, result) {
		PrintTestError(t, "paths not matching", paths, result)
	}

}