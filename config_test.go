package arsyncfs

import (
	"testing"
	"io/ioutil"
	"encoding/json"
	"github.com/google/go-cmp/cmp"
	"os"
	"fmt"
)

const configLocation = "testConfig"

func PrintTestError(t *testing.T, message string, got interface{}, want interface{}) {
	t.Errorf("%s, got: %s, want %s", message, got, want)
}

func TestLoad(t *testing.T) {

	initialCfg := Config{
		MountPoint: "/tmp",
		RemotePaths: []string{"l1:121@/tmp", "l2:1121@/tmp/test"},
	}

	data, _ := json.Marshal(initialCfg)

	err := ioutil.WriteFile(configLocation, data, 0666)

	if err != nil {
		fmt.Println(err.Error())
	}

	cfg := Config{}

	cfg.Load(configLocation)

	if !cmp.Equal(initialCfg, cfg) {
		PrintTestError(t,"cfg not matching", cfg, initialCfg)
	}


	os.Remove(configLocation)
}
