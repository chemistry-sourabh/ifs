package arsyncfs

import (
	"io/ioutil"
	"fmt"
	"os"
	"encoding/json"
)

type Config struct {
	MountPoint string `json:"mount_point"`
	CacheLocation string `json:"cache_location"`
	RemotePaths []string `json:"remote_paths"`
}



func (c *Config) Load(path string) {

	data, err := ioutil.ReadFile(path)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	json.Unmarshal(data, c)

}
