package arsyncfs

import (
	"io/ioutil"
	"fmt"
	"os"
	"encoding/json"
)

type Config struct {
	MountPoint    string     `json:"mount_point"`
	CacheLocation string     `json:"cache_location"`
	RemoteRoot    RemoteRoot `json:"remote_root"`
}

func (c *Config) Load(path string) {

	data, err := ioutil.ReadFile(path)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	json.Unmarshal(data, c)

}

type RemoteRoot struct {
	Address string   `json:"address"`
	Paths   []string `json:"paths"`
}

func (rr *RemoteRoot) StringArray() []string {

	var joinedPaths []string
	for _, path := range rr.Paths {
		joinedPaths = append(joinedPaths, rr.Address+"@"+path)
	}

	return joinedPaths
}
