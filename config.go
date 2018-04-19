package ifs

import (
	"io/ioutil"
	"encoding/json"
)

type LogConfig struct {
	Logging bool   `json:"logging"`
	Console bool   `json:"console"`
	Debug   bool   `json:"debug"`
	Path    string `json:"path"`
}

type Config struct {
	MountPoint    string      `json:"mount_point"`
	CacheLocation string      `json:"cache_location"`
	RemoteRoot    *RemoteRoot `json:"remote_root"`
	Log           LogConfig   `json:"log"`
}

func (c *Config) Load(path string) error {

	data, err := ioutil.ReadFile(path)

	if err == nil {
		json.Unmarshal(data, c)
	}

	return err
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
