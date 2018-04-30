package ifs

import (
	"io/ioutil"
	"encoding/json"
	"strconv"
)

type LogConfig struct {
	Logging bool   `json:"logging"`
	Console bool   `json:"console"`
	Debug   bool   `json:"debug"`
	Path    string `json:"path"`
}

type FsConfig struct {
	MountPoint    string        `json:"mount_point"`
	CacheLocation string        `json:"cache_location"`
	RemoteRoots   []*RemoteRoot `json:"remote_roots"`
	Log           *LogConfig    `json:"log"`
	ConnCount     int           `json:"connection_count"`
}

func (c *FsConfig) Load(path string) error {

	data, err := ioutil.ReadFile(path)

	if err == nil {
		err = json.Unmarshal(data, c)
	}

	return err
}

type RemoteRoot struct {
	Hostname string   `json:"hostname"`
	Port     uint16   `json:"port"`
	Paths    []string `json:"paths"`
}

func (rr *RemoteRoot) RemotePaths() []*RemotePath {
	var remotePaths []*RemotePath
	for _, path := range rr.Paths {
		remotePaths = append(remotePaths, &RemotePath{
			Hostname: rr.Hostname,
			Port:     rr.Port,
			Path:     path,
		})
	}

	return remotePaths
}

func (rr *RemoteRoot) StringArray() []string {

	var joinedPaths []string
	for _, path := range rr.Paths {
		joinedPaths = append(joinedPaths, rr.Address()+"@"+path)
	}

	return joinedPaths
}

func (rr *RemoteRoot) Address() string {
	return rr.Hostname + ":" + strconv.FormatInt(int64(rr.Port), 10)
}

type AgentConfig struct {
	Address string     `json:"address"`
	Port    uint16     `json:"port"`
	Log     *LogConfig `json:"log"`
}

func (c *AgentConfig) Load(path string) error {

	data, err := ioutil.ReadFile(path)

	if err == nil {
		err = json.Unmarshal(data, c)
	}

	return err
}
