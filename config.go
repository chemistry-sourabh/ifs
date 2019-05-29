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

package ifs

import (
	"github.com/chemistry-sourabh/ifs/structure"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strconv"
)

type LogConfig struct {
	Logging bool   `yaml:"logging"`
	Console bool   `yaml:"console"`
	Debug   bool   `yaml:"debug"`
	Path    string `yaml:"path"`
}

type FsConfig struct {
	MountPoint  string        `yaml:"mount_point"`
	CachePath   string        `yaml:"cache_path"`
	RemoteRoots []*RemoteRoot `yaml:"remote_roots"`
	Log         *LogConfig    `yaml:"log"`
}

func (c *FsConfig) Load(path string) error {

	data, err := ioutil.ReadFile(path)

	if err == nil {
		err = yaml.Unmarshal(data, c)
	}

	return err
}

type RemoteRoot struct {
	Hostname string   `yaml:"hostname"`
	Port     uint16   `yaml:"port"`
	Paths    []string `yaml:"paths"`
}

func (rr *RemoteRoot) RemotePaths() []*structure.RemotePath {
	var remotePaths []*structure.RemotePath
	for _, path := range rr.Paths {
		remotePaths = append(remotePaths, &structure.RemotePath{
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
	Address string     `yaml:"address"`
	Port    uint16     `yaml:"port"`
	Log     *LogConfig `yaml:"log"`
}

func (c *AgentConfig) Load(path string) error {

	data, err := ioutil.ReadFile(path)

	if err == nil {
		err = yaml.Unmarshal(data, c)
	}

	return err
}
