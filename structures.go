package arsyncfs

import (
	"fmt"
	"strings"
	"strconv"
	"os"
	"github.com/mitchellh/mapstructure"
)

type Request struct {
	Id              uint64
	Op              uint8          `json:"op"`
	RemoteNode      *RemoteNode    `json:"remote-node"`
	ResponseChannel chan BaseResponse `json:"-"`
}


type CacheRequest struct {
	Op              uint8
	RemoteNode      *RemoteNode
	ResponseChannel chan []byte
}

type Stat struct {
	Name    string
	Size    int64
	Mode    os.FileMode
	ModTime int64
	IsDir   bool
}

func ConvertToStat(v interface{}) *Stat {
	s := &Stat{}
	mapstructure.Decode(v.(map[string]interface{}), s)
	return s
}

type RemotePath struct {
	Hostname string `json:"hostname"`
	Port     uint32 `json:"port"`
	Path     string `json:"path"`
}

func (rp *RemotePath) String() string {
	return fmt.Sprintf("%s:%d@%s", rp.Hostname, rp.Port, rp.Path)
}

func (rp *RemotePath) Convert(str string) {
	parts := strings.Split(str, ":")
	rp.Hostname = parts[0]
	parts = strings.Split(parts[1], "@")
	p64, _ := strconv.ParseUint(parts[0], 10, 32)
	rp.Port = uint32(p64)
	rp.Path = parts[1]
}

func (rp *RemotePath) Address() string {
	return fmt.Sprintf("%s:%d", rp.Hostname, rp.Port)
}
