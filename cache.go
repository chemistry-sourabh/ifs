package arsyncfs

import (
	"io/ioutil"
	"fmt"
	"encoding/base64"
	"path"
	"log"
	"os"
)

type Cache struct {
	FileId               uint64
	Path                 string
	Size                 uint64
	RequestChannel       chan *CacheRequest
	EgressRequestChannel chan *Request
	Map                  map[string] uint64
}

func (c *Cache) ProcessRequests() {
	log.Println("Starting Cache Manager")

	log.Println("Deleting Cache")
	os.RemoveAll(c.Path)
	os.MkdirAll(c.Path, 0755)

	for creq := range c.RequestChannel {
		log.Println("Got Cache Request")

		switch creq.Op {

		case FetchFileCacheOp:
			respChannel := make(chan *Response)

			req := &Request{
				Op:              FetchFileOp,
				RemoteNode:      creq.RemoteNode,
				ResponseChannel: respChannel,
			}

			log.Println("Forwarding Cache Request")
			c.EgressRequestChannel <- req

			c.ReceiveFile(respChannel)

		case GetLocalFileCacheOp:

			filename := fmt.Sprintf("%d", c.Map[creq.RemoteNode.RemotePath.String()])
			data, err := ioutil.ReadFile(path.Join(c.Path, filename))

			if err != nil {
				log.Fatal(err)
			}

			creq.ResponseChannel <- data

		}
	}

}

func (c *Cache) ReceiveFile(respChannel chan *Response) {

	resp := <-respChannel

	data, _ := base64.StdEncoding.DecodeString(resp.Response.(string))

	id := c.FileId
	c.FileId++

	err := ioutil.WriteFile(path.Join(c.Path, fmt.Sprintf("%d", id)), data, 0666)

	if err != nil {
		log.Fatal(err)
	}

	c.Map[resp.RemoteNode.RemotePath.String()] = id

}
