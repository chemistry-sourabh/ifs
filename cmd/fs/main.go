package main

import (
	"bazil.org/fuse"
	"log"
	"bazil.org/fuse/fs"
	"arsyncfs"
	"github.com/gorilla/websocket"
)

func generateRemotePaths(paths []string) []*arsyncfs.RemotePath {

	var remotePaths []*arsyncfs.RemotePath
	for _, path := range paths {
		remotePath := &arsyncfs.RemotePath{}
		remotePath.Convert(path)
		remotePaths = append(remotePaths, remotePath)
	}

	return remotePaths
}

func generateRemoteNodes(paths []*arsyncfs.RemotePath, egressRequestChannel chan *arsyncfs.Request, cacheRequestChannel chan *arsyncfs.CacheRequest) map[string]*arsyncfs.RemoteNode {

	remoteRoots := make(map[string]*arsyncfs.RemoteNode)

	for _, path := range paths {
		rn := &arsyncfs.RemoteNode{
			IsDir:                true,
			RemotePath:           path,
			EgressRequestChannel: egressRequestChannel,
			CacheRequestChannel:  cacheRequestChannel,
		}

		remoteRoots[path.Address()] = rn
	}

	return remoteRoots
}

func generateIngressChannels(paths []*arsyncfs.RemotePath) map[string]chan *arsyncfs.Request {

	ingressChannels := make(map[string]chan *arsyncfs.Request)

	for _, path := range paths {
		ingressChannels[path.Address()] = make(chan *arsyncfs.Request, arsyncfs.ChannelLength)
	}

	return ingressChannels
}

func main() {

	cfg := arsyncfs.Config{}

	cfg.Load("./fs.json")

	c, err := fuse.Mount(cfg.MountPoint)
	if err != nil {
		log.Fatal(err)
	}

	server := fs.New(c, nil)

	egressRequestChannel := make(chan *arsyncfs.Request, arsyncfs.ChannelLength)
	cacheRequestChannel := make(chan *arsyncfs.CacheRequest, arsyncfs.ChannelLength)

	remoteRoots := generateRemotePaths(cfg.RemotePaths)

	cache := &arsyncfs.Cache{
		Path:                 cfg.CacheLocation,
		Size:                 100,
		RequestChannel:       cacheRequestChannel,
		EgressRequestChannel: egressRequestChannel,
		Map: make(map[string] uint64),
	}

	fileSystem := &arsyncfs.Root{
		RemoteRoots: generateRemoteNodes(remoteRoots, egressRequestChannel, cacheRequestChannel),
	}

	talker := arsyncfs.Talker{
		EgressRequestChannel:   egressRequestChannel,
		WebSockets:             make(map[string]*websocket.Conn),
		IngressRequestChannels: generateIngressChannels(remoteRoots),
	}

	talker.MountRemoteRoots(remoteRoots)

	go cache.ProcessRequests()

	for _, path := range remoteRoots {
		go talker.ProcessAgentMessages(path.Address())
	}

	go talker.ProcessChannel()

	server.Serve(fileSystem)

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Panicln(err)
	}

	c.Close()
}
