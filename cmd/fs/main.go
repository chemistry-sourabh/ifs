package main

import (
	"bazil.org/fuse"
	"log"
	"bazil.org/fuse/fs"
	"arsyncfs"
	"path"
)

func generateRemoteNodes(ifs *arsyncfs.Ifs, remoteRoot *arsyncfs.RemoteRoot) map[string]*arsyncfs.RemoteNode {

	remoteRoots := make(map[string]*arsyncfs.RemoteNode)

	for _, joinedPath := range remoteRoot.StringArray() {

		rp := &arsyncfs.RemotePath{}

		rp.Convert(joinedPath)

		rn := &arsyncfs.RemoteNode{
			Ifs:        ifs,
			IsDir:      true,
			RemotePath: rp,
		}

		remoteRoots[path.Base(rp.Path)] = rn
	}

	return remoteRoots
}

func main() {

	cfg := arsyncfs.Config{}

	cfg.Load("./fs.json")

	c, err := fuse.Mount(cfg.MountPoint)
	if err != nil {
		log.Fatal(err)
	}

	server := fs.New(c, nil)

	//cache := &arsyncfs.FileHandler{
	//	Path:                 cfg.CacheLocation,
	//	Size:                 100,
	//	RequestChannel:       cacheRequestChannel,
	//	EgressRequestChannel: egressRequestChannel,
	//	Map:                  make(map[string]uint64),
	//}

	fileSystem := &arsyncfs.Ifs{}

	remoteRootNodes := generateRemoteNodes(fileSystem, &cfg.RemoteRoot)

	talker := &arsyncfs.Talker{
		Ifs: fileSystem,
	}

	fileHandler := &arsyncfs.FileHandler{
		Ifs: fileSystem,
		Path: cfg.CacheLocation,
		Size: 0,
	}

	fileSystem.Talker = talker
	fileSystem.FileHandler = fileHandler
	fileSystem.RemoteRoots = remoteRootNodes

	talker.Startup(cfg.RemoteRoot.Address)
	fileHandler.StartUp()

	server.Serve(fileSystem)

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Panicln(err)
	}

	c.Close()
}
