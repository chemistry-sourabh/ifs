package main

import (
	"bazil.org/fuse"
	"log"
	"bazil.org/fuse/fs"
	"ifs"
	"path"
	"fmt"
)

func generateRemoteNodes(fs *ifs.Ifs, remoteRoot *ifs.RemoteRoot) map[string]*ifs.RemoteNode {

	remoteRoots := make(map[string]*ifs.RemoteNode)

	for _, joinedPath := range remoteRoot.StringArray() {

		rp := &ifs.RemotePath{}

		rp.Convert(joinedPath)

		rn := &ifs.RemoteNode{
			Ifs:        fs,
			IsDir:      true,
			RemotePath: rp,
		}

		remoteRoots[path.Base(rp.Path)] = rn
	}

	return remoteRoots
}

func main() {
	//log.SetOutput(ioutil.Discard)
	cfg := ifs.Config{}

	cfg.Load("./fs.json")

	c, err := fuse.Mount(cfg.MountPoint)
	if err != nil {
		fmt.Println("Error is Here")
		log.Fatal(err)
	}

	server := fs.New(c, nil)

	fmt.Println("Starting")
	fileSystem := &ifs.Ifs{}

	remoteRootNodes := generateRemoteNodes(fileSystem, cfg.RemoteRoot)

	talker := &ifs.Talker{
		Ifs: fileSystem,
	}

	fileHandler := &ifs.FileHandler{
		Ifs:  fileSystem,
		Path: cfg.CacheLocation,
		Size: 100,
	}

	hoarder := &ifs.Hoarder{
		Ifs:  fileSystem,
		Path: cfg.CacheLocation,
		Size: 100,
	}

	fileSystem.Talker = talker
	fileSystem.FileHandler = fileHandler
	fileSystem.Hoarder = hoarder
	fileSystem.RemoteRoots = remoteRootNodes

	talker.Startup(cfg.RemoteRoot.Address)
	hoarder.Startup()
	fileHandler.StartUp()

	server.Serve(fileSystem)

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Panicln(err)
	}

	c.Close()
}
