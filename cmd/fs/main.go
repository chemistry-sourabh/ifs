package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"ifs"
	"path"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"bufio"
	"os"
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
	cfg := ifs.Config{}
	cfg.Load("./fs.json")

	if !cfg.Log.Logging {
		log.SetOutput(ioutil.Discard)
	} else {
		f, _ := os.Create(cfg.Log.Path)
		defer f.Close()
		log.SetOutput(bufio.NewWriter(f))
	}

	if cfg.Log.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.ErrorLevel)
	}


	c, err := fuse.Mount(cfg.MountPoint)
	if err != nil {
		log.Fatal(err)
	}

	server := fs.New(c, nil)

	fileSystem := &ifs.Ifs{
		CachedStats: make(map[string]*ifs.Stat),
	}

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
