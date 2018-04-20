package ifs

import (
	"bazil.org/fuse"
	log "github.com/sirupsen/logrus"
	"bazil.org/fuse/fs"
	"path"
	"io/ioutil"
	"os"
	"bufio"
)

func generateRemoteNodes(fs *Ifs, remoteRoot *RemoteRoot) map[string]*RemoteNode {

	remoteRoots := make(map[string]*RemoteNode)

	for _, joinedPath := range remoteRoot.StringArray() {

		rp := &RemotePath{}

		rp.Convert(joinedPath)

		rn := &RemoteNode{
			Ifs:        fs,
			IsDir:      true,
			RemotePath: rp,
		}

		remoteRoots[path.Base(rp.Path)] = rn
	}

	return remoteRoots
}

func SetupLogger(cfg *Config) {
	if !cfg.Log.Logging {
		log.SetOutput(ioutil.Discard)
	} else if !cfg.Log.Console{
		f, _ := os.Create(cfg.Log.Path)
		defer f.Close()
		log.SetOutput(bufio.NewWriter(f))
	}

	if cfg.Log.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}


}

func MountRemoteRoots(cfg *Config) {
	c, err := fuse.Mount(cfg.MountPoint)
	if err != nil {
		log.Fatal(err)
	}

	server := fs.New(c, nil)

	fileSystem := &Ifs{
		CachedStats: make(map[string]*Stat),
	}

	remoteRootNodes := generateRemoteNodes(fileSystem, cfg.RemoteRoot)

	talker := &Talker{
		Ifs: fileSystem,
		Pool: &FsConnectionPool{},
	}

	fileHandler := &FileHandler{
		Ifs: fileSystem,
	}

	hoarder := &Hoarder{
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
