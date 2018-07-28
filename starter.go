package ifs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"path/filepath"
	"strings"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"github.com/orcaman/concurrent-map"
)

func SetupLogger(cfg *LogConfig) {

	loggerCfg := zap.NewDevelopmentConfig()

	if !cfg.Logging {
		logger := zap.NewNop()
		zap.ReplaceGlobals(logger)
		return
	} else if !cfg.Console {
		loggerCfg.OutputPaths = []string{cfg.Path}
		loggerCfg.ErrorOutputPaths = []string{cfg.Path}
	}

	if cfg.Debug {
		loggerCfg.Level.SetLevel(zapcore.DebugLevel)
	} else {
		loggerCfg.Level.SetLevel(zapcore.InfoLevel)
	}

	logger, _ := loggerCfg.Build()

	zap.ReplaceGlobals(logger)

}

func MountRemoteRoots(cfg *FsConfig) {

	// TODO Figure out more options to add
	options := []fuse.MountOption{
		fuse.NoAppleDouble(),
		fuse.NoAppleXattr(),
		fuse.VolumeName("IFS Volume"),
	}

	c, err := fuse.Mount(cfg.MountPoint, options...)
	if err != nil {
		zap.L().Fatal("Mount Failed",
			zap.Error(err),
		)
	}

	server := fs.New(c, nil)

	fileSystem := Ifs()


	remoteRootNodes := generateRemoteRoots(fileSystem, cfg.RemoteRoots)

	fileSystem.RemoteRoots = remoteRootNodes

	Talker().Startup(cfg.RemoteRoots, cfg.ConnCount)
	Hoarder().Startup(cfg.CacheLocation, 100)
	FileHandler().StartUp()

	server.Serve(fileSystem)

	<-c.Ready
	if err := c.MountError; err != nil {
		zap.L().Fatal("Mount Failed",
			zap.Error(err),
		)
	}

	c.Close()
}
