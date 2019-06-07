///*
//Copyright 2018 Sourabh Bollapragada
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//*/
//
package ifs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/chemistry-sourabh/ifs/cache_manager"
	"github.com/chemistry-sourabh/ifs/communicator"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	fuseServerInstance *fs.Server
)

func FuseServer() *fs.Server {
	return fuseServerInstance
}

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

	//sigChan := make(chan os.Signal, 1)
	//signal.Notify(sigChan, os.Interrupt)
	//
	//go func() {
	//	for range sigChan {
	//		err := fuse.Unmount(cfg.MountPoint)
	//
	//		if err == nil {
	//			zap.L().Info("Unmounted Successfully")
	//		} else {
	//			zap.L().Warn("Unmount Failed",
	//				zap.Error(err),
	//			)
	//		}
	//	}
	//}()

	// TODO Figure out more options to add
	options := []fuse.MountOption{
		fuse.NoAppleDouble(),
		fuse.NoAppleXattr(),
		fuse.VolumeName("IFS Volume"),
	}

	c, err := fuse.Mount(cfg.MountPoint, options...)
	defer c.Close()

	if err != nil {
		zap.L().Fatal("Mount Failed",
			zap.Error(err),
		)
	}

	fuseServerInstance = fs.New(c, nil)

	var addresses [] string

	for _, remoteRoot := range cfg.RemoteRoots {
		addresses = append(addresses, remoteRoot.Address())
	}

	comm := communicator.NewFsZmqSender("127.0.0.1:5000")
	comm.Connect(addresses)
	cache := cache_manager.NewDiskCacheManager()
	cache.Sender = comm
	root := NewRoot(cfg.RemoteRoots,cache)
	//Ifs().Connect(cfg.RemoteRoots)
	//Talker().Connect(cfg.RemoteRoots, cfg.ConnCount)
	//Hoarder().Connect(cfg.CacheLocation, 100)
	//FileHandler().StartUp()

	FuseServer().Serve(root)

	<-c.Ready
	if err := c.MountError; err != nil {
		zap.L().Fatal("Mount Failed",
			zap.Error(err),
		)
	}

	zap.L().Core().Sync()
}
