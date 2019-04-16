// +build unit

/*
 * Copyright 2019 Sourabh Bollapragada
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cache_manager_test

import (
	"github.com/chemistry-sourabh/ifs/cache_manager"
	"github.com/chemistry-sourabh/ifs/ifstest"
	"github.com/chemistry-sourabh/ifs/network_manager"
	"github.com/chemistry-sourabh/ifs/structures"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

//func TestHoarder_GetCacheFileName(t *testing.T) {
//
//	h := ifs.Hoarder()
//
//	for i := 0; i < 5; i++ {
//		p := h.GetCacheFileName()
//		Compare(t, p, strconv.FormatInt(int64(i+2), 10))
//	}
//
//}

func TestDiskCacheManager_Open(t *testing.T) {

	cachePath := "/tmp/test_cache"

	dcm := cache_manager.NewDiskCacheManager()
	dcm.Nm = &network_manager.TestNetworkManager{}
	dcm.Startup(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	f, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	ifstest.Compare(t, f.Name(), path.Join(cachePath, "2"))

	data, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)
	ifstest.Compare(t, len(data), 100)

	err = f.Close()
	ifstest.Ok(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

}

func TestDiskCacheManager_Open2(t *testing.T) {
	cachePath := "/tmp/test_cache"

	dcm := cache_manager.NewDiskCacheManager()
	dcm.Nm = &network_manager.TestNetworkManager{}
	dcm.Startup(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	f, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	data, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	err = f.Close()
	ifstest.Ok(t, err)

	f, err = dcm.Open(rp, os.O_RDONLY)
	ifstest.Ok(t, err)

	data1, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	ifstest.Compare(t, data1, data)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Rename(t *testing.T) {
	cachePath := "/tmp/test_cache"

	dcm := cache_manager.NewDiskCacheManager()
	dcm.Nm = &network_manager.TestNetworkManager{}
	dcm.Startup(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	f, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	data, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	err = f.Close()
	ifstest.Ok(t, err)

	err = dcm.Rename(rp, "/tmp/test1")
	ifstest.Ok(t, err)

	rp.Path = "/tmp/test1"

	f, err = dcm.Open(rp, os.O_RDONLY)
	ifstest.Ok(t, err)

	data1, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	ifstest.Compare(t, data1, data)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}
