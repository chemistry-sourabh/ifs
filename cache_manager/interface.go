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

package cache_manager

import (
	"github.com/chemistry-sourabh/ifs/structures"
)

type CacheManager interface {
	Open(path *structures.RemotePath, flags uint32) (uint64, error)
	Rename(path *structures.RemotePath, dst string) error
	Truncate(path *structures.RemotePath) error
	Remove(path *structures.RemotePath) error

	// fd functions
	Read(fd uint64)
	Write(fd uint64)
	Close(fd uint64)

	Run(path string, size uint64)
}

//func NewCacheManager() *CacheManager {
//	return NewDiskCacheManager()
//}
