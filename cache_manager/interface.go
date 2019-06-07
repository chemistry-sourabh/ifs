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
	"github.com/chemistry-sourabh/ifs/structure"
)

type CacheManager interface {
	Attr(filePath *structure.RemotePath) (*structure.FileInfo, error)
	Open(path *structure.RemotePath, flags uint32) (uint64, error)
	Rename(path *structure.RemotePath, dst string) error
	Truncate(path *structure.RemotePath, size uint64) error
	Create(dirPath *structure.RemotePath, name string) (uint64, error)
	Remove(path *structure.RemotePath) error
	ReadDir(path *structure.RemotePath) ([]*structure.FileInfo, error)

	// fd functions
	Read(fd uint64, offset uint64, size uint64) ([]byte, error)
	Write(fd uint64, offset uint64, data []byte) (int, error)
	Close(fd uint64) error
	Flush(fd uint64) error

	Run(path string, size uint64)
}
