// +build unit

/*
Copyright 2018 Sourabh Bollapragada

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package ifs_test

import (
	"testing"
	"github.com/chemistry-sourabh/ifs"
	)

func TestFileChunk_Compress_Decompress(t *testing.T) {
	t.SkipNow()
	str := "hello world!! Bye World!!!"

	bytes := []byte(str)

	fileChunk := &ifs.FileChunk{
		Chunk: bytes,
		Size:  len(str),
	}

	fileChunk.Compress()
	fileChunk.Decompress()

	decompressed := string(fileChunk.Chunk)
	if str != decompressed {
		PrintTestError(t, "strings not matching", decompressed, str)
	}
}
