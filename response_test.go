// +build unit

package ifs_test

import (
	"testing"
	"ifs"
	)

func TestFileChunk_Compress_Decompress(t *testing.T) {
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
