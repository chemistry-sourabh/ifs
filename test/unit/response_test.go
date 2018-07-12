// +build unit

package unit

import (
	"testing"
	"ifs"
	"ifs/test"
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
		test.PrintTestError(t, "strings not matching", decompressed, str)
	}
}
