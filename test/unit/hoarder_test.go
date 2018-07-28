// +build unit

package unit

import (
	"testing"
	"ifs"
	"ifs/test"
	"strconv"
)

func TestHoarder_GetCacheFileName(t *testing.T) {

	h := ifs.Hoarder()

	for i := 0; i < 5; i++ {
		p := h.GetCacheFileName()
		test.Compare(t, p, strconv.FormatInt(int64(i+1), 10))
	}

}
