// +build unit

package ifs_test

import (
	"testing"
	"ifs"
	"strconv"
)

func TestHoarder_GetCacheFileName(t *testing.T) {

	h := ifs.Hoarder()

	for i := 0; i < 5; i++ {
		p := h.GetCacheFileName()
		Compare(t, p, strconv.FormatInt(int64(i+1), 10))
	}

}
