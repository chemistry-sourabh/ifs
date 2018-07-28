// +build unit

package ifs_test

import (
	"testing"
	"ifs"
	)

func TestGetMapKey(t *testing.T) {

	str := ifs.GetMapKey("host1",0, 10)

	if str != "host1_0_10" {
		PrintTestError(t, "strings not matching", str, "0_10")
	}
}

