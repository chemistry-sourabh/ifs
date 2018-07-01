// +build unit

package ifs

import "testing"

func TestGetMapKey(t *testing.T) {

	str := GetMapKey("host1",0, 10)

	if str != "host1_0_10" {
		PrintTestError(t, "strings not matching", str, "0_10")
	}
}

