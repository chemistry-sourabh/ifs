package ifs

import "testing"

func TestGetMapKey(t *testing.T) {

	str := GetMapKey(0, 10)

	if str != "0_10" {
		PrintTestError(t, "strings not matching", str, "0_10")
	}
}

