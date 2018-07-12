// +build unit

package unit

import (
	"testing"
	"ifs"
	"ifs/test"
)

func TestGetMapKey(t *testing.T) {

	str := ifs.GetMapKey("host1",0, 10)

	if str != "host1_0_10" {
		test.PrintTestError(t, "strings not matching", str, "0_10")
	}
}

