package unit

import (
	"testing"
	"ifs"
	"ifs/test"
)

func TestFirstDir(t *testing.T) {

	path := "/home/sourabh/hello.py"

	firstDir := ifs.FirstDir(path)

	if firstDir != "home" {
		test.PrintTestError(t, "Dont Match", firstDir, "home")
	}

	path = "home/sourabh/hello.py"

	firstDir = ifs.FirstDir(path)

	if firstDir != "home" {
		test.PrintTestError(t, "Dont Match", firstDir, "home")
	}

	path = "home"

	firstDir = ifs.FirstDir(path)

	if firstDir != "home" {
		test.PrintTestError(t, "Dont Match", firstDir, "home")
	}

}

func TestRemoveFirstDir(t *testing.T) {

	path := "/home/sourabh/hello.py"

	newPath := ifs.RemoveFirstDir(path)

	if newPath != "sourabh/hello.py" {
		test.PrintTestError(t, "Dont Match", newPath, "sourabh/hello.py")
	}

	path = "home/sourabh/hello.py"

	newPath = ifs.RemoveFirstDir(path)

	if newPath != "sourabh/hello.py" {
		test.PrintTestError(t, "Dont Match", newPath, "sourabh/hello.py")
	}

	path = "home"

	newPath = ifs.RemoveFirstDir(path)

	if newPath != "" {
		test.PrintTestError(t, "Dont Match", newPath, "")
	}

}
