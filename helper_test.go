package ifs

import "testing"

func TestFirstDir(t *testing.T) {

	path := "/home/sourabh/hello.py"

	firstDir := FirstDir(path)

	if firstDir != "home" {
		PrintTestError(t, "Dont Match", firstDir, "home")
	}

	path = "home/sourabh/hello.py"

	firstDir = FirstDir(path)

	if firstDir != "home" {
		PrintTestError(t, "Dont Match", firstDir, "home")
	}

	path = "home"

	firstDir = FirstDir(path)

	if firstDir != "home" {
		PrintTestError(t, "Dont Match", firstDir, "home")
	}

}

func TestRemoveFirstDir(t *testing.T) {

	path := "/home/sourabh/hello.py"

	newPath := RemoveFirstDir(path)

	if newPath != "sourabh/hello.py" {
		PrintTestError(t, "Dont Match", newPath, "sourabh/hello.py")
	}

	path = "home/sourabh/hello.py"

	newPath = RemoveFirstDir(path)

	if newPath != "sourabh/hello.py" {
		PrintTestError(t, "Dont Match", newPath, "sourabh/hello.py")
	}

	path = "home"

	newPath = RemoveFirstDir(path)

	if newPath != "" {
		PrintTestError(t, "Dont Match", newPath, "")
	}

}
