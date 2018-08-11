/*
Copyright 2018 Sourabh Bollapragada

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package ifs_test

import (
	"testing"
	"github.com/chemistry-sourabh/ifs"
)

func TestFirstDir(t *testing.T) {

	path := "/home/sourabh/hello.py"

	firstDir := ifs.FirstDir(path)

	if firstDir != "home" {
		PrintTestError(t, "Dont Match", firstDir, "home")
	}

	path = "home/sourabh/hello.py"

	firstDir = ifs.FirstDir(path)

	if firstDir != "home" {
		PrintTestError(t, "Dont Match", firstDir, "home")
	}

	path = "home"

	firstDir = ifs.FirstDir(path)

	if firstDir != "home" {
		PrintTestError(t, "Dont Match", firstDir, "home")
	}

}

func TestRemoveFirstDir(t *testing.T) {

	path := "/home/sourabh/hello.py"

	newPath := ifs.RemoveFirstDir(path)

	if newPath != "sourabh/hello.py" {
		PrintTestError(t, "Dont Match", newPath, "sourabh/hello.py")
	}

	path = "home/sourabh/hello.py"

	newPath = ifs.RemoveFirstDir(path)

	if newPath != "sourabh/hello.py" {
		PrintTestError(t, "Dont Match", newPath, "sourabh/hello.py")
	}

	path = "home"

	newPath = ifs.RemoveFirstDir(path)

	if newPath != "" {
		PrintTestError(t, "Dont Match", newPath, "")
	}

}
