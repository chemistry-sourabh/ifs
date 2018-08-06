// +build unit

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

	// Case 1
	path := "/home/sourabh/hello.py"
	firstDir := ifs.FirstDir(path)
	Compare(t, firstDir, "home")

	// Case 2
	path = "home/sourabh/hello.py"
	firstDir = ifs.FirstDir(path)
	Compare(t, firstDir, "home")

	// Case 3
	path = "home"
	firstDir = ifs.FirstDir(path)
	Compare(t, firstDir, "home")

}

func TestRemoveFirstDir(t *testing.T) {

	// Case 1
	path := "/home/sourabh/hello.py"
	newPath := ifs.RemoveFirstDir(path)
	Compare(t, newPath, "sourabh/hello.py")

	// Case 2
	path = "home/sourabh/hello.py"
	newPath = ifs.RemoveFirstDir(path)
	Compare(t, newPath, "sourabh/hello.py")

	// Case 3
	path = "home"
	newPath = ifs.RemoveFirstDir(path)
	Compare(t, newPath, "")

}
