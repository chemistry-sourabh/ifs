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

package ifs

import (
	"math/rand"
	"os"
	"path"
	"strings"
	"time"
)

func ConvertOpCodeToString(opCode uint8) string {

	switch opCode {
	case AttrRequest:
		return "Attr Request"
	case ReadDirRequest:
		return "ReadDir Request"
	case ReadDirAllRequest:
		return "ReadDirAll Request"
	case FetchFileRequest:
		return "FetchFile Request"
	case ReadFileRequest:
		return "ReadFile Request"
	case WriteFileRequest:
		return "WriteFile Request"
	case SetAttrRequest:
		return "SetAttr Request"
	case CreateRequest:
		return "Create Request"
	case RemoveRequest:
		return "Remove Request"
	case RenameRequest:
		return "Rename Request"
	case OpenRequest:
		return "Open Request"
	case CloseRequest:
		return "Close Request"

	case StatResponse:
		return "Stat Response"
	case StatsResponse:
		return "Stats Response"
	case FileDataResponse:
		return "FileData Response"
	case WriteResponse:
		return "Write Response"
	case ErrorResponse:
		return "Error Response"
	}

	return "Unknown Op"
}

func GetRandomIndex(length int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(length)
}

func ConvertErr(err error) error {
	switch t := err.(type) {
	case *os.PathError:
		return t.Err
	case *os.LinkError:
		return t.Err
	default:
		return err

	}
}

func FirstDir(path string) string {
	parts := strings.Split(path, "/")

	firstDir := parts[0]

	if firstDir == "" {
		firstDir = parts[1]
	}

	return firstDir
}

func RemoveFirstDir(filePath string) string {
	parts := strings.Split(filePath, "/")

	index := 0

	if parts[index] == "" {
		index = 2
	} else {
		index = 1
	}

	return path.Join(parts[index:]...)
}
