/*
 * Copyright 2019 Sourabh Bollapragada
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ifstest

import (
	"crypto/rand"
	"encoding/binary"
	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"testing"
)

func Compare(t *testing.T, got interface{}, want interface{}) {
	if !cmp.Equal(got, want) {
		_, file, line, _ := runtime.Caller(1)
		t.Errorf("Compare Failed in File: %s at Line: %d, got: %s, want: %s", file, line, got, want)
	}
}

func Err(t *testing.T, err error) {
	if err == nil {
		_, file, line, _ := runtime.Caller(1)
		t.Errorf("Didnt Get Error in File: %s at Line: %d", file, line)
	}
}

func Ok(t *testing.T, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		t.Errorf("Got Error in File: %s at Line: %d, %s", file, line, err)
	}
}

func NotNil(t *testing.T, got interface{}) {
	if got == nil {
		_, file, line, _ := runtime.Caller(1)
		t.Errorf("Got Nil in File: %s at Line: %d, %s", file, line, got)
	}
}


func CreateTempFile(name string) {
	fPath := path.Join("/tmp", name)
	f, _ := os.Create(fPath)
	defer f.Close()
}

func RemoveTempFile(name string) {
	fPath := path.Join("/tmp", name)
	os.Remove(fPath)
}

func WriteDummyData(name string, size int) []byte {
	fPath := path.Join("/tmp", name)
	data := make([]byte, size)
	binary.Read(rand.Reader, binary.LittleEndian, &data)
	ioutil.WriteFile(fPath, data, 0666)
	return data
}

func SetupLogger() {
	logCfg := zap.NewDevelopmentConfig()
	logger, _ := logCfg.Build()
	zap.ReplaceGlobals(logger)
}