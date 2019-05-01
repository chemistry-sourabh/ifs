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

package structures

import (
	"bytes"
	"compress/zlib"
	"go.uber.org/zap"
	"io"
)

// TODO Skip compression if file is too small
func (fm *FileMessage) Compress() {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err := w.Write(fm.File)
	if err != nil {
		zap.L().Fatal("Compression Failed",
			zap.Error(err),
		)
	}

	err = w.Close()
	if err != nil {
		zap.L().Warn("Couldnt Close Writer",
			zap.Error(err),
		)
	}

	fm.File = b.Bytes()
}

func (fm *FileMessage) Decompress() {
	var b bytes.Buffer
	b.Write(fm.File)
	r, err := zlib.NewReader(&b)

	if err != nil && err != io.EOF {
		zap.L().Fatal("Decompression Failed",
			zap.Error(err),
		)
	}

	var out bytes.Buffer
	_, err = out.ReadFrom(r)
	if err != nil {
		zap.L().Fatal("Decompression Failed",
			zap.Error(err),
		)
	}

	fm.File = out.Bytes()
	err = r.Close()

	if err != nil {
		zap.L().Warn("Decompression Failed",
			zap.Error(err),
		)
	}
}
