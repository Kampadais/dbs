// Copyright © 2024 FORTH-ICS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbs

import (
	"fmt"
	"io"
	"os"

	"github.com/ncw/directio"
)

// Wrapper to file object supporting direct I/O
type DirectFile struct {
	*os.File
	Name string
}

func NewDirectFile(name string, flag int, perm os.FileMode) (*DirectFile, error) {
	file, err := directio.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	df := &DirectFile{
		File: file,
		Name: name,
	}
	return df, nil
}

func (file *DirectFile) Size() (int64, error) {
	pos, err := file.File.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("cannot seek in %v: %w", file.Name, err)
	}
	return pos, nil
}

// Read using direct I/O
func (file *DirectFile) ReadAt(data []byte, offset uint64) (int, error) {
	if directio.IsAligned(data) {
		return file.File.ReadAt(data, int64(offset))
	}
	buf := directio.AlignedBlock(len(data))
	n, err := file.File.ReadAt(buf, int64(offset))
	if err == nil {
		copy(data, buf)
	}
	return n, err
}

// Write using direct I/O
func (file *DirectFile) WriteAt(data []byte, offset uint64) (int, error) {
	if directio.IsAligned(data) {
		return file.File.WriteAt(data, int64(offset))
	}
	buf := directio.AlignedBlock(len(data))
	copy(buf, data)
	return file.File.WriteAt(buf, int64(offset))
}

func (file *DirectFile) Close() error {
	// file.File.Sync()
	return file.File.Close()
}
