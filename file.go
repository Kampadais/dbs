package dbs

import (
	"fmt"
	"os"

	"github.com/chazapis/directio"
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
	stat, err := file.File.Stat()
	if err != nil {
		return 0, fmt.Errorf("cannot stat %v: %w", file.Name, err)
	}
	return stat.Size(), nil
}

// Read using direct I/O
func (file *DirectFile) ReadAt(data []byte, offset uint64) (int, error) {
	if directio.IsAligned(data) {
		return file.File.ReadAt(data, int64(offset))
	}
	buf := directio.AlignedBlock(len(data))
	n, err := file.File.ReadAt(buf, int64(offset))
	if err != nil {
		copy(data, buf)
	}
	return n, err
}

// Write using direct I/O,
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
