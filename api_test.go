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
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"golang.org/x/exp/slices"
	. "gopkg.in/check.v1"
)

const (
	MEGABYTE = 1024 * 1024
	GIGABYTE = MEGABYTE * 1024

	DEVICE                = "test.img"
	DEVICE_SIZE           = MEGABYTE * 100
	SECONDARY_DEVICE_PATH = "test_secondary.img"
	SECONDARY_DEVICE_SIZE = MEGABYTE * 50
)

func Test(t *testing.T) {
	err := InitDevice(DEVICE, SECONDARY_DEVICE_PATH)
	if err != nil {
		fmt.Println("Failed to initialize device:", err)
		return
	}
	TestingT(t)
}

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestDevice(c *C) {
	err := InitDevice(DEVICE, SECONDARY_DEVICE_PATH)
	c.Assert(err, IsNil)
	deviceInfo, err := GetDeviceInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(deviceInfo.AllocatedDeviceExtents, Equals, uint(0))
	c.Assert(deviceInfo.VolumeCount, Equals, uint(0))
	volumeInfo, err := GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 0)
}

func assertVolume(c *C, vi *VolumeInfo, volumeName string, volumeSize uint64, snapshotCount uint16) {
	c.Assert(vi.VolumeName, Equals, volumeName)
	c.Assert(vi.VolumeSize, Equals, volumeSize)
	if time.Now().Add(-5 * time.Minute).After(vi.CreatedAt) {
		c.FailNow()
	}
	c.Assert(vi.VolumeName, Equals, volumeName)
}

func (s *TestSuite) TestVolume(c *C) {
	err := InitDevice(DEVICE, SECONDARY_DEVICE_PATH)
	c.Assert(err, IsNil)

	// Create a volume
	err = CreateVolume(DEVICE, "vol1", GIGABYTE)
	c.Assert(err, IsNil)
	volumeInfo, err := GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 1)
	assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)

	// Create multiple volumes
	err = CreateVolume(DEVICE, "vol1", GIGABYTE)
	c.Assert(err, NotNil)
	err = CreateVolume(DEVICE, "vol2", 2*GIGABYTE)
	c.Assert(err, IsNil)
	err = CreateVolume(DEVICE, "vol3", 3*GIGABYTE)
	c.Assert(err, IsNil)
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 3)
	assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
	assertVolume(c, &volumeInfo[1], "vol2", 2*GIGABYTE, 1)
	assertVolume(c, &volumeInfo[2], "vol3", 3*GIGABYTE, 1)

	// Delete a volume
	err = DeleteVolume(DEVICE, "vol2")
	c.Assert(err, IsNil)
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 2)
	assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
	assertVolume(c, &volumeInfo[1], "vol3", 3*GIGABYTE, 1)
	err = DeleteVolume(DEVICE, "vol2")
	c.Assert(err, NotNil)

	// Create volume again (goes in empty spot)
	err = CreateVolume(DEVICE, "vol2new", 2*GIGABYTE)
	c.Assert(err, IsNil)
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 3)
	assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
	assertVolume(c, &volumeInfo[1], "vol2new", 2*GIGABYTE, 1)
	assertVolume(c, &volumeInfo[2], "vol3", 3*GIGABYTE, 1)

	// Rename volume
	err = RenameVolume(DEVICE, "vol2new", "vol2renamed")
	c.Assert(err, IsNil)
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 3)
	assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
	assertVolume(c, &volumeInfo[1], "vol2renamed", 2*GIGABYTE, 1)
	assertVolume(c, &volumeInfo[2], "vol3", 3*GIGABYTE, 1)

	// Delete multiple volumes
	err = DeleteVolume(DEVICE, "vol2renamed")
	c.Assert(err, IsNil)
	err = DeleteVolume(DEVICE, "vol3")
	c.Assert(err, IsNil)
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 1)
	assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
	err = DeleteVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 0)
}

func (s *TestSuite) TestSnapshot(c *C) {
	err := InitDevice(DEVICE, SECONDARY_DEVICE_PATH)
	c.Assert(err, IsNil)

	// Create a volume
	err = CreateVolume(DEVICE, "vol1", GIGABYTE)
	c.Assert(err, IsNil)
	volumeInfo, err := GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	volumeSnapshotIdx := slices.IndexFunc(volumeInfo, func(vi VolumeInfo) bool { return vi.VolumeName == "vol1" })
	volumeSnapshotId := volumeInfo[volumeSnapshotIdx].SnapshotId
	snapshotInfo, err := GetSnapshotInfo(DEVICE, "vol1")
	c.Assert(err, IsNil)
	c.Assert(snapshotInfo, HasLen, 1)
	if snapshotInfo[0].SnapshotId == 0 {
		c.FailNow()
	}
	c.Assert(snapshotInfo[0].ParentSnapshotId, Equals, uint(0))
	initialSnapshotId := snapshotInfo[0].SnapshotId
	c.Assert(volumeSnapshotId, Equals, initialSnapshotId)

	// Create a snapshot
	labels := map[string]string{
		"snap1": "first",
	}

	err = CreateSnapshot(DEVICE, "vol1", true, time.Now().Format(time.RFC3339), labels)
	c.Assert(err, IsNil)
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	volumeSnapshotIdx = slices.IndexFunc(volumeInfo, func(vi VolumeInfo) bool { return vi.VolumeName == "vol1" })
	volumeSnapshotId = volumeInfo[volumeSnapshotIdx].SnapshotId
	if volumeSnapshotId == initialSnapshotId {
		c.FailNow()
	}
	snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
	c.Assert(err, IsNil)
	c.Assert(snapshotInfo, HasLen, 2)
	initialSnapshotIdx := slices.IndexFunc(snapshotInfo, func(si SnapshotInfo) bool { return si.ParentSnapshotId == 0 })
	if initialSnapshotIdx == -1 {
		c.FailNow()
	}
	initialSnapshot := snapshotInfo[initialSnapshotIdx]
	c.Assert(initialSnapshot.SnapshotId, Equals, initialSnapshotId)
	currentSnapshotIdx := slices.IndexFunc(snapshotInfo, func(si SnapshotInfo) bool { return si.ParentSnapshotId != 0 })
	if currentSnapshotIdx == -1 {
		c.FailNow()
	}
	currentSnapshot := snapshotInfo[currentSnapshotIdx]
	c.Assert(currentSnapshot.SnapshotId, Equals, volumeSnapshotId)
	c.Assert(currentSnapshot.ParentSnapshotId, Equals, initialSnapshotId)

	err = CreateSnapshot(DEVICE, "vol1", true, time.Now().Format(time.RFC3339), labels)
	c.Assert(err, IsNil)
	labels = map[string]string{
		"snap3": "third",
		"foo":   "bar",
	}
	err = CreateSnapshot(DEVICE, "vol1", true, time.Now().Format(time.RFC3339), nil)
	c.Assert(err, IsNil)
	labels = map[string]string{
		"snap4": "fourth",
	}
	err = CreateSnapshot(DEVICE, "vol1", true, time.Now().Format(time.RFC3339), labels)
	c.Assert(err, IsNil)
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	volumeSnapshotIdx = slices.IndexFunc(volumeInfo, func(vi VolumeInfo) bool { return vi.VolumeName == "vol1" })
	volumeSnapshotId = volumeInfo[volumeSnapshotIdx].SnapshotId
	if volumeSnapshotId == initialSnapshotId {
		c.FailNow()
	}
	snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
	c.Assert(err, IsNil)
	c.Assert(snapshotInfo, HasLen, 5)
	initialSnapshotIdx = slices.IndexFunc(snapshotInfo, func(si SnapshotInfo) bool { return si.ParentSnapshotId == 0 })
	if initialSnapshotIdx == -1 {
		c.FailNow()
	}
	initialSnapshot = snapshotInfo[initialSnapshotIdx]
	c.Assert(initialSnapshot.SnapshotId, Equals, initialSnapshotId)
	currentSnapshotIdx = slices.IndexFunc(snapshotInfo, func(si SnapshotInfo) bool { return si.ParentSnapshotId != volumeSnapshotId })
	if currentSnapshotIdx == -1 {
		c.FailNow()
	}
	currentSnapshot = snapshotInfo[currentSnapshotIdx]
	c.Assert(currentSnapshot.SnapshotId, Equals, volumeSnapshotId)
	if currentSnapshot.ParentSnapshotId == initialSnapshotId {
		c.FailNow()
	}

	// Delete a snapshot
	err = DeleteSnapshot(DEVICE, currentSnapshot.SnapshotId)
	c.Assert(err, NotNil)
	snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
	c.Assert(err, IsNil)
	c.Assert(snapshotInfo, HasLen, 5)
	err = DeleteSnapshot(DEVICE, initialSnapshot.SnapshotId)
	c.Assert(err, IsNil)
	snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
	c.Assert(err, IsNil)
	c.Assert(snapshotInfo, HasLen, 4)

	// Create snapshot again
	labels = map[string]string{
		"snap5": "fifth",
	}
	err = CreateSnapshot(DEVICE, "vol1", true, time.Now().Format(time.RFC3339), labels)
	c.Assert(err, IsNil)
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	volumeSnapshotIdx = slices.IndexFunc(volumeInfo, func(vi VolumeInfo) bool { return vi.VolumeName == "vol1" })
	volumeSnapshotId = volumeInfo[volumeSnapshotIdx].SnapshotId
	if volumeSnapshotId == currentSnapshot.SnapshotId {
		c.FailNow()
	}
	snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
	c.Assert(err, IsNil)
	c.Assert(snapshotInfo, HasLen, 5)

	// Delete multiple snapshots
	for i, _ := range snapshotInfo {
		if snapshotInfo[i].SnapshotId == volumeSnapshotId {
			continue
		}
		err = DeleteSnapshot(DEVICE, snapshotInfo[i].SnapshotId)
		c.Assert(err, IsNil)
	}
	snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
	c.Assert(err, IsNil)
	c.Assert(snapshotInfo, HasLen, 1)
	c.Assert(snapshotInfo[0].SnapshotId, Equals, volumeSnapshotId)
	c.Assert(snapshotInfo[0].ParentSnapshotId, Equals, uint(0))

	// Clone latest snapshot
	err = CloneSnapshot(DEVICE, "vol2cloned", volumeSnapshotId)
	c.Assert(err, IsNil)
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 2)
	assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
	assertVolume(c, &volumeInfo[1], "vol2cloned", GIGABYTE, 1)
	err = DeleteVolume(DEVICE, "vol2cloned")
	c.Assert(err, IsNil)
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 1)

	// Snapshot and clone both the previous snapshot and latest snapshot
	err = CreateSnapshot(DEVICE, "vol1", true, time.Now().Format(time.RFC3339), labels)
	c.Assert(err, IsNil)
	snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
	c.Assert(err, IsNil)
	for i, _ := range snapshotInfo {
		err = CloneSnapshot(DEVICE, fmt.Sprintf("vol2clone%d", i+1), snapshotInfo[i].SnapshotId)
		c.Assert(err, IsNil)
	}
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 3)
	assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
	assertVolume(c, &volumeInfo[1], "vol2clone1", GIGABYTE, 1)
	assertVolume(c, &volumeInfo[2], "vol2clone2", GIGABYTE, 1)

	// Clean up
	err = DeleteVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)
	err = DeleteVolume(DEVICE, "vol2clone1")
	c.Assert(err, IsNil)
	err = DeleteVolume(DEVICE, "vol2clone2")
	c.Assert(err, IsNil)
	volumeInfo, err = GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 0)
}

func loadBlocks() [][]byte {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("")
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		panic("")
	}
	blockCount := len(data) / BLOCK_SIZE
	blockData := make([][]byte, blockCount)
	for i := 0; i < blockCount; i++ {
		blockData[i] = data[i*BLOCK_SIZE : (i+1)*BLOCK_SIZE]
	}
	return blockData
}

func readBlocks(c *C, vc *VolumeContext, blockIndices []int, blockData [][]byte) {
	data := make([]byte, BLOCK_SIZE)
	blockCount := len(blockData)
	for i, _ := range blockIndices {
		err := vc.ReadBlock(data, uint64(blockIndices[i]))
		c.Assert(err, IsNil)
		c.Assert(data, DeepEquals, blockData[i%blockCount])
	}
}

func writeBlocks(c *C, vc *VolumeContext, blockIndices []int, blockData [][]byte) {
	blockCount := len(blockData)
	for i, _ := range blockIndices {
		err := vc.WriteBlock(blockData[i%blockCount], uint64(blockIndices[i]), true)
		c.Assert(err, IsNil)
	}
}

func unmapBlocks(c *C, vc *VolumeContext, blockIndices []int) {
	for i, _ := range blockIndices {
		err := vc.UnmapBlock(uint64(blockIndices[i]))
		c.Assert(err, IsNil)
	}
}

func (s *TestSuite) TestVolumeIO(c *C) {
	err := InitDevice(DEVICE, SECONDARY_DEVICE_PATH)
	c.Assert(err, IsNil)

	repeats := 10
	spread := 100
	positions := []int{0, 3, 43, 53, 92}

	blockData := loadBlocks()
	blockIndices := make([]int, len(positions)*repeats)
	i := 0
	for r := 0; r < repeats; r++ {
		for _, p := range positions {
			blockIndices[i] = p + (r * spread)
			i++
		}
	}

	// Create a volume and open it
	err = CreateVolume(DEVICE, "vol1", GIGABYTE)
	c.Assert(err, IsNil)
	vc, err := OpenVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)

	// Read (should get empty data)
	emptyBlock := make([]byte, BLOCK_SIZE)
	readBlocks(c, vc, blockIndices, [][]byte{emptyBlock})

	// Write and read back
	writeBlocks(c, vc, blockIndices, blockData)
	readBlocks(c, vc, blockIndices, blockData)

	// Read other (should get empty data)
	otherBlockIndices := make([]int, len(blockIndices)*2)
	for i := 0; i < len(blockIndices)*2; i += 2 {
		otherBlockIndices[i] = blockIndices[i/2] - 1
		otherBlockIndices[i+1] = blockIndices[i/2] + 1
	}
	sort.Ints(otherBlockIndices[:])
	otherBlockIndices = otherBlockIndices[1:]
	readBlocks(c, vc, otherBlockIndices, [][]byte{emptyBlock})

	// Unmap and read back
	unmapBlocks(c, vc, blockIndices)
	readBlocks(c, vc, blockIndices, [][]byte{emptyBlock})
	vc.CloseVolume()

	// Validate metadata and clean up
	volumeInfo, err := GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 1)
	assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
	err = DeleteVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestSnapshotIO(c *C) {
	err := InitDevice(DEVICE, SECONDARY_DEVICE_PATH)
	c.Assert(err, IsNil)

	repeats := 10
	// ... rest of the setup
	spread := 100
	positions := []int{0, 3, 43, 53, 92}

	blockData := loadBlocks()
	blockIndices := make([]int, len(positions)*repeats)
	i := 0
	for r := 0; r < repeats; r++ {
		for _, p := range positions {
			blockIndices[i] = p + (r * spread)
			i++
		}
	}

	// Create a volume and open it
	err = CreateVolume(DEVICE, "vol1", GIGABYTE)
	c.Assert(err, IsNil)
	vc, err := OpenVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)

	// Write
	writeBlocks(c, vc, blockIndices, blockData)
	vc.CloseVolume()

	// Snapshot, open again and read back
	err = CreateSnapshot(DEVICE, "vol1", true, time.Now().Format(time.RFC3339), nil)
	c.Assert(err, IsNil)
	vc, err = OpenVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)
	readBlocks(c, vc, blockIndices, blockData)

	// Overwrite and read back
	dummyBlock := make([]byte, BLOCK_SIZE)
	for i := 0; i < BLOCK_SIZE; i++ {
		dummyBlock[i] = 0xF0
	}
	writeBlocks(c, vc, blockIndices, [][]byte{dummyBlock})
	readBlocks(c, vc, blockIndices, [][]byte{dummyBlock})
	vc.CloseVolume()

	// Clone volume and open
	snapshotInfo, err := GetSnapshotInfo(DEVICE, "vol1")
	c.Assert(err, IsNil)
	c.Assert(snapshotInfo, HasLen, 2)
	initialSnapshotIdx := slices.IndexFunc(snapshotInfo, func(si SnapshotInfo) bool { return si.ParentSnapshotId == 0 })
	if initialSnapshotIdx == -1 {
		c.FailNow()
	}
	initialSnapshotId := snapshotInfo[initialSnapshotIdx].SnapshotId
	err = CloneSnapshot(DEVICE, "vol1clone", initialSnapshotId)
	c.Assert(err, IsNil)
	vc, err = OpenVolume(DEVICE, "vol1clone")
	c.Assert(err, IsNil)

	// Read original blocks from clone
	readBlocks(c, vc, blockIndices, blockData)
	vc.CloseVolume()

	// Delete initial snapshot, open again and read back
	err = DeleteSnapshot(DEVICE, initialSnapshotId)
	c.Assert(err, IsNil)
	vc, err = OpenVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)
	readBlocks(c, vc, blockIndices, [][]byte{dummyBlock})

	// Validate metadata and clean up
	volumeInfo, err := GetVolumeInfo(DEVICE)
	c.Assert(err, IsNil)
	c.Assert(volumeInfo, HasLen, 2)
	assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
	assertVolume(c, &volumeInfo[1], "vol1clone", GIGABYTE, 1)
	err = DeleteVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)
	err = DeleteVolume(DEVICE, "vol1clone")
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestMigration(c *C) {
	err := InitDevice(DEVICE, SECONDARY_DEVICE_PATH)
	c.Assert(err, IsNil)

	repeats := 10
	// ... rest of the setup
	spread := 100
	positions := []int{0, 3, 43, 53, 92}

	blockData := loadBlocks()
	blockIndices := make([]int, len(positions)*repeats)
	i := 0
	for r := 0; r < repeats; r++ {
		for _, p := range positions {
			blockIndices[i] = p + (r * spread)
			i++
		}
	}

	// Create a volume and open it
	err = CreateVolume(DEVICE, "vol1", GIGABYTE)
	c.Assert(err, IsNil)
	vc, err := OpenVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)

	// Write
	writeBlocks(c, vc, blockIndices, blockData)
	vc.CloseVolume()

	// Snapshot, open again and read back
	err = CreateSnapshot(DEVICE, "vol1", true, time.Now().Format(time.RFC3339), nil)
	c.Assert(err, IsNil)
	vc, err = OpenVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)
	readBlocks(c, vc, blockIndices, blockData)

	//Migrate volume and read back
	err = MigrateVolume(DEVICE, "vol1", "default")
	c.Assert(err, IsNil)
	vc.CloseVolume()
	vc, err = OpenVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)
	readBlocks(c, vc, blockIndices, blockData)

	// Overwrite first and last blocks and read back
	dummyBlock := make([]byte, BLOCK_SIZE)
	for i := 0; i < BLOCK_SIZE; i++ {
		dummyBlock[i] = 0xF0
	}

	writeBlocks(c, vc, []int{blockIndices[0], blockIndices[len(blockIndices)-1]}, [][]byte{dummyBlock, dummyBlock})
	vc.CloseVolume()
	vc, err = OpenVolume(DEVICE, "vol1")

	// Create a merged expected block slice for verification
	mergedBlocks := make([][]byte, len(blockIndices))
	blockCount := len(blockData)
	for i := range blockIndices {
		mergedBlocks[i] = blockData[i%blockCount]
	}

	// Overwrite first and last blocks with dummyBlock
	mergedBlocks[0] = dummyBlock
	mergedBlocks[len(blockIndices)-1] = dummyBlock

	// Now read and verify against the mergedBlocks
	readBlocks(c, vc, blockIndices, mergedBlocks)

	err = DeleteVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)

}

func (s *TestSuite) TestSnapshotDelete(c *C) {

	const (
		EXTENTS        = 5
		EXTENT_SIZE_MB = 2
		DEVICE_NAME    = DEVICE
		VOL_NAME       = "vol_snap_chain"
		CYCLES         = 1
	)
	defer DeleteVolume(DEVICE, VOL_NAME)

	extentSize := EXTENT_SIZE_MB * MEGABYTE
	blocksPerExtent := extentSize / BLOCK_SIZE
	if blocksPerExtent == 0 {
		c.Fatalf("invalid extent size or BLOCK_SIZE")
	}

	// -----------------------------
	// 1. Create volume
	// -----------------------------
	err := InitDevice(DEVICE_NAME, SECONDARY_DEVICE_PATH)
	c.Assert(err, IsNil)

	err = CreateVolume(DEVICE_NAME, VOL_NAME, 4*GIGABYTE)
	c.Assert(err, IsNil)

	vc, err := OpenVolume(DEVICE_NAME, VOL_NAME)
	c.Assert(err, IsNil)

	// logical extent start block index per extent
	extentStartBlock := func(extentIdx int) int {
		return extentIdx * blocksPerExtent
	}

	// Initial extents
	initialExtents := make([][]byte, EXTENTS)
	for i := 0; i < EXTENTS; i++ {
		buf := make([]byte, extentSize)
		fill := byte((i + 1) & 0xFF)
		for j := range buf {
			buf[j] = fill
		}
		initialExtents[i] = buf
	}

	// Helper: expand extents into blocks
	expandExtentsToBlocks := func(extentIndices []int, extentBuffers [][]byte) ([]int, [][]byte) {
		var blockIndices []int
		var blocks [][]byte

		for ei, extIdx := range extentIndices {
			buf := extentBuffers[ei]
			if len(buf) != extentSize {
				c.Fatalf("expandExtentsToBlocks: buffer size mismatch")
			}
			startBlock := extentStartBlock(extIdx)
			for b := 0; b < blocksPerExtent; b++ {
				blockIndices = append(blockIndices, startBlock+b)
				sb := make([]byte, BLOCK_SIZE)
				copy(sb, buf[b*BLOCK_SIZE:(b+1)*BLOCK_SIZE])
				blocks = append(blocks, sb)
			}
		}
		return blockIndices, blocks
	}

	// -----------------------------
	// 2. Initial write
	// -----------------------------
	allExtentIdxs := make([]int, EXTENTS)
	for i := 0; i < EXTENTS; i++ {
		allExtentIdxs[i] = i
	}

	blkIdxs, blkPayloads := expandExtentsToBlocks(allExtentIdxs, initialExtents)
	writeBlocks(c, vc, blkIdxs, blkPayloads)

	vc.CloseVolume()

	// Prepare overwrite logic
	overwriteSize := EXTENTS / 2
	if overwriteSize == 0 {
		overwriteSize = 1
	}
	slideStep := overwriteSize / 2
	if slideStep == 0 {
		slideStep = 1
	}

	// Record final expected extents
	expectedFinalExtents := make([][]byte, EXTENTS)
	copy(expectedFinalExtents, initialExtents)

	// -----------------------------
	// 3. Snapshot → overwrite cycles
	// -----------------------------
	for cycle := 1; cycle <= CYCLES; cycle++ {
		ts := time.Now().Add(time.Duration(cycle) * time.Second).Format(time.RFC3339)
		err := CreateSnapshot(DEVICE_NAME, VOL_NAME, true, ts, nil)
		c.Assert(err, IsNil)

		vc, err = OpenVolume(DEVICE_NAME, VOL_NAME)
		c.Assert(err, IsNil)

		// Sliding overwrite window
		start := ((cycle - 1) * slideStep) % EXTENTS
		end := start + overwriteSize

		var targetExtentIdxs []int
		if end <= EXTENTS {
			for e := start; e < end; e++ {
				targetExtentIdxs = append(targetExtentIdxs, e)
			}
		} else {
			for e := start; e < EXTENTS; e++ {
				targetExtentIdxs = append(targetExtentIdxs, e)
			}
			for e := 0; e < end-EXTENTS; e++ {
				targetExtentIdxs = append(targetExtentIdxs, e)
			}
		}

		// Generate overwrite data
		makeExtentData := func(cycle int, extentIdx int) []byte {
			b := make([]byte, extentSize)
			fill := byte((cycle*31 + extentIdx*7) & 0xFF)
			for i := range b {
				b[i] = fill
			}
			return b
		}

		extentBuffers := make([][]byte, len(targetExtentIdxs))
		for i, extIdx := range targetExtentIdxs {
			buf := makeExtentData(cycle, extIdx)
			extentBuffers[i] = buf
			expectedFinalExtents[extIdx] = buf // FINAL expected state
		}

		blkIdxs, blkPayloads = expandExtentsToBlocks(targetExtentIdxs, extentBuffers)
		writeBlocks(c, vc, blkIdxs, blkPayloads)

		vc.CloseVolume()
	}

	vc, err = OpenVolume(DEVICE_NAME, VOL_NAME)
	c.Assert(err, IsNil)
	NofExts := vc.NumberOfExtents()

	err = DeleteSnapshot(DEVICE, 1)

	vc, err = OpenVolume(DEVICE, VOL_NAME)
	c.Assert(err, IsNil)

	c.Assert(vc.NumberOfExtents(), Equals, NofExts)
}

func (s *TestSuite) TestVolumeLimits(c *C) {
	err := InitDevice(DEVICE, SECONDARY_DEVICE_PATH)
	c.Assert(err, IsNil)

	// Max volumes is 256. Let's try to exceed it.
	for i := 0; i < 256; i++ {
		err := CreateVolume(DEVICE, fmt.Sprintf("vol%d", i), MEGABYTE)
		c.Assert(err, IsNil)
	}

	// 257th volume should fail
	err = CreateVolume(DEVICE, "vol256", MEGABYTE)
	c.Assert(err, NotNil)

	// Clean up some to ensure we can create again
	err = DeleteVolume(DEVICE, "vol0")
	c.Assert(err, IsNil)
	err = CreateVolume(DEVICE, "vol0_new", MEGABYTE)
	c.Assert(err, IsNil)

	// Cleanup for other tests
	for i := 1; i < 256; i++ {
		DeleteVolume(DEVICE, fmt.Sprintf("vol%d", i))
	}
	DeleteVolume(DEVICE, "vol0_new")
}

func (s *TestSuite) TestOutOfSpace(c *C) {
	err := InitDevice(DEVICE, SECONDARY_DEVICE_PATH)
	c.Assert(err, IsNil)

	di, err := GetDeviceInfo(DEVICE)
	c.Assert(err, IsNil)
	limit := di.TotalDeviceExtents

	// Create a volume large enough to hold all extents
	err = CreateVolume(DEVICE, "full_vol", uint64(limit+1)*EXTENT_SIZE)
	c.Assert(err, IsNil)

	vc, err := OpenVolume(DEVICE, "full_vol")
	c.Assert(err, IsNil)
	defer vc.CloseVolume()

	data := make([]byte, BLOCK_SIZE)
	copy(data, "some data")

	// Write up to the limit
	for i := 0; i < int(limit); i++ {
		// Each write to a new extent (1MB apart)
		blockIdx := uint64(i * (EXTENT_SIZE / BLOCK_SIZE))
		err := vc.WriteBlock(data, blockIdx, true)
		c.Assert(err, IsNil)
	}

	// Next extent should fail
	blockIdx := uint64(uint(limit) * (EXTENT_SIZE / BLOCK_SIZE))
	err = vc.WriteBlock(data, blockIdx, true)
	c.Assert(err, NotNil)
}

func (s *TestSuite) TestConcurrentIO(c *C) {
	err := InitDevice(DEVICE, SECONDARY_DEVICE_PATH)
	c.Assert(err, IsNil)

	err = CreateVolume(DEVICE, "concurrent_vol", GIGABYTE)
	c.Assert(err, IsNil)

	vc, err := OpenVolume(DEVICE, "concurrent_vol")
	c.Assert(err, IsNil)
	defer vc.CloseVolume()

	const (
		numGoroutines = 10
		numWrites     = 50
	)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			data := make([]byte, BLOCK_SIZE)
			for j := 0; j < numWrites; j++ {
				fill := byte(id*numWrites + j)
				for k := range data {
					data[k] = fill
				}
				// Use a unique block index for each write to avoid conflicts in this simple test
				blockIdx := uint64(id*numWrites + j)
				err := vc.WriteBlock(data, blockIdx, true)
				if err != nil {
					c.Errorf("Concurrent write failed: %v", err)
				}

				// Read back and verify
				readData := make([]byte, BLOCK_SIZE)
				err = vc.ReadBlock(readData, blockIdx)
				if err != nil {
					c.Errorf("Concurrent read failed: %v", err)
				}
				if !bytes.Equal(data, readData) {
					c.Errorf("Data corruption at block %d", blockIdx)
				}
			}
		}(i)
	}

	wg.Wait()
}

func (s *TestSuite) TestPartialUnmap(c *C) {
	err := InitDevice(DEVICE, SECONDARY_DEVICE_PATH)
	c.Assert(err, IsNil)

	err = CreateVolume(DEVICE, "unmap_vol", GIGABYTE)
	c.Assert(err, IsNil)

	vc, err := OpenVolume(DEVICE, "unmap_vol")
	c.Assert(err, IsNil)
	defer vc.CloseVolume()

	// Write 3 consecutive blocks
	data := make([]byte, BLOCK_SIZE)
	for i := 0; i < 3; i++ {
		fill := byte(i + 1)
		for j := range data {
			data[j] = fill
		}
		err = vc.WriteBlock(data, uint64(i), true)
		c.Assert(err, IsNil)
	}

	// Unmap range: 0.5 to 2.5 (BLOCK_SIZE * 2 length, starting at 0.5 * BLOCK_SIZE)
	// This covers half of block 0, all of block 1, and half of block 2.
	// Expected result: block 1 is unmapped, block 0 and 2 remain.
	unmapOffset := uint64(BLOCK_SIZE / 2)
	unmapLength := uint64(BLOCK_SIZE * 2)
	err = vc.UnmapAt(unmapLength, unmapOffset)
	c.Assert(err, IsNil)

	// Verify block 1 is empty (all zeros)
	emptyBlock := make([]byte, BLOCK_SIZE)
	readData := make([]byte, BLOCK_SIZE)
	err = vc.ReadBlock(readData, 1)
	c.Assert(err, IsNil)
	c.Assert(readData, DeepEquals, emptyBlock)

	// Verify block 0 and 2 are still there
	for i := 0; i < 3; i += 2 {
		fill := byte(i + 1)
		expected := make([]byte, BLOCK_SIZE)
		for j := range expected {
			expected[j] = fill
		}
		err = vc.ReadBlock(readData, uint64(i))
		c.Assert(err, IsNil)
		c.Assert(readData, DeepEquals, expected)
	}
}
