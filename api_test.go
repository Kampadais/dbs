package dbs

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"

	"golang.org/x/exp/slices"
	. "gopkg.in/check.v1"
)

const (
	MEGABYTE = 1024 * 1024
	GIGABYTE = MEGABYTE * 1024

	DEVICE      = "test.img"
	DEVICE_SIZE = MEGABYTE * 100
)

func Test(t *testing.T) {
	// InitDevice(DEVICE)
	TestingT(t)
}

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestDevice(c *C) {
	err := InitDevice(DEVICE)
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
	// Create a volume
	err := CreateVolume(DEVICE, "vol1", GIGABYTE)
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
	// Create a volume
	err := CreateVolume(DEVICE, "vol1", GIGABYTE)
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
	err = CreateSnapshot(DEVICE, "vol1")
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

	// Create multiple snapshots
	err = CreateSnapshot(DEVICE, "vol1")
	c.Assert(err, IsNil)
	err = CreateSnapshot(DEVICE, "vol1")
	c.Assert(err, IsNil)
	err = CreateSnapshot(DEVICE, "vol1")
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
	err = CreateSnapshot(DEVICE, "vol1")
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
	err = CreateSnapshot(DEVICE, "vol1")
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
	blockCount := len(data) / 512
	blockData := make([][]byte, blockCount)
	for i := 0; i < blockCount; i++ {
		blockData[i] = data[i*512 : (i+1)*512]
	}
	return blockData
}

func readBlocks(c *C, vc *VolumeContext, blockIndices []int, blockData [][]byte) {
	data := make([]byte, 512)
	blockCount := len(blockData)
	for i, _ := range blockIndices {
		err := vc.ReadBlock(uint64(blockIndices[i]), data)
		c.Assert(err, IsNil)
		c.Assert(data, DeepEquals, blockData[i%blockCount])
	}
}

func writeBlocks(c *C, vc *VolumeContext, blockIndices []int, blockData [][]byte) {
	blockCount := len(blockData)
	for i, _ := range blockIndices {
		err := vc.WriteBlock(uint64(blockIndices[i]), blockData[i%blockCount])
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
	err := CreateVolume(DEVICE, "vol1", GIGABYTE)
	c.Assert(err, IsNil)
	vc, err := OpenVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)

	// Read (should get empty data)
	emptyBlock := make([]byte, 512)
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
	err := CreateVolume(DEVICE, "vol1", GIGABYTE)
	c.Assert(err, IsNil)
	vc, err := OpenVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)

	// Write
	writeBlocks(c, vc, blockIndices, blockData)
	vc.CloseVolume()

	// Snapshot, open again and read back
	err = CreateSnapshot(DEVICE, "vol1")
	c.Assert(err, IsNil)
	vc, err = OpenVolume(DEVICE, "vol1")
	c.Assert(err, IsNil)
	readBlocks(c, vc, blockIndices, blockData)

	// Overwrite and read back
	dummyBlock := make([]byte, 512)
	for i := 0; i < 512; i++ {
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
