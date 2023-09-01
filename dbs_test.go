package dbs

import (
    "fmt"
    "testing"
    "time"

    "golang.org/x/exp/slices"

	. "gopkg.in/check.v1"
)

const (
    MEGABYTE = 1024 * 1024
    GIGABYTE = MEGABYTE * 1024

	DEVICE = "test.img"
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
    c.Assert(deviceInfo.allocatedDeviceExtents, Equals, uint32(0))
    c.Assert(deviceInfo.volumeCount, Equals, uint8(0))
    volumeInfo, err := GetVolumeInfo(DEVICE)
    c.Assert(err, IsNil)
    c.Assert(volumeInfo, HasLen, 0)
}

func assertVolume(c *C, vi *VolumeInfo, volumeName string, volumeSize uint64, snapshotCount uint16) {
    c.Assert(vi.volumeName, Equals, volumeName)
    c.Assert(vi.volumeSize, Equals, volumeSize)
    if time.Now().Add(-5 * time.Minute).After(vi.createdAt) {
        c.FailNow()
    }
    c.Assert(vi.volumeName, Equals, volumeName)
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
    err = CreateVolume(DEVICE, "vol2", 2 * GIGABYTE)
    c.Assert(err, IsNil)
    err = CreateVolume(DEVICE, "vol3", 3 * GIGABYTE)
    c.Assert(err, IsNil)
    volumeInfo, err = GetVolumeInfo(DEVICE)
    c.Assert(err, IsNil)
    c.Assert(volumeInfo, HasLen, 3)
    assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
    assertVolume(c, &volumeInfo[1], "vol2", 2 * GIGABYTE, 1)
    assertVolume(c, &volumeInfo[2], "vol3", 3 * GIGABYTE, 1)

    // Delete a volume
    err = DeleteVolume(DEVICE, "vol2")
    c.Assert(err, IsNil)
    volumeInfo, err = GetVolumeInfo(DEVICE)
    c.Assert(err, IsNil)
    c.Assert(volumeInfo, HasLen, 2)
    assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
    assertVolume(c, &volumeInfo[1], "vol3", 3 * GIGABYTE, 1)
    err = DeleteVolume(DEVICE, "vol2")
    c.Assert(err, NotNil)

    // Create volume again (goes in empty spot)
    err = CreateVolume(DEVICE, "vol2new", 2 * GIGABYTE)
    c.Assert(err, IsNil)
    volumeInfo, err = GetVolumeInfo(DEVICE)
    c.Assert(err, IsNil)
    c.Assert(volumeInfo, HasLen, 3)
    assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
    assertVolume(c, &volumeInfo[1], "vol2new", 2 * GIGABYTE, 1)
    assertVolume(c, &volumeInfo[2], "vol3", 3 * GIGABYTE, 1)

    // Rename volume
    err = RenameVolume(DEVICE, "vol2new", "vol2renamed")
    c.Assert(err, IsNil)
    volumeInfo, err = GetVolumeInfo(DEVICE)
    c.Assert(err, IsNil)
    c.Assert(volumeInfo, HasLen, 3)
    assertVolume(c, &volumeInfo[0], "vol1", GIGABYTE, 1)
    assertVolume(c, &volumeInfo[1], "vol2renamed", 2 * GIGABYTE, 1)
    assertVolume(c, &volumeInfo[2], "vol3", 3 * GIGABYTE, 1)

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
    volumeSnapshotIdx := slices.IndexFunc(volumeInfo, func(vi VolumeInfo) bool { return vi.volumeName == "vol1" })
    volumeSnapshotId := volumeInfo[volumeSnapshotIdx].snapshotId
    snapshotInfo, err := GetSnapshotInfo(DEVICE, "vol1")
    c.Assert(err, IsNil)
    c.Assert(snapshotInfo, HasLen, 1)
    if snapshotInfo[0].snapshotId == 0 {
        c.FailNow()
    }
    c.Assert(snapshotInfo[0].parentSnapshotId, Equals, uint16(0))
    initialSnapshotId := snapshotInfo[0].snapshotId
    c.Assert(volumeSnapshotId, Equals, initialSnapshotId)

    // Create a snapshot
    err = CreateSnapshot(DEVICE, "vol1")
    c.Assert(err, IsNil)
    volumeInfo, err = GetVolumeInfo(DEVICE)
    c.Assert(err, IsNil)
    volumeSnapshotIdx = slices.IndexFunc(volumeInfo, func(vi VolumeInfo) bool { return vi.volumeName == "vol1" })
    volumeSnapshotId = volumeInfo[volumeSnapshotIdx].snapshotId
    if volumeSnapshotId == initialSnapshotId {
        c.FailNow()
    }
    snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
    c.Assert(err, IsNil)
    c.Assert(snapshotInfo, HasLen, 2)
    initialSnapshotIdx := slices.IndexFunc(snapshotInfo, func(si SnapshotInfo) bool { return si.parentSnapshotId == 0 })
    if initialSnapshotIdx == -1 {
        c.FailNow()
    }
    initialSnapshot := snapshotInfo[initialSnapshotIdx]
    c.Assert(initialSnapshot.snapshotId, Equals, initialSnapshotId)
    currentSnapshotIdx := slices.IndexFunc(snapshotInfo, func(si SnapshotInfo) bool { return si.parentSnapshotId != 0 })
    if currentSnapshotIdx == -1 {
        c.FailNow()
    }
    currentSnapshot := snapshotInfo[currentSnapshotIdx]
    c.Assert(currentSnapshot.snapshotId, Equals, volumeSnapshotId)
    c.Assert(currentSnapshot.parentSnapshotId, Equals, initialSnapshotId)

    // Create multiple snapshots
    err = CreateSnapshot(DEVICE, "vol1")
    c.Assert(err, IsNil)
    err = CreateSnapshot(DEVICE, "vol1")
    c.Assert(err, IsNil)
    err = CreateSnapshot(DEVICE, "vol1")
    c.Assert(err, IsNil)
    volumeInfo, err = GetVolumeInfo(DEVICE)
    c.Assert(err, IsNil)
    volumeSnapshotIdx = slices.IndexFunc(volumeInfo, func(vi VolumeInfo) bool { return vi.volumeName == "vol1" })
    volumeSnapshotId = volumeInfo[volumeSnapshotIdx].snapshotId
    if volumeSnapshotId == initialSnapshotId {
        c.FailNow()
    }
    snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
    c.Assert(err, IsNil)
    c.Assert(snapshotInfo, HasLen, 5)
    initialSnapshotIdx = slices.IndexFunc(snapshotInfo, func(si SnapshotInfo) bool { return si.parentSnapshotId == 0 })
    if initialSnapshotIdx == -1 {
        c.FailNow()
    }
    initialSnapshot = snapshotInfo[initialSnapshotIdx]
    c.Assert(initialSnapshot.snapshotId, Equals, initialSnapshotId)
    currentSnapshotIdx = slices.IndexFunc(snapshotInfo, func(si SnapshotInfo) bool { return si.parentSnapshotId != volumeSnapshotId })
    if currentSnapshotIdx == -1 {
        c.FailNow()
    }
    currentSnapshot = snapshotInfo[currentSnapshotIdx]
    c.Assert(currentSnapshot.snapshotId, Equals, volumeSnapshotId)
    if currentSnapshot.parentSnapshotId == initialSnapshotId {
        c.FailNow()
    }

    // Delete a snapshot
    err = DeleteSnapshot(DEVICE, currentSnapshot.snapshotId)
    c.Assert(err, NotNil)
    snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
    c.Assert(err, IsNil)
    c.Assert(snapshotInfo, HasLen, 5)
    err = DeleteSnapshot(DEVICE, initialSnapshot.snapshotId)
    c.Assert(err, IsNil)
    snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
    c.Assert(err, IsNil)
    c.Assert(snapshotInfo, HasLen, 4)

    // Create snapshot again
    err = CreateSnapshot(DEVICE, "vol1")
    c.Assert(err, IsNil)
    volumeInfo, err = GetVolumeInfo(DEVICE)
    c.Assert(err, IsNil)
    volumeSnapshotIdx = slices.IndexFunc(volumeInfo, func(vi VolumeInfo) bool { return vi.volumeName == "vol1" })
    volumeSnapshotId = volumeInfo[volumeSnapshotIdx].snapshotId
    if volumeSnapshotId == currentSnapshot.snapshotId {
        c.FailNow()
    }
    snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
    c.Assert(err, IsNil)
    c.Assert(snapshotInfo, HasLen, 5)

    // Delete multiple snapshots
    for i, _ := range snapshotInfo {
        if snapshotInfo[i].snapshotId == volumeSnapshotId {
            continue
        }
        err = DeleteSnapshot(DEVICE, snapshotInfo[i].snapshotId)
        c.Assert(err, IsNil)
    }
    snapshotInfo, err = GetSnapshotInfo(DEVICE, "vol1")
    c.Assert(err, IsNil)
    c.Assert(snapshotInfo, HasLen, 1)
    c.Assert(snapshotInfo[0].snapshotId, Equals, volumeSnapshotId)
    c.Assert(snapshotInfo[0].parentSnapshotId, Equals, uint16(0))

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
        err = CloneSnapshot(DEVICE, fmt.Sprintf("vol2clone%d", i + 1), snapshotInfo[i].snapshotId)
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
