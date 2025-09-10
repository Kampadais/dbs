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

// A library for maintaining virtual volumes on top of a physical block device (or file).
// Snapshots supported. Command-line utility for query and management operations included.
//
// Device layout:
//   - Bytes [0, 4096) contain the the superblock
//   - Bytes [4096, ExtentOffset) hold the volume and snapshot metadata (ExtentOffset is block aligned)
//   - Bytes [ExtentOffset, DataOffset) hold the extent metadata (DataOffset is extent aligned)
//   - Bytes [DataOffset, DeviceSize) hold the data
package dbs

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/kelindar/bitmap"
)

const (
	MAGIC   = "DBS@393!"
	VERSION = 0x00010000

	MAX_VOLUMES          = 256
	MAX_SNAPSHOTS        = 65535
	MAX_VOLUME_NAME_SIZE = 255

	BLOCK_SIZE           = 4096
	EXTENT_SIZE          = 1048576 // 1 MB
	EXTENT_BITMAP_SIZE   = 32
	BLOCK_BITS_IN_EXTENT = 8
	BLOCK_MASK_IN_EXTENT = 0xFF
)

type Superblock struct {
	Magic                  [8]byte
	Version                uint32 // 16-bit major, 8-bit minor, 8-bit patch
	AllocatedDeviceExtents uint32
	DeviceSize             uint64
}

type VolumeMetadata struct {
	SnapshotId uint16 // Index in snapshots table + 1
	VolumeSize uint64
	VolumeName [MAX_VOLUME_NAME_SIZE + 1]byte
}

type SnapshotMetadata struct {
	ParentSnapshotId uint16
	CreatedAt        int64
}

type ExtentMetadata struct {
	SnapshotId  uint16
	ExtentPos   uint32
	BlockBitmap [EXTENT_BITMAP_SIZE]byte
}

func (v *VolumeMetadata) setName(volumeName string) {
	copy(v.VolumeName[:], volumeName)
	v.VolumeName[MAX_VOLUME_NAME_SIZE] = 0x00
}

// Query API

type DeviceInfo struct {
	Version                string
	DeviceSize             uint64
	TotalDeviceExtents     uint
	AllocatedDeviceExtents uint
	VolumeCount            uint
}

type VolumeInfo struct {
	VolumeName    string
	VolumeSize    uint64
	SnapshotId    uint
	CreatedAt     time.Time
	SnapshotCount uint
}

type SnapshotInfo struct {
	SnapshotId       uint
	ParentSnapshotId uint
	CreatedAt        time.Time
}

func humanVersion(version uint32) string {
	return fmt.Sprintf("%d.%d.%d", version>>16, (version&0xFF00)>>8, version&0xFF)
}

func GetDeviceInfo(device string) (*DeviceInfo, error) {
	dc, err := GetDeviceContext(device)
	if err != nil {
		return nil, err
	}
	di := &DeviceInfo{
		Version:                humanVersion(dc.superblock.Version),
		DeviceSize:             dc.superblock.DeviceSize,
		TotalDeviceExtents:     dc.totalDeviceExtents,
		AllocatedDeviceExtents: uint(dc.superblock.AllocatedDeviceExtents),
		VolumeCount:            dc.CountVolumes(),
	}
	dc.Close()
	return di, nil
}

func GetVolumeInfo(device string) ([]VolumeInfo, error) {
	dc, err := GetDeviceContext(device)
	if err != nil {
		return nil, err
	}
	vi := make([]VolumeInfo, dc.CountVolumes())
	viidx := 0
	for i := 0; i < MAX_VOLUMES; i++ {
		if dc.volumes[i].SnapshotId == 0 {
			continue
		}
		vn := dc.volumes[i].VolumeName
		vi[viidx].VolumeName = string(vn[:bytes.IndexByte(vn[:], 0)])
		vi[viidx].VolumeSize = dc.volumes[i].VolumeSize
		vi[viidx].SnapshotId = uint(dc.volumes[i].SnapshotId)
		vi[viidx].CreatedAt = time.Unix(dc.snapshots[dc.volumes[i].SnapshotId-1].CreatedAt, 0)
		vi[viidx].SnapshotCount = dc.CountSnapshots(&dc.volumes[i])
		viidx++
	}
	dc.Close()
	return vi, nil
}

func GetSnapshotInfo(device string, volumeName string) ([]SnapshotInfo, error) {
	dc, err := GetDeviceContext(device)
	if err != nil {
		return nil, err
	}
	v := dc.FindVolume(volumeName)
	if v == nil {
		return nil, fmt.Errorf("volume %v not found", volumeName)
	}
	si := make([]SnapshotInfo, dc.CountSnapshots(v))
	siidx := 0
	for sid := v.SnapshotId; sid > 0; sid = dc.snapshots[sid-1].ParentSnapshotId {
		si[siidx].SnapshotId = uint(sid)
		si[siidx].ParentSnapshotId = uint(dc.snapshots[sid-1].ParentSnapshotId)
		si[siidx].CreatedAt = time.Unix(dc.snapshots[sid-1].CreatedAt, 0)
		siidx++
	}
	dc.Close()
	return si, nil
}

// Management API

func InitDevice(device string) error {
	dc, err := NewDeviceContext(device)
	if err != nil {
		return err
	}
	eb := make([]ExtentMetadata, EXTENT_BATCH)
	for offset := uint(0); offset < dc.totalDeviceExtents; offset += EXTENT_BATCH {
		size := min(dc.totalDeviceExtents-offset, EXTENT_BATCH)
		if err := dc.WriteExtents(eb[:size], offset); err != nil {
			return err
		}
	}
	if err := dc.WriteMetadata(); err != nil {
		return err
	}
	if err := dc.WriteSuperblock(); err != nil {
		return err
	}
	return dc.Close()
}

func VacuumDevice(device string) error {
	return fmt.Errorf("not implemented")
}

func CreateVolume(device string, volumeName string, volumeSize uint64) error {
	if volumeSize/EXTENT_SIZE == 0 {
		return fmt.Errorf("volume with zero size")
	}
	dc, err := GetDeviceContext(device)
	if err != nil {
		return err
	}
	if v := dc.FindVolume(volumeName); v != nil {
		return fmt.Errorf("volume %v already exists", volumeName)
	}
	if _, err = dc.AddVolume(volumeName, volumeSize); err != nil {
		return err
	}
	if err := dc.WriteMetadata(); err != nil {
		return err
	}
	return dc.Close()
}

func RenameVolume(device string, volumeName string, newVolumeName string) error {
	dc, err := GetDeviceContext(device)
	if err != nil {
		return err
	}
	v := dc.FindVolume(volumeName)
	if v == nil {
		return fmt.Errorf("volume %v not found", volumeName)
	}
	v.setName(newVolumeName)
	if err := dc.WriteMetadata(); err != nil {
		return err
	}
	return dc.Close()
}

func CreateSnapshot(device string, volumeName string) error {
	dc, err := GetDeviceContext(device)
	if err != nil {
		return err
	}
	v := dc.FindVolume(volumeName)
	if v == nil {
		return fmt.Errorf("volume %v not found", volumeName)
	}
	sid, err := dc.AddSnapshot(v.SnapshotId)
	if err != nil {
		return err
	}
	v.SnapshotId = uint16(sid)
	if err := dc.WriteMetadata(); err != nil {
		return err
	}
	return dc.Close()
}

func CloneSnapshot(device string, newVolumeName string, snapshotId uint) error {
	dc, err := GetDeviceContext(device)
	if err != nil {
		return err
	}
	vsrc := dc.FindVolumeWithSnapshot(uint16(snapshotId))
	if vsrc == nil {
		return fmt.Errorf("snapshot %v not found", snapshotId)
	}
	vem, err := GetVolumeExtentMap(dc, vsrc.VolumeSize, uint16(snapshotId))
	if err != nil {
		return err
	}
	if uint(dc.superblock.AllocatedDeviceExtents)+uint(vem.extentBitmap.Count()) > dc.totalDeviceExtents {
		return fmt.Errorf("no space left on device")
	}
	vdst, err := dc.AddVolume(newVolumeName, vsrc.VolumeSize)
	if err != nil {
		return err
	}
	if err := dc.WriteMetadata(); err != nil {
		return err
	}
	if err := vem.CopyAllToSnapshot(vdst.SnapshotId); err != nil {
		return err
	}
	if err := dc.WriteSuperblock(); err != nil {
		return err
	}
	return dc.Close()
}

func DeleteVolume(device string, volumeName string) error {
	dc, err := GetDeviceContext(device)
	if err != nil {
		return err
	}
	v := dc.FindVolume(volumeName)
	if v == nil {
		return fmt.Errorf("volume %v not found", volumeName)
	}
	for sid := v.SnapshotId; sid > 0; sid = dc.snapshots[sid-1].ParentSnapshotId {
		sem, err := GetSnapshotExtentMap(dc, v.VolumeSize, sid)
		if err != nil {
			return err
		}
		if err := sem.ClearAll(); err != nil {
			return err
		}
		dc.snapshots[sid-1].CreatedAt = 0
	}
	*v = VolumeMetadata{}
	if err := dc.WriteMetadata(); err != nil {
		return err
	}
	return dc.Close()
}

func DeleteSnapshot(device string, snapshotId uint) error {
	dc, err := GetDeviceContext(device)
	if err != nil {
		return err
	}
	v := dc.FindVolumeWithSnapshot(uint16(snapshotId))
	if v == nil {
		return fmt.Errorf("snapshot %v not found", snapshotId)
	}
	if v.SnapshotId == uint16(snapshotId) {
		return fmt.Errorf("cannot delete current snapshot")
	}
	sem, err := GetSnapshotExtentMap(dc, v.VolumeSize, uint16(snapshotId))
	if err != nil {
		return err
	}
	childSnapshotId := dc.FindChildSnapshot(uint16(snapshotId))
	if childSnapshotId == 0 {
		return fmt.Errorf("cannot delete top-level snapshot")
	}
	cem, err := GetSnapshotExtentMap(dc, v.VolumeSize, childSnapshotId)
	if err != nil {
		return err
	}
	if err := sem.MergeAllInto(cem, childSnapshotId); err != nil {
		return err
	}
	if err := sem.ClearAll(); err != nil {
		return err
	}
	dc.snapshots[childSnapshotId-1].ParentSnapshotId = dc.snapshots[snapshotId-1].ParentSnapshotId
	dc.snapshots[snapshotId-1] = SnapshotMetadata{}
	if err := dc.WriteMetadata(); err != nil {
		return err
	}
	return dc.Close()
}

// Block API

type VolumeContext struct {
	dc     *DeviceContext
	volume *VolumeMetadata
	vem    *ExtentMap
}

var emptyBlock [BLOCK_SIZE]byte

func OpenVolume(device string, volumeName string) (*VolumeContext, error) {
	dc, err := GetDeviceContext(device)
	if err != nil {
		return nil, err
	}
	v := dc.FindVolume(volumeName)
	if v == nil {
		return nil, fmt.Errorf("volume %v not found", volumeName)
	}
	vem, err := GetVolumeExtentMap(dc, v.VolumeSize, v.SnapshotId)
	if err != nil {
		return nil, err
	}
	vc := &VolumeContext{
		dc:     dc,
		volume: v,
		vem:    vem,
	}
	return vc, nil
}

func (vc *VolumeContext) CloseVolume() error {
	return vc.dc.Close()
}

func (vc *VolumeContext) ReadBlock(data []byte, block uint64) error {
	eidx := uint(block >> BLOCK_BITS_IN_EXTENT)
	if eidx > vc.vem.totalVolumeExtents {
		return fmt.Errorf("block offset out of bounds")
	}
	e := &vc.vem.extents[eidx]
	bidx := uint(block & BLOCK_MASK_IN_EXTENT)
	bb := bitmap.FromBytes(e.BlockBitmap[:])
	// Unallocated extent or block
	if e.SnapshotId == 0 || !bb.Contains(uint32(bidx)) {
		copy(data, emptyBlock[:])
		return nil
	}
	// Read data from device
	if err := vc.dc.ReadBlockData(data, uint(e.ExtentPos), bidx); err != nil {
		return err
	}
	return nil
}

func (vc *VolumeContext) ReadAt(data []byte, offset uint64) error {
	doffset := uint64(0)
	for remaining := uint64(len(data)); remaining > 0; remaining = uint64(len(data)) - doffset {
		block := (offset + doffset) / BLOCK_SIZE
		boffset := (offset + doffset) % BLOCK_SIZE
		if boffset == 0 && remaining >= BLOCK_SIZE {
			if err := vc.ReadBlock(data[doffset:doffset+BLOCK_SIZE], block); err != nil {
				return err
			}
			doffset += BLOCK_SIZE
		} else {
			buf := make([]byte, BLOCK_SIZE)
			if err := vc.ReadBlock(buf, block); err != nil {
				return err
			}
			dlength := BLOCK_SIZE - boffset
			if remaining < dlength {
				copy(data[doffset:doffset+remaining], buf[boffset:boffset+remaining])
				doffset += remaining
			} else {
				copy(data[doffset:doffset+dlength], buf[boffset:boffset+dlength])
				doffset += dlength
			}
		}
	}
	return nil
}

var ErrMetadataNeedsUpdate = errors.New("metadata needs update")

func (vc *VolumeContext) WriteBlock(data []byte, block uint64, updateMetadata bool) error {
	eidx := uint(block >> BLOCK_BITS_IN_EXTENT)
	if eidx > vc.vem.totalVolumeExtents {
		return fmt.Errorf("block offset out of bounds")
	}
	e := &vc.vem.extents[eidx]
	bidx := uint(block & BLOCK_MASK_IN_EXTENT)
	bb := bitmap.FromBytes(e.BlockBitmap[:])
	// Unallocated or previous snapshot extent
	if e.SnapshotId != vc.volume.SnapshotId {
		if !updateMetadata {
			return ErrMetadataNeedsUpdate
		}
		// Allocate new extent
		if e.SnapshotId == 0 {
			if err := vc.vem.NewExtentToSnapshot(uint32(eidx), vc.volume.SnapshotId); err != nil {
				return err
			}
		} else {
			if err := vc.vem.CopyExtentToSnapshot(uint32(eidx), vc.volume.SnapshotId); err != nil {
				return err
			}
		}
		// Update allocation count
		if err := vc.dc.WriteSuperblock(); err != nil {
			return err
		}
	} else {
		if !bb.Contains(uint32(bidx)) && !updateMetadata {
			return ErrMetadataNeedsUpdate
		}
	}
	// Write data to device
	if err := vc.dc.WriteBlockData(data, uint(e.ExtentPos), bidx); err != nil {
		return err
	}
	// Update metadata
	if bb.Contains(uint32(bidx)) {
		return nil
	}
	bb.Set(uint32(bidx))
	if err := vc.vem.WriteExtent(uint32(eidx)); err != nil {
		return err
	}
	return nil
}

func (vc *VolumeContext) WriteAt(data []byte, offset uint64, updateMetadata bool) error {
	doffset := uint64(0)
	for remaining := uint64(len(data)); remaining > 0; remaining = uint64(len(data)) - doffset {
		block := (offset + doffset) / BLOCK_SIZE
		boffset := (offset + doffset) % BLOCK_SIZE
		if boffset == 0 && remaining >= BLOCK_SIZE {
			if err := vc.WriteBlock(data[doffset:doffset+BLOCK_SIZE], block, updateMetadata); err != nil {
				return err
			}
			doffset += BLOCK_SIZE
		} else {
			buf := make([]byte, BLOCK_SIZE)
			if err := vc.ReadBlock(buf, block); err != nil {
				return err
			}
			dlength := BLOCK_SIZE - boffset
			if remaining < dlength {
				copy(buf[boffset:boffset+remaining], data[doffset:doffset+remaining])
				doffset += remaining
			} else {
				copy(buf[boffset:boffset+dlength], data[doffset:doffset+dlength])
				doffset += dlength
			}
			if err := vc.WriteBlock(buf, block, updateMetadata); err != nil {
				return err
			}
		}
	}
	return nil
}

func (vc *VolumeContext) UnmapBlock(block uint64) error {
	eidx := uint(block >> BLOCK_BITS_IN_EXTENT)
	if eidx > vc.vem.totalVolumeExtents {
		return fmt.Errorf("block offset out of bounds")
	}
	e := &vc.vem.extents[eidx]
	bidx := uint(block & BLOCK_MASK_IN_EXTENT)
	bb := bitmap.FromBytes(e.BlockBitmap[:])
	// Unallocated extent or block
	if e.SnapshotId == 0 || !bb.Contains(uint32(bidx)) {
		return nil
	}
	// Update metadata
	bb.Remove(uint32(bidx))
	if bb.Count() == 0 {
		// Release if not used
		e.SnapshotId = 0
	}
	if err := vc.vem.WriteExtent(uint32(eidx)); err != nil {
		return err
	}
	return nil
}

func (vc *VolumeContext) UnmapAt(length uint64, offset uint64) error {
	doffset := uint64(0)
	for remaining := length; remaining > 0; remaining = length - doffset {
		block := (offset + doffset) / BLOCK_SIZE
		boffset := (offset + doffset) % BLOCK_SIZE
		if boffset == 0 && remaining >= BLOCK_SIZE {
			if err := vc.UnmapBlock(block); err != nil {
				return err
			}
			doffset += BLOCK_SIZE
		} else {
			dlength := BLOCK_SIZE - boffset
			if remaining < dlength {
				doffset += remaining
			} else {
				doffset += dlength
			}
		}
	}
	return nil
}
