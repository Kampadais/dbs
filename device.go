package dbs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/chazapis/directio"
)

const (
	SIZEOF_EXTENT_METADATA = 6 + EXTENT_BITMAP_SIZE
)

func divRoundUp(x uint, y uint) uint {
	return 1 + ((x - 1) / y)
}

// The device context holds the device file descriptor and all metadata except extents.
type DeviceContext struct {
	f                  *DirectFile
	superblock         *Superblock
	volumes            [MAX_VOLUMES]VolumeMetadata
	snapshots          [MAX_SNAPSHOTS]SnapshotMetadata
	extentOffset       uint
	totalDeviceExtents uint
	dataOffset         uint
}

// Initialize a new, empty device context.
func NewDeviceContext(device string) (*DeviceContext, error) {
	f, err := NewDirectFile(device, os.O_RDWR, 0660)
	if err != nil {
		return nil, fmt.Errorf("cannot open %v: %w", device, err)
	}
	deviceSize, err := f.Size()
	if err != nil {
		return nil, err
	}
	if deviceSize == 0 {
		return nil, fmt.Errorf("device with zero size")
	}
	if deviceSize < (100 * (1 << 20)) {
		return nil, fmt.Errorf("device size less than 100 MB")
	}

	dc := &DeviceContext{
		f: f,
		superblock: &Superblock{
			Version:    VERSION,
			DeviceSize: uint64(deviceSize),
		},
	}
	copy(dc.superblock.Magic[:], []byte(MAGIC))
	dc.extentOffset = (1 + divRoundUp(uint(binary.Size(dc.volumes)+binary.Size(dc.snapshots)), 512)) * 512
	dc.totalDeviceExtents = uint((dc.superblock.DeviceSize - uint64(dc.extentOffset)) / EXTENT_SIZE)
	metadataSize := dc.extentOffset + uint(dc.totalDeviceExtents*SIZEOF_EXTENT_METADATA)
	dc.dataOffset = divRoundUp(metadataSize, EXTENT_SIZE) * EXTENT_SIZE
	// Account for storage of extent metadata
	dc.totalDeviceExtents -= (dc.totalDeviceExtents * SIZEOF_EXTENT_METADATA) / EXTENT_SIZE
	return dc, nil
}

func GetDeviceContext(device string) (*DeviceContext, error) {
	dc, err := NewDeviceContext(device)
	if err != nil {
		return nil, err
	}
	if err := dc.ReadSuperblock(); err != nil {
		return nil, err
	}
	if err := dc.ReadMetadata(); err != nil {
		return nil, err
	}
	return dc, nil
}

func (dc *DeviceContext) ReadSuperblock() error {
	var sb Superblock
	abuf := directio.AlignedBlock(512)
	if _, err := dc.f.ReadAt(abuf, 0); err != nil {
		return fmt.Errorf("failed to read superblock: %w", err)
	}
	buf := bytes.NewBuffer(abuf)
	if err := binary.Read(buf, binary.LittleEndian, &sb); err != nil {
		return fmt.Errorf("failed to deserialize superblock: %w", err)
	}
	if dc.superblock.Magic != sb.Magic {
		return fmt.Errorf("device not initialized")
	}
	if dc.superblock.Version != sb.Version {
		return fmt.Errorf("version mismatch in superblock")
	}
	if dc.superblock.DeviceSize != sb.DeviceSize {
		return fmt.Errorf("device size mismatch in superblock")
	}
	dc.superblock = &sb
	return nil
}

func (dc *DeviceContext) ReadMetadata() error {
	abuf := directio.AlignedBlock(int(dc.extentOffset - 512))
	if _, err := dc.f.ReadAt(abuf, 512); err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}
	buf := bytes.NewBuffer(abuf)
	if err := binary.Read(buf, binary.LittleEndian, dc.volumes[:]); err != nil {
		return fmt.Errorf("failed to deserialize volume metadata: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, dc.snapshots[:]); err != nil {
		return fmt.Errorf("failed to deserialize snapshot metadata: %w", err)
	}
	return nil
}

func (dc *DeviceContext) ReadExtents(eb []ExtentMetadata, eidx uint) error {
	offset := uint64(dc.extentOffset + (eidx * SIZEOF_EXTENT_METADATA))
	size := uint64(binary.Size(eb))
	blocks := ((offset + size) / 512) - (offset / 512) + 1
	abuf := directio.AlignedBlock(int(512 * blocks))
	if _, err := dc.f.ReadAt(abuf, (offset/512)*512); err != nil {
		return fmt.Errorf("failed to read extent metadata: %w", err)
	}
	buf := bytes.NewBuffer(abuf[offset%512 : (offset%512)+size])
	if err := binary.Read(buf, binary.LittleEndian, eb); err != nil {
		return fmt.Errorf("failed to deserialize extent metadata: %w", err)
	}
	return nil
}

func (dc *DeviceContext) ReadBlockData(data []byte, epos uint, bidx uint) error {
	offset := uint64(dc.dataOffset + (epos * EXTENT_SIZE) + (bidx * 512))
	if _, err := dc.f.ReadAt(data[0:512], offset); err != nil {
		return fmt.Errorf("failed to read block: %w", err)
	}
	return nil
}

func (dc *DeviceContext) WriteSuperblock() error {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, dc.superblock); err != nil {
		return fmt.Errorf("failed to serialize superblock: %w", err)
	}
	abuf := directio.AlignedBlock(512)
	copy(abuf[0:], buf.Bytes())
	if _, err := dc.f.WriteAt(abuf, 0); err != nil {
		return fmt.Errorf("failed to write superblock: %w", err)
	}
	return nil
}

func (dc *DeviceContext) WriteMetadata() error {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, dc.volumes); err != nil {
		return fmt.Errorf("failed to serialize volume metadata: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, dc.snapshots); err != nil {
		return fmt.Errorf("failed to serialize snapshot metadata: %w", err)
	}
	abuf := directio.AlignedBlock(int(dc.extentOffset - 512))
	copy(abuf[0:], buf.Bytes())
	if _, err := dc.f.WriteAt(abuf, 512); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}
	return nil
}

func (dc *DeviceContext) WriteExtents(eb []ExtentMetadata, eidx uint) error {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, eb); err != nil {
		return fmt.Errorf("failed to serialize extent metadata: %w", err)
	}
	offset := uint64(dc.extentOffset + (eidx * SIZEOF_EXTENT_METADATA))
	size := uint64(binary.Size(eb))
	blocks := ((offset + size) / 512) - (offset / 512) + 1
	abuf := directio.AlignedBlock(int(512 * blocks))
	if _, err := dc.f.ReadAt(abuf, (offset/512)*512); err != nil {
		return fmt.Errorf("failed to read extent metadata: %w", err)
	}
	copy(abuf[offset%512:(offset%512)+size], buf.Bytes())
	if _, err := dc.f.WriteAt(abuf, (offset/512)*512); err != nil {
		return fmt.Errorf("failed to write extent metadata: %w", err)
	}
	return nil
}

func (dc *DeviceContext) WriteExtent(e *ExtentMetadata, eidx uint) error {
	return dc.WriteExtents([]ExtentMetadata{*e}, eidx)
}

func (dc *DeviceContext) WriteBlockData(data []byte, epos uint, bidx uint) error {
	offset := uint64(dc.dataOffset + (epos * EXTENT_SIZE) + (bidx * 512))
	if _, err := dc.f.WriteAt(data[0:512], offset); err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}
	return nil
}

func (dc *DeviceContext) CopyExtentData(esrc uint, edst uint) error {
	abuf := directio.AlignedBlock(EXTENT_SIZE)
	if _, err := dc.f.ReadAt(abuf, uint64(dc.dataOffset+(esrc*EXTENT_SIZE))); err != nil {
		return fmt.Errorf("failed to read extent data: %w", err)
	}
	if _, err := dc.f.WriteAt(abuf, uint64(dc.dataOffset+(edst*EXTENT_SIZE))); err != nil {
		return fmt.Errorf("failed to read extent data: %w", err)
	}
	return nil
}

// Find the volume metadata for the given volume name. Returns nil if not found.
func (dc *DeviceContext) FindVolume(volumeName string) *VolumeMetadata {
	var vname [MAX_VOLUME_NAME_SIZE + 1]byte
	copy(vname[:], volumeName)
	for i := 0; i < MAX_VOLUMES; i++ {
		if dc.volumes[i].SnapshotId == 0 {
			continue
		}
		if dc.volumes[i].VolumeName == vname {
			return &dc.volumes[i]
		}
	}
	return nil
}

// Find the descendant of the snapshot with the given identifier. Returns 0 if not found.
func (dc *DeviceContext) FindChildSnapshot(snapshotId uint16) uint16 {
	for i := 0; i < MAX_SNAPSHOTS; i++ {
		if dc.snapshots[i].CreatedAt == 0 {
			continue
		}
		if dc.snapshots[i].ParentSnapshotId == snapshotId {
			return uint16(i + 1)
		}
	}
	return 0
}

// Find the volume metadata for the given snapshot identifier. Returns 0 if not found.
func (dc *DeviceContext) FindVolumeWithSnapshot(snapshotId uint16) *VolumeMetadata {
	for sid := snapshotId; sid > 0; sid = dc.FindChildSnapshot(sid) {
		for i := 0; i < MAX_VOLUMES; i++ {
			if dc.volumes[i].SnapshotId == sid {
				return &dc.volumes[i]
			}
		}
	}
	return nil
}

func (dc *DeviceContext) CountVolumes() uint {
	count := uint(0)
	for i := 0; i < MAX_VOLUMES; i++ {
		if dc.volumes[i].SnapshotId == 0 {
			continue
		}
		count++
	}
	return count
}

func (dc *DeviceContext) CountSnapshots(v *VolumeMetadata) uint {
	count := uint(0)
	for sid := v.SnapshotId; sid > 0; sid = dc.snapshots[sid-1].ParentSnapshotId {
		count++
	}
	return count
}

// Add a new volume (and corresponding snapshot). Return a pointer to the volume metadata.
func (dc *DeviceContext) AddVolume(volumeName string, volumeSize uint64) (*VolumeMetadata, error) {
	var vidx uint
	for vidx = 0; vidx < MAX_VOLUMES && dc.volumes[vidx].SnapshotId != 0; vidx++ {
	}
	if vidx == MAX_VOLUMES {
		return nil, fmt.Errorf("max volume count reached")
	}

	sid, err := dc.AddSnapshot(0)
	if err != nil {
		return nil, err
	}
	dc.volumes[vidx].SnapshotId = uint16(sid)
	dc.volumes[vidx].VolumeSize = (volumeSize / EXTENT_SIZE) * EXTENT_SIZE
	dc.volumes[vidx].setName(volumeName)
	return &dc.volumes[vidx], nil
}

// Add a new snapshot. Return the snapshot identifier.
func (dc *DeviceContext) AddSnapshot(parentSnapshotId uint16) (uint16, error) {
	var sidx uint
	for sidx = 0; sidx < MAX_SNAPSHOTS && dc.snapshots[sidx].CreatedAt != 0; sidx++ {
	}
	if sidx == MAX_SNAPSHOTS {
		return 0, fmt.Errorf("max snapshot count reached")
	}

	dc.snapshots[sidx].ParentSnapshotId = parentSnapshotId
	dc.snapshots[sidx].CreatedAt = time.Now().Unix()
	return uint16(sidx) + 1, nil
}

// Close the device file descriptor.
func (dc *DeviceContext) Close() error {
	if err := dc.f.Sync(); err != nil {
		return fmt.Errorf("cannot sync device: %w", err)
	}
	dc.f.Close()
	return nil
}
