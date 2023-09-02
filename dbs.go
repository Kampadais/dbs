package dbs

// #cgo CFLAGS: -g -Wall
// #include <stdlib.h>
// #include <stdint.h>
// #include "dbs.h"
import "C"
import (
    "errors"
    "unsafe"
    "time"
)

const (
	MAX_VOLUMES = 256
	MAX_SNAPSHOTS = 65536
)

// Query API

type DeviceInfo struct {
	version					uint32
	deviceSize				uint64
	totalDeviceExtents		uint32
	allocatedDeviceExtents	uint32
	volumeCount				uint8
}

type VolumeInfo struct {
	volumeName 				string
	volumeSize 				uint64
	snapshotId 				uint16
	createdAt 				time.Time
	snapshotCount 			uint16
}

type SnapshotInfo struct {
	snapshotId 				uint16
	parentSnapshotId 		uint16
	createdAt 				time.Time
}

func GetDeviceInfo(device string) (*DeviceInfo, error) {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))
	cDeviceInfo := &C.dbs_device_info{}

	if C.dbs_fill_device_info(cDevice, cDeviceInfo) == 0 {
		return nil, errors.New("dbs_fill_device_info failed")
	}
	deviceInfo := &DeviceInfo{
		version:					uint32(cDeviceInfo.version),
		deviceSize:					uint64(cDeviceInfo.device_size),
		totalDeviceExtents:			uint32(cDeviceInfo.total_device_extents),
		allocatedDeviceExtents:		uint32(cDeviceInfo.allocated_device_extents),
		volumeCount:				uint8(cDeviceInfo.volume_count),
	}
	return deviceInfo, nil
}

func GetVolumeInfo(device string) ([]VolumeInfo, error) {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))
	cVolumeInfo := [MAX_VOLUMES]C.dbs_volume_info{}
	var cVolumeCount C.uint8_t

	if C.dbs_fill_volume_info(cDevice, &(cVolumeInfo[0]), &cVolumeCount) == 0 {
		return nil, errors.New("dbs_fill_volume_info failed")
	}
	volumeInfo := make([]VolumeInfo, uint8(cVolumeCount))
	for i, _ := range volumeInfo {
		volumeInfo[i].volumeName = C.GoString(&(cVolumeInfo[i].volume_name[0]))
		volumeInfo[i].volumeSize = uint64(cVolumeInfo[i].volume_size)
		volumeInfo[i].snapshotId = uint16(cVolumeInfo[i].snapshot_id)
		volumeInfo[i].createdAt = time.Unix(int64(cVolumeInfo[i].created_at), 0)
		volumeInfo[i].snapshotCount = uint16(cVolumeInfo[i].snapshot_count)
	}
	return volumeInfo, nil
}

func GetSnapshotInfo(device string, volumeName string) ([]SnapshotInfo, error) {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))
	cVolumeName := C.CString(volumeName)
	defer C.free(unsafe.Pointer(cVolumeName))
	cSnapshotInfo := [MAX_SNAPSHOTS]C.dbs_snapshot_info{}
	var cSnapshotCount C.uint16_t

	if C.dbs_fill_snapshot_info(cDevice, cVolumeName, &(cSnapshotInfo[0]), &cSnapshotCount) == 0 {
		return nil, errors.New("dbs_fill_snapshot_info failed")
	}
	snapshotInfo := make([]SnapshotInfo, uint8(cSnapshotCount))
	for i, _ := range snapshotInfo {
		snapshotInfo[i].snapshotId = uint16(cSnapshotInfo[i].snapshot_id)
		snapshotInfo[i].parentSnapshotId = uint16(cSnapshotInfo[i].parent_snapshot_id)
		snapshotInfo[i].createdAt = time.Unix(int64(cSnapshotInfo[i].created_at), 0)
	}
	return snapshotInfo, nil
}

// Management API

func InitDevice(device string) error {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))

	if C.dbs_init_device(cDevice) == 0 {
		return errors.New("dbs_init_device failed")
	}
	return nil
}

func VacuumDevice(device string) error {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))

	if C.dbs_vacuum_device(cDevice) == 0 {
		return errors.New("dbs_vacuum_device failed")
	}
	return nil
}

func CreateVolume(device string, volumeName string, volumeSize uint64) error {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))
	cVolumeName := C.CString(volumeName)
	defer C.free(unsafe.Pointer(cVolumeName))

	if C.dbs_create_volume(cDevice, cVolumeName, C.uint64_t(volumeSize)) == 0 {
		return errors.New("dbs_create_volume failed")
	}
	return nil
}

func RenameVolume(device string, volumeName string, newVolumeName string) error {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))
	cVolumeName := C.CString(volumeName)
	defer C.free(unsafe.Pointer(cVolumeName))
	cNewVolumeName := C.CString(newVolumeName)
	defer C.free(unsafe.Pointer(cNewVolumeName))

	if C.dbs_rename_volume(cDevice, cVolumeName, cNewVolumeName) == 0 {
		return errors.New("dbs_rename_volume failed")
	}
	return nil
}

func CreateSnapshot(device string, volumeName string) error {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))
	cVolumeName := C.CString(volumeName)
	defer C.free(unsafe.Pointer(cVolumeName))

	if C.dbs_create_snapshot(cDevice, cVolumeName) == 0 {
		return errors.New("dbs_create_snapshot failed")
	}
	return nil
}

func CloneSnapshot(device string, newVolumeName string, snapshotId uint16) error {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))
	cNewVolumeName := C.CString(newVolumeName)
	defer C.free(unsafe.Pointer(cNewVolumeName))

	if C.dbs_clone_snapshot(cDevice, cNewVolumeName, C.uint16_t(snapshotId)) == 0 {
		return errors.New("dbs_clone_snapshot failed")
	}
	return nil
}

func DeleteVolume(device string, volumeName string) error {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))
	cVolumeName := C.CString(volumeName)
	defer C.free(unsafe.Pointer(cVolumeName))

	if C.dbs_delete_volume(cDevice, cVolumeName) == 0 {
		return errors.New("dbs_delete_volume failed")
	}
	return nil
}

func DeleteSnapshot(device string, snapshotId uint16) error {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))

	if C.dbs_delete_snapshot(cDevice, C.uint16_t(snapshotId)) == 0 {
		return errors.New("dbs_delete_snapshot failed")
	}
	return nil
}

// Block API

type Context struct {
	pointer C.dbs_context
}

func OpenVolume(device string, volumeName string) (*Context, error) {
	cDevice := C.CString(device)
	defer C.free(unsafe.Pointer(cDevice))
	cVolumeName := C.CString(volumeName)
	defer C.free(unsafe.Pointer(cVolumeName))

	ctx := &Context{}
	ctx.pointer = C.dbs_open_volume(cDevice, cVolumeName)
	if ctx.pointer == nil {
		return nil, errors.New("dbs_open_volume failed")
	}
	return ctx, nil
}

func (ctx *Context) CloseVolume() {
	C.dbs_close_volume(ctx.pointer)
}

func (ctx *Context) ReadBlock(block uint64, data []byte) error {
	cData := C.malloc(512)
	defer C.free(unsafe.Pointer(cData))

	if C.dbs_read_block(ctx.pointer, C.uint64_t(block), cData) == 0 {
		return errors.New("dbs_read_block failed")
	}
	copy(data, C.GoBytes(cData, 512))
	return nil
}

func (ctx *Context) WriteBlock(block uint64, data []byte) error {
	cData := C.CBytes(data)
	defer C.free(unsafe.Pointer(cData))

	if C.dbs_write_block(ctx.pointer, C.uint64_t(block), cData) == 0 {
		return errors.New("dbs_write_block failed")
	}
	return nil
}

func (ctx *Context) UnmapBlock(block uint64) error {
	if C.dbs_unmap_block(ctx.pointer, C.uint64_t(block)) == 0 {
		return errors.New("dbs_unmap_block failed")
	}
	return nil
}
