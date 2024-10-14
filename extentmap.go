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
	"github.com/kelindar/bitmap"
)

const (
	EXTENT_BATCH = 65536
)

// Map of the whole volume. Empty extents have an empty snapshot identifier. The extent bitmap is used to speed up operations.
type ExtentMap struct {
	dc                 *DeviceContext
	totalVolumeExtents uint
	extentBitmap       bitmap.Bitmap
	extents            []ExtentMetadata
}

// Get the map of a specific snapshot.
func GetSnapshotExtentMap(dc *DeviceContext, deviceSize uint64, snapshotId uint16) (*ExtentMap, error) {
	sem := &ExtentMap{
		dc:                 dc,
		totalVolumeExtents: uint(deviceSize / EXTENT_SIZE),
	}
	sem.extentBitmap.Grow(uint32(sem.totalVolumeExtents - 1))
	sem.extents = make([]ExtentMetadata, sem.totalVolumeExtents)

	eb := make([]ExtentMetadata, EXTENT_BATCH)
	remaining := min(dc.totalDeviceExtents, uint(dc.superblock.AllocatedDeviceExtents))
	for offset := uint(0); offset < remaining; offset += EXTENT_BATCH {
		size := min(remaining-offset, EXTENT_BATCH)
		if err := dc.ReadExtents(eb[:size], offset); err != nil {
			return nil, err
		}
		for i := uint(0); i < size; i++ {
			if eb[i].SnapshotId == snapshotId {
				eidx := eb[i].ExtentPos
				sem.extentBitmap.Set(eidx)
				sem.extents[eidx] = eb[i]
				// Convert ExtentPos from position in volume to position in device
				sem.extents[eidx].ExtentPos = uint32(offset + i)
			}
		}
	}
	return sem, nil
}

// Get the map of a volume starting at a snapshot and including all ancestors.
func GetVolumeExtentMap(dc *DeviceContext, deviceSize uint64, snapshotId uint16) (*ExtentMap, error) {
	vem, err := GetSnapshotExtentMap(dc, deviceSize, snapshotId)
	if err != nil {
		return nil, err
	}

	sid := snapshotId
	for sid := dc.snapshots[sid-1].ParentSnapshotId; sid > 0; sid = dc.snapshots[sid-1].ParentSnapshotId {
		sem, err := GetSnapshotExtentMap(dc, deviceSize, sid)
		if err != nil {
			return nil, err
		}
		sem.extentBitmap.Range(func(x uint32) {
			if vem.extents[x].SnapshotId == 0 {
				vem.extents[x] = sem.extents[x]
				vem.extentBitmap.Set(x)
			}
		})
	}
	return vem, nil
}

// Write extent metadata to the device.
func (em *ExtentMap) WriteExtent(eidx uint32) error {
	e := em.extents[eidx]
	// Convert ExtentPos from position in device to position in volume
	e.ExtentPos = eidx
	return em.dc.WriteExtent(&e, uint(em.extents[eidx].ExtentPos))
}

// Allocate a new extent into the map.
func (em *ExtentMap) NewExtentToSnapshot(eidx uint32, snapshotId uint16) error {
	em.extents[eidx].SnapshotId = snapshotId
	em.extents[eidx].ExtentPos = em.dc.superblock.AllocatedDeviceExtents
	if err := em.WriteExtent(eidx); err != nil {
		return err
	}
	em.dc.superblock.AllocatedDeviceExtents++
	return nil
}

// Copy over all data from an extent to another snapshot and update the map.
func (em *ExtentMap) CopyExtentToSnapshot(eidx uint32, snapshotId uint16) error {
	psrc := em.extents[eidx].ExtentPos
	pdst := em.dc.superblock.AllocatedDeviceExtents
	if err := em.dc.CopyExtentData(uint(psrc), uint(pdst)); err != nil {
		return err
	}
	em.extents[eidx].SnapshotId = snapshotId
	em.extents[eidx].ExtentPos = pdst
	if err := em.WriteExtent(eidx); err != nil {
		return err
	}
	em.dc.superblock.AllocatedDeviceExtents++
	return nil
}

// Copy the whole map to another snapshot.
func (em *ExtentMap) CopyAllToSnapshot(snapshotId uint16) error {
	var cbErr error
	em.extentBitmap.Range(func(x uint32) {
		if cbErr != nil {
			return
		}
		if err := em.CopyExtentToSnapshot(x, snapshotId); err != nil {
			cbErr = err
			return
		}
	})
	if cbErr != nil {
		return cbErr
	}
	return nil

}

// Clear all metadata included in the map.
func (em *ExtentMap) MergeAllInto(emdst *ExtentMap, snapshotId uint16) error {
	var cbErr error
	em.extentBitmap.Range(func(x uint32) {
		if cbErr != nil {
			return
		}
		if emdst.extents[x].SnapshotId != 0 {
			return
		}
		emdst.extents[x] = em.extents[x]
		emdst.extents[x].SnapshotId = snapshotId
		emdst.extentBitmap.Set(x)
		em.extents[x] = ExtentMetadata{}
		em.extentBitmap.Remove(x)
		e := em.extents[x]
		// Convert ExtentPos from position in device to position in volume
		e.ExtentPos = x
		if err := em.dc.WriteExtent(&e, uint(em.extents[x].ExtentPos)); err != nil {
			cbErr = err
			return
		}
	})
	if cbErr != nil {
		return cbErr
	}
	return nil
}

// Clear all metadata included in the map.
func (em *ExtentMap) ClearAll() error {
	var e ExtentMetadata
	var cbErr error
	em.extentBitmap.Range(func(x uint32) {
		if cbErr != nil {
			return
		}
		eidx := em.extents[x].ExtentPos
		if err := em.dc.WriteExtent(&e, uint(eidx)); err != nil {
			cbErr = err
			return
		}
	})
	if cbErr != nil {
		return cbErr
	}
	return nil
}
