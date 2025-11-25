package dbs

import (
	"fmt"
	"os"
)

func MigrateVolume(device string, volume string, policy string) error {
	dc, err := GetDeviceContext(device)
	if err != nil {
		return err
	}
	vol := dc.FindVolume(volume)
	if vol == nil {
		return fmt.Errorf("volume %v not found", volume)
	}

	fmt.Printf("Starting storage migration for device: %s , volume : %s with policy %s\n", device, volume, policy)

	mex, err := calculateMigratedExtents(policy, vol, dc)

	if uint(dc.superblock.AllocatedSecondaryExtents)+uint(len(mex)*EXTENT_SIZE) > dc.totalSecondaryExtents {
		//return fmt.Errorf("no space left on device")
		//TODO
		fmt.Println("Sec storage size :", dc.totalSecondaryExtents, " Allocated sec extents :", dc.superblock.AllocatedSecondaryExtents, " Extents to migrate size :", len(mex))
		fmt.Printf("Warning: not enough space on device for migration, proceeding anyway\n")
	}

	if err != nil {
		return err
	}

	if len(mex) == 0 {
		fmt.Printf("No extents to migrate for volume: %s\n", volume)
		return nil
	}

	fmt.Printf("Number of extents to migrate: %d\n", len(mex))

	err = copyToSecondaryStorage(mex, dc)
	if err != nil {
		return err
	}

	dc.WriteMetadata()

	fmt.Printf("Storage migration completed ")

	return nil
}

func copyToSecondaryStorage(mex []ExtentMetadata, dc *DeviceContext) error {

	for i, extent := range mex {
		fmt.Printf("Migrating extent %d : ExtentPos %d , SnapshotId %d , DeviceLocation %d \n", i, extent.ExtentPos, extent.SnapshotId, extent.DeviceLocation)

		err := dc.CopyExtentToSecondary(&extent)
		if err != nil {
			return err
		}

	}
	return nil
}

func calculateMigratedExtents(policy string, vol *VolumeMetadata, dc *DeviceContext) ([]ExtentMetadata, error) {
	var migratedExtents []ExtentMetadata
	vem, err := GetVolumeExtentMap(dc, vol.VolumeSize, vol.SnapshotId)

	if err != nil {
		return migratedExtents, err
	}

	switch policy {
	case "lru100":
		//Placeholder: select least recently used 100 extents
	default: //Migrate all extents that are not in the latest snapshot

		for i, extent := range vem.extents {

			if extent.SnapshotId == 0 {
				fmt.Println("Skipping unallocated extent at index ", i)
			}

			if extent.SnapshotId != vol.SnapshotId && extent.DeviceLocation == PRIMARY_DEVICE {
				migratedExtents = append(migratedExtents, extent)
			}

		}

	}

	return migratedExtents, nil
}

func GetDeviceStats(device string) (*DirectFile, int64, error) {
	f, err := NewDirectFile(device, os.O_RDWR, 0660)
	if err != nil {
		return nil, 0, fmt.Errorf("cannot open %v: %w", device, err)
	}
	deviceSize, err := f.Size()
	if err != nil {
		return nil, 0, err
	}
	if deviceSize == 0 {
		return nil, 0, fmt.Errorf("device with zero size")
	}
	if deviceSize < (100 * (1 << 20)) {
		return nil, 0, fmt.Errorf("device size less than 100 MB")
	}

	return f, deviceSize, nil

}
