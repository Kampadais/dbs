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
	vol, err := OpenVolume(device, volume)

	if err != nil {
		return err
	}

	fmt.Printf("Starting storage migration for device: %s , volume : %s with policy %s\n", device, volume, policy)

	mex, err := calculateMigratedExtents(policy, vol, dc)

	if uint(dc.superblock.AllocatedSecondaryExtents)+uint(mex*EXTENT_SIZE) > dc.totalSecondaryExtents {
		//return fmt.Errorf("no space left on device")
		//TODO
		fmt.Println("Sec storage size :", dc.totalSecondaryExtents, " Allocated sec extents :", dc.superblock.AllocatedSecondaryExtents, " Extents to migrate size :", mex)
		fmt.Printf("Warning: not enough space on device for migration, proceeding anyway\n")
	}

	if err != nil {
		return err
	}

	fmt.Printf("Number of extents to migrate: %d\n", mex)

	err = dc.WriteMetadata()
	if err != nil {
		return err
	}

	fmt.Printf("Storage migration completed \n")

	return nil
}

func calculateMigratedExtents(policy string, vol *VolumeContext, dc *DeviceContext) (int, error) {
	var migratedCount = 0
	switch policy {
	case "lru100":
		//Placeholder: select least recently used 100 extents
	default: //Migrate all extents that are not in the latest snapshot

		for i := range vol.vem.extents {
			extent := &vol.vem.extents[i] // take pointer to original
			if extent.SnapshotId == 0 {
				continue
			}
			if extent.SnapshotId != vol.volume.SnapshotId && extent.DeviceLocation == PRIMARY_DEVICE {
				//migratedExtents = append(migratedExtents, extent)
				err := dc.CopyExtentToSecondary(extent)

				if err != nil {
					return migratedCount, err
				}
				extent.DeviceLocation = SECONDARY_DEVICE
				migratedCount++
				err = dc.WriteExtent(extent, uint(i), SECONDARY_DEVICE)
				if err != nil {
					return 0, err
				}
			}
		}

	}

	return migratedCount, nil
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
