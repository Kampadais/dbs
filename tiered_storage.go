package dbs

func MigrateVolume(device string, volume string, policy string) error {
	dc, err := GetDeviceContext(device)
	if err != nil {
		return err
	}
	vol, err := OpenVolume(device, volume)

	if err != nil {
		return err
	}

	mex, err := calculateMigratedExtents(policy, vol, dc)

	if uint(dc.superblock.AllocatedSecondaryExtents)+uint(mex*EXTENT_SIZE) > dc.totalSecondaryExtents {
		//return fmt.Errorf("no space left on device")
		//TODO
	}

	if err != nil {
		return err
	}

	err = dc.WriteMetadata()
	if err != nil {
		return err
	}

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
