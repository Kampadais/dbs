#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/errno.h>
#include <fcntl.h>
#include <time.h>

#include "dbs.h"

#define MIN(x, y) (((x) < (y)) ? (x) : (y))

// Internal structures, sizes

#define DBS_VOLUME_NAME_SIZE 256
#define DBS_EXTENT_SIZE 131072      // 128 KB
#define DBS_BLOCKS_PER_EXTENT 256   // 256 512-byte blocks
#define DBS_BLOCK_BITS_IN_EXTENT 8
#define DBS_BLOCK_MASK_IN_EXTENT 0xFF
#define DBS_EXTENT_BITMAP_SIZE 8    // Each bitmap is 32 bits

typedef struct {
    uint16_t snapshot_id;           // Index in snapshots table + 1
    uint64_t volume_size;
    char volume_name[DBS_VOLUME_NAME_SIZE];
} dbs_volume_metadata;

typedef struct {
    uint16_t parent_snapshot_id;
    time_t created_at;
} dbs_snapshot_metadata;

typedef struct {
    uint16_t snapshot_id;
    uint32_t extent_pos;            // Address up to ~500 TB per volume
    uint32_t block_bitmap[DBS_EXTENT_BITMAP_SIZE];
} dbs_extent_metadata;

const uint8_t dbs_magic[] = {0x44, 0x42, 0x53, 0x40, 0x33, 0x39, 0x0d, 0x21};
const uint16_t dbs_version = 0x0001;

#define DBS_MAX_VOLUMES 256         // Named volumes
#define DBS_MAX_SNAPSHOTS 65536     // Total snapshots

typedef struct {
    char magic[8];
    uint16_t version;
    uint64_t disk_size;
    uint32_t allocated_extents;
    dbs_volume_metadata volumes[DBS_MAX_VOLUMES];
    dbs_snapshot_metadata snapshots[DBS_MAX_SNAPSHOTS];
} dbs_superblock;

// Private variables and parameters

typedef struct {
    int fd;
    uint32_t extent_offset;
    uint32_t data_offset;
    dbs_superblock *superblock;
    dbs_volume_metadata *volume;
    dbs_extent_metadata *extents;
} dbs_volume_info;

#define DBS_EXTENT_LOAD_BATCH 65536

// Bitmap operations

uint8_t bitmap_check_bit(uint32_t *bitmap, uint32_t pos) {
    return (bitmap[pos >> 5] & (1 << (pos & 0x1F))) == 1;
}

void bitmap_set_bit(uint32_t *bitmap, uint32_t pos) {
    bitmap[pos >> 5] = bitmap[pos >> 5] | (1 << (pos & 0x1F));
}

void bitmap_unset_bit(uint32_t *bitmap, uint32_t pos) {
    bitmap[pos >> 5] = bitmap[pos >> 5] & ~(1 << (pos & 0x1F));
}

uint8_t bitmap_is_empty(uint32_t *bitmap, uint32_t size) {
    uint32_t sum = 0;
    for (int i = 0; i < size; i++) {
        sum |= bitmap[sum];
        if (sum)
            return 0;
    }
    return 1;
}

// Metadata helpers

uint32_t create_snapshot(dbs_superblock *superblock, uint16_t parent_snapshot_id) {
    uint32_t snapshot_idx;

    for (snapshot_idx = 0; snapshot_idx < DBS_MAX_SNAPSHOTS || superblock->snapshots[snapshot_idx].created_at != 0; snapshot_idx++);
    if (snapshot_idx == DBS_MAX_SNAPSHOTS)
        return 0;

    superblock->snapshots[snapshot_idx].parent_snapshot_id = parent_snapshot_id;
    superblock->snapshots[snapshot_idx].created_at = time(NULL);

    return snapshot_idx + 1;
}

// API

dbs_context dbs_open(char *device, char *volume_name, uint64_t volume_size) {
    // Open device and read super block
    int fd = open(device, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (fd < 0) {
        printf("ERROR: Cannot open %s: %s\n", device, strerror(errno));
        return NULL;
    }

    dbs_superblock *superblock = (dbs_superblock *)malloc(sizeof(dbs_superblock));
    if (!superblock) {
        printf("ERROR: Cannot allocate context\n");
        return NULL;
    }
    if (pread(fd, superblock, sizeof(dbs_superblock), 0) != sizeof(dbs_superblock)) {
        printf("ERROR: Cannot read superblock from device: %s\n", strerror(errno));
        goto context_failed;
    }

    // Check magic
    if (memcmp((uint8_t *)superblock, dbs_magic, 8) != 0) {
        printf("ERROR: Device not formatted\n");
        goto context_failed;
    }
    if (superblock->version != dbs_version) {
        printf("ERROR: Metadata version mismatch\n");
        goto context_failed;
    }

    // Get or create volume metadata
    uint8_t created = 0;
    uint16_t volume_idx;
    for (volume_idx = 0; volume_idx < DBS_MAX_VOLUMES || superblock->volumes[volume_idx].snapshot_id != 0; volume_idx++) {
        if (strncmp(volume_name, superblock->volumes[volume_idx].volume_name, DBS_VOLUME_NAME_SIZE - 1) == 0)
            break;
    }
    if (volume_idx == DBS_MAX_VOLUMES) {
        printf("ERROR: Max volume count reached\n");
        goto context_failed;
    }
    if (superblock->volumes[volume_idx].snapshot_id == 0) {
        printf("INFO: Creating volume %s\n", device);
        uint16_t snapshot_id;
        snapshot_id = create_snapshot(superblock, 0);
        if (!snapshot_id) {
            printf("ERROR: Max snapshot count reached\n");
            goto context_failed;
        }
        superblock->volumes[volume_idx].snapshot_id = snapshot_id;
        superblock->volumes[volume_idx].volume_size = volume_size;
        strncpy(superblock->volumes[volume_idx].volume_name, volume_name, DBS_VOLUME_NAME_SIZE - 1);
        created = 1;
    } else {
        if (superblock->volumes[volume_idx].volume_size != volume_size) {
            printf("ERROR: Volume size mismatch %llu != %llu\n", superblock->volumes[volume_idx].volume_size, volume_size);
            goto context_failed;
        }
    }

    // Prepare context
    uint32_t total_volume_extents = (volume_size / DBS_EXTENT_SIZE) + (volume_size % DBS_EXTENT_SIZE ? 1 : 0);
    dbs_volume_info *volume_info = (dbs_volume_info *)malloc(sizeof(dbs_volume_info));
    if (!volume_info) {
        printf("ERROR: Cannot allocate context\n");
        goto context_failed;
    }
    dbs_extent_metadata *extents = (dbs_extent_metadata *)malloc(sizeof(dbs_extent_metadata) * total_volume_extents);
    if (!extents) {
        printf("ERROR: Cannot allocate context\n");
        free(volume_info);
        goto context_failed;
    }
    volume_info->fd = fd;
    volume_info->extent_offset = sizeof(superblock);
    uint32_t total_disk_extents = superblock->disk_size / DBS_EXTENT_SIZE;
    uint32_t metadata_size = sizeof(superblock) + (sizeof(dbs_extent_metadata) * total_disk_extents);
    uint32_t data_offset = ((metadata_size / DBS_EXTENT_SIZE) + (metadata_size % DBS_EXTENT_SIZE ? 1 : 0)) * DBS_EXTENT_SIZE;
    volume_info->data_offset = data_offset;
    volume_info->superblock = superblock;
    volume_info->volume = &(superblock->volumes[volume_idx]);
    volume_info->extents = extents;
    memset(extents, 0, sizeof(dbs_extent_metadata) * total_volume_extents);

    // Populate extents
    if (created)
        return (dbs_context)volume_info;

    dbs_extent_metadata *disk_extents = (dbs_extent_metadata *)malloc(sizeof(dbs_extent_metadata) * DBS_EXTENT_LOAD_BATCH);
    if (!disk_extents) {
        printf("ERROR: Cannot allocate extent buffer\n");
        goto populate_extents_failed;
    }

    uint16_t current_snapshot_id = volume_info->volume->snapshot_id;
    do {
        // Scan all extent metadata for the current snapshot id and place them in the extent map
        uint64_t extents_remaining = MIN(total_disk_extents, superblock->allocated_extents);
        uint32_t batch_extent_start = 0;
        while (extents_remaining) {
            uint32_t batch_size = MIN(DBS_EXTENT_LOAD_BATCH, extents_remaining);
            uint32_t batch_offset = volume_info->extent_offset + (sizeof(dbs_extent_metadata) * batch_extent_start);
            if (pread(fd, &disk_extents, batch_size * sizeof(dbs_extent_metadata), batch_offset) != (batch_size * sizeof(dbs_extent_metadata))) {
                printf("ERROR: Cannot read extents from device: %s\n", strerror(errno));
                free(disk_extents);
                goto populate_extents_failed;
            }

            for (uint32_t i = 0; i < batch_size; i++) {
                if (disk_extents[i].snapshot_id == current_snapshot_id && volume_info->extents[disk_extents[i].extent_pos].snapshot_id == 0) {
                    memcpy(&(volume_info->extents[disk_extents[i].extent_pos]), &(disk_extents[i]), sizeof(dbs_extent_metadata));
                    // Convert extent_pos from position in volume to position in disk
                    volume_info->extents[disk_extents[i].extent_pos].extent_pos = batch_extent_start + i;
                }
            }
            extents_remaining -= batch_size;
            batch_extent_start += batch_size;
        };

        // Move on to the parent volume
        current_snapshot_id = superblock->snapshots[current_snapshot_id - 1].parent_snapshot_id;
    } while (current_snapshot_id);

    return (dbs_context)volume_info;

context_failed:
    free(superblock);
    return NULL;

populate_extents_failed:
    free(volume_info->extents);
    free(volume_info->superblock);
    free(volume_info);
    return NULL;
}

void dbs_close(dbs_context volume) {
    dbs_volume_info *volume_info = (dbs_volume_info *)volume;

    close(volume_info->fd);
    free(volume_info->extents);
    free(volume_info);
}

uint8_t dbs_read(dbs_context volume, uint64_t block, void *data) {
    dbs_volume_info *volume_info = (dbs_volume_info *)volume;

    uint32_t total_volume_extents = (volume_info->volume->volume_size / DBS_EXTENT_SIZE) + (volume_info->volume->volume_size % DBS_EXTENT_SIZE ? 1 : 0);
    uint32_t extent_idx = block >> DBS_BLOCK_BITS_IN_EXTENT;
    if (extent_idx > total_volume_extents) {
        return 1;
    }

    // Unallocated extent or block
    dbs_extent_metadata *extent_metadata = &(volume_info->extents[extent_idx]);
    if (extent_metadata->snapshot_id == 0 || !bitmap_check_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT)) {
        memset(data, 0, 512);
        return 0;
    }

    // Read data from device
    uint64_t block_data_offset = volume_info->data_offset + (extent_metadata->extent_pos * DBS_EXTENT_SIZE) + ((block & DBS_BLOCK_MASK_IN_EXTENT) * 512);
    if (pread(volume_info->fd, data, 512, block_data_offset) != 512)
        return -1;

    return 0;
}

uint8_t dbs_write(dbs_context volume, uint64_t block, void *data) {
    dbs_volume_info *volume_info = (dbs_volume_info *)volume;

    uint32_t total_volume_extents = (volume_info->volume->volume_size / DBS_EXTENT_SIZE) + (volume_info->volume->volume_size % DBS_EXTENT_SIZE ? 1 : 0);
    uint32_t extent_idx = block >> DBS_BLOCK_BITS_IN_EXTENT;
    if (extent_idx > total_volume_extents) {
        return 1;
    }

    // Unallocated or previous snapshot extent
    dbs_extent_metadata *extent_metadata = &(volume_info->extents[extent_idx]);
    if (extent_metadata->snapshot_id == 0 || extent_metadata->snapshot_id != volume_info->volume->snapshot_id) {
        // XXX Allocate new extent
        return -1;
    }

    // Write data to device
    uint64_t block_data_offset = volume_info->data_offset + (extent_metadata->extent_pos * DBS_EXTENT_SIZE) + ((block & DBS_BLOCK_MASK_IN_EXTENT) * 512);
    if (pwrite(volume_info->fd, data, 512, block_data_offset) != 512) {
        printf("ERROR: Failed writing to device at block %llu: %s\n", block, strerror(errno));
        return -1;
    }

    // Update metadata
    if (bitmap_check_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT))
        return 0;

    bitmap_set_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT);
    uint64_t extent_data_offset = volume_info->extent_offset + (extent_metadata->extent_pos * sizeof(dbs_extent_metadata));
    if (pwrite(volume_info->fd, extent_metadata, sizeof(dbs_extent_metadata), extent_data_offset) != sizeof(dbs_extent_metadata)) {
        printf("ERROR: Failed writing metadata to device for block %llu: %s\n", block, strerror(errno));
        return -1;
    }

    return 0;
}

uint8_t dbs_unmap(dbs_context volume, uint64_t block) {
    dbs_volume_info *volume_info = (dbs_volume_info *)volume;

    uint32_t total_volume_extents = (volume_info->volume->volume_size / DBS_EXTENT_SIZE) + (volume_info->volume->volume_size % DBS_EXTENT_SIZE ? 1 : 0);
    uint32_t extent_idx = block >> DBS_BLOCK_BITS_IN_EXTENT;
    if (extent_idx > total_volume_extents) {
        return 1;
    }

    // Unallocated extent or block
    dbs_extent_metadata *extent_metadata = &(volume_info->extents[extent_idx]);
    if (extent_metadata->snapshot_id == 0 || !bitmap_check_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT)) {
        return 0;
    }

    // Update metadata
    bitmap_unset_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT);
    uint64_t extent_data_offset = volume_info->extent_offset + (extent_metadata->extent_pos * sizeof(dbs_extent_metadata));
    if (bitmap_is_empty(extent_metadata->block_bitmap, DBS_EXTENT_BITMAP_SIZE) == 0) {
        // Release if not used
        memset(extent_metadata, 0, sizeof(dbs_extent_metadata));
    }
    if (pwrite(volume_info->fd, extent_metadata, sizeof(dbs_extent_metadata), extent_data_offset) != sizeof(dbs_extent_metadata)) {
        printf("ERROR: Failed writing metadata to device for block %llu: %s\n", block, strerror(errno));
        return -1;
    }

    return 0;
}
