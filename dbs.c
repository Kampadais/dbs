#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

#include "dbs.h"

#define DIV_ROUND_UP(N, S) (((N) + (S) - 1) / (S))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

// Internal structures, sizes

#define DBS_MAX_VOLUMES 256         // Named volumes
#define DBS_MAX_SNAPSHOTS 65536     // Total snapshots

#define DBS_VOLUME_NAME_SIZE 256
#define DBS_EXTENT_SIZE 131072      // 128 KB
#define DBS_BLOCKS_PER_EXTENT 256   // 256 512-byte blocks
#define DBS_BLOCK_BITS_IN_EXTENT 8
#define DBS_BLOCK_MASK_IN_EXTENT 0xFF
#define DBS_EXTENT_BITMAP_SIZE 8    // Each bitmap is 32 bits

const uint8_t dbs_magic[] = {0x44, 0x42, 0x53, 0x40, 0x33, 0x39, 0x0d, 0x21};
const uint32_t dbs_version = 0x00010000; // 16-bit major, 8-bit minor, 8-bit patch

typedef struct {
    char magic[8];
    uint32_t version;
    uint32_t allocated_extents;
    uint64_t device_size;
} __attribute__((packed)) dbs_superblock;

typedef struct {
    uint16_t snapshot_id;           // Index in snapshots table + 1
    uint64_t volume_size;
    char volume_name[DBS_VOLUME_NAME_SIZE];
} __attribute__((packed)) dbs_volume_metadata;

typedef struct {
    uint16_t parent_snapshot_id;
    time_t created_at;
} __attribute__((packed)) dbs_snapshot_metadata;

typedef struct {
    dbs_volume_metadata volumes[DBS_MAX_VOLUMES];
    dbs_snapshot_metadata snapshots[DBS_MAX_SNAPSHOTS];
} dbs_device_metadata;

typedef struct {
    uint16_t snapshot_id;
    uint32_t extent_pos;            // Address up to ~500 TB per volume
    uint32_t block_bitmap[DBS_EXTENT_BITMAP_SIZE];
} dbs_extent_metadata;

// Private variables and parameters

typedef struct {
    int fd;
    uint32_t extent_offset;
    uint32_t total_device_extents;
    uint32_t metadata_size;
    uint32_t data_offset;
    dbs_superblock superblock;
    dbs_device_metadata device_metadata;
} dbs_device_info;

typedef struct {
    int fd;
    uint32_t extent_offset;
    uint32_t data_offset;
    dbs_superblock superblock;
    dbs_volume_metadata volume;
    dbs_extent_metadata *extents;
} dbs_volume_info;

#define DBS_EXTENT_BATCH 65536

// Bitmap operations

uint8_t bitmap_check_bit(uint32_t *bitmap, uint32_t pos) {
    return ((bitmap[pos >> 5] & (1 << (pos & 0x1F))) > 0 ? DBS_SUCCESS : DBS_FAILURE);
}

void bitmap_set_bit(uint32_t *bitmap, uint32_t pos) {
    bitmap[pos >> 5] = bitmap[pos >> 5] | (1 << (pos & 0x1F));
}

void bitmap_unset_bit(uint32_t *bitmap, uint32_t pos) {
    bitmap[pos >> 5] = bitmap[pos >> 5] & ~(1 << (pos & 0x1F));
}

dbs_bool bitmap_is_empty(uint32_t *bitmap, uint32_t size) {
    for (int i = 0; i < size; i++) {
        if (bitmap[i])
            return DBS_FAILURE;
    }
    return DBS_SUCCESS;
}

// Metadata helpers

void fill_device_attributes(dbs_device_info *device_info) {
    device_info->extent_offset = (1 + DIV_ROUND_UP(sizeof(dbs_device_metadata), 512)) * 512;
    device_info->total_device_extents = (device_info->superblock.device_size - device_info->extent_offset) / DBS_EXTENT_SIZE;
    device_info->metadata_size = device_info->extent_offset + (sizeof(dbs_extent_metadata) * device_info->total_device_extents);
    device_info->data_offset = DIV_ROUND_UP(sizeof(dbs_device_metadata), DBS_EXTENT_SIZE) * DBS_EXTENT_SIZE;
}

dbs_bool get_device_info(char *device, dbs_device_info *device_info, dbs_bool check) {
    // Open device
    device_info->fd = open(device, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (device_info->fd < 0) {
        printf("ERROR: Cannot open %s: %s\n", device, strerror(errno));
        return DBS_FAILURE;
    }

    // Read superblock and metadata
    if (pread(device_info->fd, &(device_info->superblock), sizeof(dbs_superblock), 0) != sizeof(dbs_superblock)) {
        printf("ERROR: Cannot read superblock: %s\n", strerror(errno));
        return DBS_FAILURE;
    }
    if (pread(device_info->fd, &(device_info->device_metadata), sizeof(dbs_device_metadata), 512) != sizeof(dbs_device_metadata)) {
        printf("ERROR: Cannot read metadata: %s\n", strerror(errno));
        return DBS_FAILURE;
    }

    if (!check)
        return DBS_SUCCESS;

    // Check magic and version
    if (memcmp(device_info->superblock.magic, dbs_magic, 8) != 0) {
        printf("ERROR: Device not initialized\n");
        return DBS_FAILURE;
    }
    if (device_info->superblock.version != dbs_version) {
        printf("ERROR: Metadata version mismatch\n");
        return DBS_FAILURE;
    }

    fill_device_attributes(device_info);
    return DBS_SUCCESS;
}

uint32_t add_snapshot(dbs_device_metadata *device_metadata, uint16_t parent_snapshot_id) {
    uint32_t snapshot_idx;
    for (snapshot_idx = 0; snapshot_idx < DBS_MAX_SNAPSHOTS || device_metadata->snapshots[snapshot_idx].created_at != 0; snapshot_idx++);
    if (snapshot_idx == DBS_MAX_SNAPSHOTS)
        return 0;
    device_metadata->snapshots[snapshot_idx].parent_snapshot_id = parent_snapshot_id;
    device_metadata->snapshots[snapshot_idx].created_at = time(NULL);
    return snapshot_idx + 1;
}

dbs_bool write_superblock(dbs_volume_info *volume_info) {
    if (pwrite(volume_info->fd, &(volume_info->superblock), sizeof(dbs_superblock), 0) != sizeof(dbs_superblock))
        return DBS_FAILURE;
    return DBS_SUCCESS;
}

dbs_bool write_extent_metadata(dbs_volume_info *volume_info, uint32_t extent_idx) {
    dbs_extent_metadata extent_metadata;
    uint64_t extent_data_offset = volume_info->extent_offset + (volume_info->extents[extent_idx].extent_pos * sizeof(dbs_extent_metadata));
    memcpy(&extent_metadata, &(volume_info->extents[extent_idx]), sizeof(dbs_extent_metadata));
    extent_metadata.extent_pos = extent_idx;
    if (pwrite(volume_info->fd, &extent_metadata, sizeof(dbs_extent_metadata), extent_data_offset) != sizeof(dbs_extent_metadata))
        return DBS_FAILURE;
    return DBS_SUCCESS;
}

// Management functions

dbs_bool dbs_init_device(char *device) {
    dbs_device_info device_info;

    device_info.fd = open(device, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (device_info.fd < 0) {
        printf("ERROR: Cannot open %s: %s\n", device, strerror(errno));
        goto fail_with_device_info;
    }

    struct stat device_stat;
    if (fstat(device_info.fd, &device_stat) < 0) {
        printf("ERROR: Cannot get device size: %s\n", strerror(errno));
        goto fail_with_device_info;
    }

    memcpy(device_info.superblock.magic, dbs_magic, 8);
    device_info.superblock.version = dbs_version;
    device_info.superblock.allocated_extents = 0;
    device_info.superblock.device_size = device_stat.st_size;
    fill_device_attributes(&device_info);

    uint32_t empty_extents_size = sizeof(dbs_extent_metadata) * DBS_EXTENT_BATCH;
    dbs_extent_metadata *empty_extents = (dbs_extent_metadata *)calloc(DBS_EXTENT_BATCH, sizeof(dbs_extent_metadata));
    if (!empty_extents) {
        printf("ERROR: Cannot allocate extent buffer\n");
        goto fail_with_device_info;
    }
    for (uint32_t device_offset = 0; device_offset < device_info.metadata_size; device_offset += empty_extents_size) {
        if (pwrite(device_info.fd, empty_extents, empty_extents_size, device_offset) != empty_extents_size)
        goto fail_with_device_info;
    }
    if (pwrite(device_info.fd, &(device_info.superblock), sizeof(dbs_superblock), 0) != sizeof(dbs_superblock))
        goto fail_with_device_info;

    close(device_info.fd);
    return DBS_SUCCESS;

fail_with_device_info:
    close(device_info.fd);
    return DBS_FAILURE;
}

void dbs_vacuum_device(char *device) {
    printf("ERROR: Not implemented\n");
}

void dbs_list_volumes(char *device) {
    dbs_device_info device_info;
    dbs_volume_metadata *volumes = device_info.device_metadata.volumes;
    dbs_snapshot_metadata *snapshots = device_info.device_metadata.snapshots;
    if (!get_device_info(device, &device_info, 1))
        goto fail_with_device_info;

    char format_string[] = "%6s | %6s | %10s | %20s | %s\n";
    char size_string[20];
    char created_at_string[20];
    printf(format_string, "ID", "Snap.", "Size", "Created at", "Name");
    printf(format_string, "------", "------", "----------", "--------------------", "----------------------------------------");
    uint16_t volume_idx;
    for (volume_idx = 0; volume_idx < DBS_MAX_VOLUMES || volumes[volume_idx].snapshot_id != 0; volume_idx++) {
        sprintf(size_string, "%.02lf GB", (double)volumes[volume_idx].volume_size / (1024.0 * 1024.0 * 1024.0));
        uint16_t snapshot_idx = volumes[volume_idx].snapshot_id - 1;
        strftime(created_at_string, 20, "%Y-%m-%d %H:%M:%S", localtime(&(snapshots[snapshot_idx].created_at)));
        uint16_t snapshot_count;
        for (snapshot_count = 1; snapshots[snapshot_idx].parent_snapshot_id != 0; snapshot_count++)
            snapshot_idx = snapshots[snapshot_idx].parent_snapshot_id - 1;
        printf(format_string, volumes[volume_idx].snapshot_id, snapshot_count, size_string, created_at_string, volumes[volume_idx].volume_name);
    }

fail_with_device_info:
    close(device_info.fd);
}

void dbs_list_snapshots(char *device, char *volume_name) {
    dbs_device_info device_info;
    dbs_volume_metadata *volumes = device_info.device_metadata.volumes;
    dbs_snapshot_metadata *snapshots = device_info.device_metadata.snapshots;
    if (!get_device_info(device, &device_info, 1))
        goto fail_with_device_info;

    char format_string[] = "%6s | %20s\n";
    char created_at_string[20];
    printf(format_string, "ID", "Created at");
    printf(format_string, "------", "--------------------");
    uint16_t volume_idx;
    for (volume_idx = 0; volume_idx < DBS_MAX_VOLUMES || volumes[volume_idx].snapshot_id != 0; volume_idx++) {
        if (strncmp(volume_name, volumes[volume_idx].volume_name, DBS_VOLUME_NAME_SIZE - 1) == 0)
            break;
    }
    if (volume_idx == DBS_MAX_VOLUMES) {
        printf("ERROR: Volume not found\n");
        goto fail_with_device_info;
    }
    uint16_t snapshot_idx = volumes[volume_idx].snapshot_id - 1;
    do {
        strftime(created_at_string, 20, "%Y-%m-%d %H:%M:%S", localtime(&(snapshots[snapshot_idx].created_at)));
        printf(format_string, volumes[volume_idx].snapshot_id, created_at_string);
        snapshot_idx = snapshots[snapshot_idx].parent_snapshot_id - 1;
    } while (snapshot_idx >= 0);

fail_with_device_info:
    close(device_info.fd);
}

dbs_bool dbs_create_volume(char *device, char *volume_name, uint64_t volume_size) {
    dbs_device_info device_info;
    dbs_volume_metadata *volumes = device_info.device_metadata.volumes;
    if (!get_device_info(device, &device_info, 1))
        goto fail_with_device_info;

    // Find volume
    uint16_t volume_idx;
    for (volume_idx = 0; volume_idx < DBS_MAX_VOLUMES || volumes[volume_idx].snapshot_id != 0; volume_idx++) {
        if (strncmp(volume_name, volumes[volume_idx].volume_name, DBS_VOLUME_NAME_SIZE - 1) == 0)
            break;
    }
    if (volume_idx == DBS_MAX_VOLUMES) {
        printf("ERROR: Max volume count reached\n");
        goto fail_with_device_info;
    }
    if (volumes[volume_idx].snapshot_id != 0) {
        printf("ERROR: Volume %s already exists\n", volume_name);
        goto fail_with_device_info;
    }

    // Create volume
    uint16_t snapshot_id = add_snapshot(&(device_info.device_metadata), 0);
    if (!snapshot_id) {
        printf("ERROR: Max snapshot count reached\n");
        goto fail_with_device_info;
    }
    volumes[volume_idx].snapshot_id = snapshot_id;
    volumes[volume_idx].volume_size = volume_size;
    strncpy(volumes[volume_idx].volume_name, volume_name, DBS_VOLUME_NAME_SIZE - 1);

    if (pwrite(device_info.fd, &(device_info.device_metadata), sizeof(dbs_device_metadata), 512) != sizeof(dbs_device_metadata))
        goto fail_with_device_info;

    close(device_info.fd);
    return DBS_SUCCESS;

fail_with_device_info:
    close(device_info.fd);
    return DBS_FAILURE;
}

void dbs_snapshot_volume(char *device, char *volume_name) {
    dbs_device_info device_info;
    dbs_volume_metadata *volumes = device_info.device_metadata.volumes;
    if (!get_device_info(device, &device_info, 1))
        goto fail_with_device_info;

    // Find volume
    uint16_t volume_idx;
    for (volume_idx = 0; volume_idx < DBS_MAX_VOLUMES || volumes[volume_idx].snapshot_id != 0; volume_idx++) {
        if (strncmp(volume_name, volumes[volume_idx].volume_name, DBS_VOLUME_NAME_SIZE - 1) == 0)
            break;
    }
    if (volume_idx == DBS_MAX_VOLUMES || volumes[volume_idx].snapshot_id == 0) {
        printf("ERROR: Volume not found\n");
        goto fail_with_device_info;
    }

    // Create snapshot
    uint16_t snapshot_id = add_snapshot(&(device_info.device_metadata), volumes[volume_idx].snapshot_id);
    if (!snapshot_id) {
        printf("ERROR: Max snapshot count reached\n");
        goto fail_with_device_info;
    }
    volumes[volume_idx].snapshot_id = snapshot_id;

    if (pwrite(device_info.fd, &(device_info.device_metadata), sizeof(dbs_device_metadata), 512) != sizeof(dbs_device_metadata))
        goto fail_with_device_info;

    close(device_info.fd);
    return DBS_SUCCESS;

fail_with_device_info:
    close(device_info.fd);
    return DBS_FAILURE;
}

void dbs_delete_volume(char *device, char *volume_name) {
    printf("ERROR: Not implemented\n");
}

void dbs_clone_snapshot(char *device, char *volume_name, uint16_t snapshot_id) {
    printf("ERROR: Not implemented\n");
}

// Block API

dbs_context dbs_open(char *device, char *volume_name) {
    dbs_device_info device_info;
    dbs_volume_metadata *volumes = device_info.device_metadata.volumes;
    dbs_snapshot_metadata *snapshots = device_info.device_metadata.snapshots;
    if (!get_device_info(device, &device_info, 1))
        return NULL;

    // Find volume
    uint16_t volume_idx;
    for (volume_idx = 0; volume_idx < DBS_MAX_VOLUMES || volumes[volume_idx].snapshot_id != 0; volume_idx++) {
        if (strncmp(volume_name, volumes[volume_idx].volume_name, DBS_VOLUME_NAME_SIZE - 1) == 0)
            break;
    }
    if (volume_idx == DBS_MAX_VOLUMES || volumes[volume_idx].snapshot_id == 0) {
        printf("ERROR: Volume not found\n");
        goto fail_with_device_info;
    }

    // Prepare context
    dbs_volume_info *volume_info = (dbs_volume_info *)malloc(sizeof(dbs_volume_info));
    if (!volume_info) {
        printf("ERROR: Cannot allocate context\n");
        goto fail_with_device_info;
    }
    uint32_t total_volume_extents = DIV_ROUND_UP(volumes[volume_idx].volume_size, DBS_EXTENT_SIZE);
    dbs_extent_metadata *extents = (dbs_extent_metadata *)calloc(total_volume_extents, sizeof(dbs_extent_metadata));
    if (!extents) {
        printf("ERROR: Cannot allocate context\n");
        free(volume_info);
        goto fail_with_device_info;
    }
    volume_info->fd = device_info.fd;
    volume_info->extent_offset = device_info.extent_offset;
    volume_info->data_offset = device_info.data_offset;
    memcpy(&(volume_info->superblock), &(device_info.superblock), sizeof(dbs_superblock));
    memcpy(&(volume_info->volume), &(volumes[volume_idx]), sizeof(dbs_volume_metadata));
    volume_info->extents = extents;

    // Populate extents
    dbs_extent_metadata *device_extents = (dbs_extent_metadata *)malloc(sizeof(dbs_extent_metadata) * DBS_EXTENT_BATCH);
    if (!device_extents) {
        printf("ERROR: Cannot allocate extent buffer\n");
        goto fail_with_volume_info;
    }

    uint16_t current_snapshot_id = volume_info->volume.snapshot_id;
    do {
        // Scan all extent metadata for the current snapshot id and place them in the extent map
        uint64_t extents_remaining = MIN(device_info.total_device_extents, device_info.superblock.allocated_extents);
        uint32_t batch_extent_start = 0;
        while (extents_remaining) {
            uint32_t batch_size = MIN(DBS_EXTENT_BATCH, extents_remaining);
            uint32_t batch_offset = volume_info->extent_offset + (sizeof(dbs_extent_metadata) * batch_extent_start);
            if (pread(device_info.fd, &device_extents, batch_size * sizeof(dbs_extent_metadata), batch_offset) != (batch_size * sizeof(dbs_extent_metadata))) {
                printf("ERROR: Cannot read extents: %s\n", strerror(errno));
                free(device_extents);
                goto fail_with_volume_info;
            }

            for (uint32_t i = 0; i < batch_size; i++) {
                if (device_extents[i].snapshot_id == current_snapshot_id && volume_info->extents[device_extents[i].extent_pos].snapshot_id == 0) {
                    memcpy(&(volume_info->extents[device_extents[i].extent_pos]), &(device_extents[i]), sizeof(dbs_extent_metadata));
                    // Convert extent_pos from position in volume to position in device
                    volume_info->extents[device_extents[i].extent_pos].extent_pos = batch_extent_start + i;
                }
            }
            extents_remaining -= batch_size;
            batch_extent_start += batch_size;
        };

        // Move on to the parent volume
        current_snapshot_id = snapshots[current_snapshot_id - 1].parent_snapshot_id;
    } while (current_snapshot_id);

    return (dbs_context)volume_info;

fail_with_device_info:
    close(device_info.fd);
    return NULL;

fail_with_volume_info:
    close(volume_info->fd);
    free(volume_info->extents);
    free(volume_info);
    return NULL;
}

void dbs_close(dbs_context volume) {
    dbs_volume_info *volume_info = (dbs_volume_info *)volume;

    close(volume_info->fd);
    free(volume_info->extents);
    free(volume_info);
}

dbs_bool dbs_read(dbs_context volume, uint64_t block, void *data) {
    dbs_volume_info *volume_info = (dbs_volume_info *)volume;

    uint32_t total_volume_extents = DIV_ROUND_UP(volume_info->volume.volume_size, DBS_EXTENT_SIZE);
    uint32_t extent_idx = block >> DBS_BLOCK_BITS_IN_EXTENT;
    if (extent_idx > total_volume_extents) {
        return DBS_FAILURE;
    }

    // Unallocated extent or block
    dbs_extent_metadata *extent_metadata = &(volume_info->extents[extent_idx]);
    if (extent_metadata->snapshot_id == 0 || !bitmap_check_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT)) {
        memset(data, 0, 512);
        return DBS_SUCCESS;
    }

    // Read data from device
    uint64_t block_data_offset = volume_info->data_offset + (extent_metadata->extent_pos * DBS_EXTENT_SIZE) + ((block & DBS_BLOCK_MASK_IN_EXTENT) * 512);
    if (pread(volume_info->fd, data, 512, block_data_offset) != 512)
        return DBS_FAILURE;

    return DBS_SUCCESS;
}

dbs_bool dbs_write(dbs_context volume, uint64_t block, void *data) {
    dbs_volume_info *volume_info = (dbs_volume_info *)volume;

    uint32_t total_volume_extents = DIV_ROUND_UP(volume_info->volume.volume_size, DBS_EXTENT_SIZE);
    uint32_t extent_idx = block >> DBS_BLOCK_BITS_IN_EXTENT;
    if (extent_idx > total_volume_extents) {
        return DBS_FAILURE;
    }

    // Unallocated or previous snapshot extent
    dbs_extent_metadata *extent_metadata = &(volume_info->extents[extent_idx]);
    if (extent_metadata->snapshot_id == 0 || extent_metadata->snapshot_id != volume_info->volume.snapshot_id) {
        // Allocate new extent
        extent_metadata->snapshot_id = volume_info->volume.snapshot_id;
        extent_metadata->extent_pos = volume_info->superblock.allocated_extents;
        if (!write_extent_metadata(volume_info, extent_idx)) {
            printf("ERROR: Failed writing metadata for block %llu: %s\n", block, strerror(errno));
            return DBS_FAILURE;
        }

        // Update allocation count
        volume_info->superblock.allocated_extents++;
        if (!write_superblock(volume_info)) {
            printf("ERROR: Failed writing metadata for block %llu: %s\n", block, strerror(errno));
            return DBS_FAILURE;
        }
    }

    // Write data to device
    uint64_t block_data_offset = volume_info->data_offset + (extent_metadata->extent_pos * DBS_EXTENT_SIZE) + ((block & DBS_BLOCK_MASK_IN_EXTENT) * 512);
    if (pwrite(volume_info->fd, data, 512, block_data_offset) != 512) {
        printf("ERROR: Failed writing at block %llu: %s\n", block, strerror(errno));
        return DBS_FAILURE;
    }

    // Update metadata
    if (bitmap_check_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT))
        return DBS_SUCCESS;

    bitmap_set_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT);
    if (!write_extent_metadata(volume_info, extent_idx)) {
        printf("ERROR: Failed writing metadata for block %llu: %s\n", block, strerror(errno));
        return DBS_FAILURE;
    }

    return DBS_SUCCESS;
}

dbs_bool dbs_unmap(dbs_context volume, uint64_t block) {
    dbs_volume_info *volume_info = (dbs_volume_info *)volume;

    uint32_t total_volume_extents = DIV_ROUND_UP(volume_info->volume.volume_size, DBS_EXTENT_SIZE);
    uint32_t extent_idx = block >> DBS_BLOCK_BITS_IN_EXTENT;
    if (extent_idx > total_volume_extents) {
        return DBS_FAILURE;
    }

    // Unallocated extent or block
    dbs_extent_metadata *extent_metadata = &(volume_info->extents[extent_idx]);
    if (extent_metadata->snapshot_id == 0 || !bitmap_check_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT)) {
        return DBS_SUCCESS;
    }

    // Update metadata
    bitmap_unset_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT);
    if (bitmap_is_empty(extent_metadata->block_bitmap, DBS_EXTENT_BITMAP_SIZE) == 0) {
        // Release if not used
        extent_metadata->snapshot_id = 0;
    }
    if (!write_extent_metadata(volume_info, extent_idx)) {
        printf("ERROR: Failed writing metadata for block %llu: %s\n", block, strerror(errno));
        return DBS_FAILURE;
    }

    return DBS_SUCCESS;
}
