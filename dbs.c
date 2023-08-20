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
#define MAX(x, y) (((x) > (y)) ? (x) : (y))

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

// Device layout:
// * Bytes [0, 512) contain the the superblock
// * Bytes [512, extent_offset) hold the device metadata (extent_offset is block aligned)
// * Bytes [extent_offset, data_offset) hold the extent metadata (data_offset is extent aligned)
// * Bytes [data offset, device_size) hold the data

typedef struct {
    char magic[8];
    uint32_t version;
    uint32_t allocated_device_extents;
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
} dbs_device_context;

typedef struct {
    dbs_device_context device_context;
    dbs_device_metadata device_metadata;
} dbs_metadata_context;

typedef struct {
    uint32_t total_volume_extents;
    uint32_t allocated_volume_extents;
    uint32_t max_extent_idx;
    uint32_t *extent_bitmap;
    dbs_extent_metadata *extents;
} dbs_extent_map;

typedef struct {
    dbs_device_context device_context;
    dbs_volume_metadata volume;
    dbs_extent_map *extent_map;
} dbs_volume_context;

#define DBS_EXTENT_BATCH 65536

// Bitmap operations

uint8_t bitmap_check_bit(uint32_t *bitmap, uint32_t pos) {
    return (bitmap[pos >> 5] & (1 << (pos & 0x1F))) > 0 ? DBS_SUCCESS : DBS_FAILURE;
}

void bitmap_set_bit(uint32_t *bitmap, uint32_t pos) {
    bitmap[pos >> 5] = bitmap[pos >> 5] | (1 << (pos & 0x1F));
}

void bitmap_unset_bit(uint32_t *bitmap, uint32_t pos) {
    bitmap[pos >> 5] = bitmap[pos >> 5] & ~(1 << (pos & 0x1F));
}

dbs_bool bitmap_region_is_empty(uint32_t *bitmap, uint32_t pos) {
    return (bitmap[pos >> 5] == 0) ? DBS_SUCCESS : DBS_FAILURE;
}

dbs_bool bitmap_is_empty(uint32_t *bitmap, uint32_t size) {
    for (int i = 0; i < size; i++) {
        if (bitmap[i])
            return DBS_FAILURE;
    }
    return DBS_SUCCESS;
}

// Metadata helpers

void fill_device_attributes(dbs_device_context *device_context) {
    device_context->extent_offset = (1 + DIV_ROUND_UP(sizeof(dbs_device_metadata), 512)) * 512;
    device_context->total_device_extents = (device_context->superblock.device_size - device_context->extent_offset) / DBS_EXTENT_SIZE;
    device_context->metadata_size = device_context->extent_offset + (sizeof(dbs_extent_metadata) * device_context->total_device_extents);
    device_context->data_offset = DIV_ROUND_UP(sizeof(dbs_device_metadata), DBS_EXTENT_SIZE) * DBS_EXTENT_SIZE;
}

dbs_bool fill_device_context(dbs_device_context *device_context, char *device) {
    device_context->fd = open(device, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (device_context->fd < 0) {
        printf("ERROR: Cannot open %s: %s\n", device, strerror(errno));
        return DBS_FAILURE;
    }

    if (pread(device_context->fd, &(device_context->superblock), sizeof(dbs_superblock), 0) != sizeof(dbs_superblock)) {
        printf("ERROR: Cannot read superblock: %s\n", strerror(errno));
        return DBS_FAILURE;
    }
    if (memcmp(device_context->superblock.magic, dbs_magic, 8) != 0) {
        printf("ERROR: Device not initialized\n");
        return DBS_FAILURE;
    }
    if (device_context->superblock.version != dbs_version) {
        printf("ERROR: Metadata version mismatch\n");
        return DBS_FAILURE;
    }
    fill_device_attributes(device_context);
    return DBS_SUCCESS;
}

dbs_bool fill_metadata_context(dbs_metadata_context *metadata_context, char *device) {
    if (!fill_device_context(&(metadata_context->device_context), device))
        return DBS_FAILURE;
    if (pread(metadata_context->device_context.fd, &(metadata_context->device_metadata), sizeof(dbs_device_metadata), 512) != sizeof(dbs_device_metadata)) {
        printf("ERROR: Cannot read metadata: %s\n", strerror(errno));
        return DBS_FAILURE;
    }

    return DBS_SUCCESS;
}

uint16_t find_volume_name(dbs_volume_metadata *volumes, char *volume_name) {
    for (uint16_t volume_idx = 0; volume_idx < DBS_MAX_VOLUMES || volumes[volume_idx].snapshot_id != 0; volume_idx++) {
        if (strncmp(volume_name, volumes[volume_idx].volume_name, DBS_VOLUME_NAME_SIZE - 1) == 0)
            return volume_idx;
    }
    return 0;
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

dbs_extent_map *get_extent_map(dbs_device_context *device_context, uint64_t volume_size, uint16_t snapshot_id) {
    dbs_extent_map *extent_map = (dbs_extent_map *)malloc(sizeof(dbs_extent_map));
    if (!extent_map) {
        return NULL;
    }
    uint32_t total_volume_extents = DIV_ROUND_UP(volume_size, DBS_EXTENT_SIZE);
    extent_map->total_volume_extents = total_volume_extents;
    extent_map->allocated_volume_extents = 0;
    extent_map->max_extent_idx = 0;
    extent_map->extent_bitmap = (uint32_t *)calloc(DIV_ROUND_UP(total_volume_extents, 32), sizeof(uint32_t));;
    extent_map->extents = (dbs_extent_metadata *)calloc(total_volume_extents, sizeof(dbs_extent_metadata));;
    if (!extent_map->extent_bitmap || !extent_map->extents) {
        printf("ERROR: Cannot allocate extent info\n");
        goto fail_with_extent_map;
    }

    dbs_extent_metadata *device_extents = (dbs_extent_metadata *)malloc(sizeof(dbs_extent_metadata) * DBS_EXTENT_BATCH);
    if (!device_extents) {
        printf("ERROR: Cannot allocate extent buffer\n");
        goto fail_with_extent_map;
    }

    // Scan all extent metadata for the current snapshot id and place them in the extent map
    uint64_t extents_remaining = MIN(device_context->total_device_extents, device_context->superblock.allocated_device_extents);
    uint32_t batch_extent_start = 0;
    while (extents_remaining) {
        uint32_t batch_size = MIN(DBS_EXTENT_BATCH, extents_remaining);
        uint32_t batch_offset = device_context->extent_offset + (sizeof(dbs_extent_metadata) * batch_extent_start);
        if (pread(device_context->fd, &device_extents, batch_size * sizeof(dbs_extent_metadata), batch_offset) != (batch_size * sizeof(dbs_extent_metadata))) {
            printf("ERROR: Cannot read extents: %s\n", strerror(errno));
            free(device_extents);
            goto fail_with_extent_map;
        }

        for (uint32_t i = 0; i < batch_size; i++) {
            if (device_extents[i].snapshot_id == snapshot_id) {
                extent_map->allocated_volume_extents++;
                uint32_t extent_idx = device_extents[i].extent_pos;
                extent_map->max_extent_idx = MAX(extent_map->max_extent_idx, extent_idx);
                bitmap_set_bit(extent_map->extent_bitmap, extent_idx);
                memcpy(&(extent_map->extents[extent_idx]), &(device_extents[i]), sizeof(dbs_extent_metadata));
                // Convert extent_pos from position in volume to position in device
                extent_map->extents[extent_idx].extent_pos = batch_extent_start + i;
            }
        }
        extents_remaining -= batch_size;
        batch_extent_start += batch_size;
    };

    return extent_map;

fail_with_extent_map:
    if (extent_map->extent_bitmap)
        free(extent_map->extent_bitmap);
    if (extent_map->extents)
        free(extent_map->extents);
    free(extent_map);
    return NULL;
}

void free_extent_map(dbs_extent_map *extent_map) {
    free(extent_map->extent_bitmap);
    free(extent_map->extents);
    free(extent_map);
}

dbs_bool write_superblock(dbs_device_context *device_context) {
    if (pwrite(device_context->fd, &(device_context->superblock), sizeof(dbs_superblock), 0) != sizeof(dbs_superblock))
        return DBS_FAILURE;
    return DBS_SUCCESS;
}

dbs_bool write_device_metadata(dbs_device_context *device_context, dbs_device_metadata *device_metadata) {
    if (pwrite(device_context->fd, device_metadata, sizeof(dbs_device_metadata), 512) != sizeof(dbs_device_metadata))
        return DBS_FAILURE;
    return DBS_SUCCESS;
}

dbs_bool write_extent_metadata(dbs_device_context *device_context, dbs_extent_map *extent_map, uint32_t extent_idx) {
    dbs_extent_metadata extent_metadata;
    uint64_t extent_data_offset = device_context->extent_offset + (extent_map->extents[extent_idx].extent_pos * sizeof(dbs_extent_metadata));
    memcpy(&extent_metadata, &(extent_map->extents[extent_idx]), sizeof(dbs_extent_metadata));
    extent_metadata.extent_pos = extent_idx;
    if (pwrite(device_context->fd, &extent_metadata, sizeof(dbs_extent_metadata), extent_data_offset) != sizeof(dbs_extent_metadata))
        return DBS_FAILURE;
    return DBS_SUCCESS;
}

// Management functions

dbs_bool dbs_init_device(char *device) {
    dbs_device_context device_context;

    device_context.fd = open(device, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (device_context.fd < 0) {
        printf("ERROR: Cannot open %s: %s\n", device, strerror(errno));
        goto fail_with_device_context;
    }

    struct stat device_stat;
    if (fstat(device_context.fd, &device_stat) < 0) {
        printf("ERROR: Cannot get device size: %s\n", strerror(errno));
        goto fail_with_device_context;
    }

    memcpy(device_context.superblock.magic, dbs_magic, 8);
    device_context.superblock.version = dbs_version;
    device_context.superblock.allocated_device_extents = 0;
    device_context.superblock.device_size = device_stat.st_size;
    fill_device_attributes(&device_context);

    uint32_t empty_extents_size = sizeof(dbs_extent_metadata) * DBS_EXTENT_BATCH;
    dbs_extent_metadata *empty_extents = (dbs_extent_metadata *)calloc(DBS_EXTENT_BATCH, sizeof(dbs_extent_metadata));
    if (!empty_extents) {
        printf("ERROR: Cannot allocate extent buffer\n");
        goto fail_with_device_context;
    }
    for (uint32_t device_offset = 0; device_offset < device_context.metadata_size; device_offset += empty_extents_size) {
        if (pwrite(device_context.fd, empty_extents, empty_extents_size, device_offset) != empty_extents_size)
            goto fail_with_device_context;
    }
    if (pwrite(device_context.fd, &(device_context.superblock), sizeof(dbs_superblock), 0) != sizeof(dbs_superblock))
        goto fail_with_device_context;

    close(device_context.fd);
    return DBS_SUCCESS;

fail_with_device_context:
    close(device_context.fd);
    return DBS_FAILURE;
}

void dbs_vacuum_device(char *device) {
    printf("ERROR: Not implemented\n");
}

void dbs_list_volumes(char *device) {
    dbs_metadata_context metadata_context;
    dbs_volume_metadata *volumes = metadata_context.device_metadata.volumes;
    dbs_snapshot_metadata *snapshots = metadata_context.device_metadata.snapshots;
    if (!fill_metadata_context(&metadata_context, device))
        goto fail_with_device_context;

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
        uint16_t snapshot_count = 0;
        for ( ; snapshot_idx >= 0; snapshot_idx = snapshots[snapshot_idx].parent_snapshot_id - 1)
            snapshot_count++;
        printf(format_string, volumes[volume_idx].snapshot_id, snapshot_count, size_string, created_at_string, volumes[volume_idx].volume_name);
    }

fail_with_device_context:
    close(metadata_context.device_context.fd);
}

void dbs_list_snapshots(char *device, char *volume_name) {
    dbs_metadata_context metadata_context;
    dbs_volume_metadata *volumes = metadata_context.device_metadata.volumes;
    dbs_snapshot_metadata *snapshots = metadata_context.device_metadata.snapshots;
    if (!fill_metadata_context(&metadata_context, device))
        goto fail_with_device_context;

    char format_string[] = "%6s | %20s\n";
    char created_at_string[20];
    printf(format_string, "ID", "Created at");
    printf(format_string, "------", "--------------------");
    uint16_t volume_idx = find_volume_name(volumes, volume_name);
    if (!volume_idx) {
        printf("ERROR: Volume not found\n");
        goto fail_with_device_context;
    }
    for (uint16_t snapshot_idx = volumes[volume_idx].snapshot_id - 1; snapshot_idx >= 0; snapshot_idx = snapshots[snapshot_idx].parent_snapshot_id - 1) {
        strftime(created_at_string, 20, "%Y-%m-%d %H:%M:%S", localtime(&(snapshots[snapshot_idx].created_at)));
        printf(format_string, volumes[volume_idx].snapshot_id, created_at_string);
    }

fail_with_device_context:
    close(metadata_context.device_context.fd);
}

dbs_bool dbs_create_volume(char *device, char *volume_name, uint64_t volume_size) {
    dbs_metadata_context metadata_context;
    dbs_volume_metadata *volumes = metadata_context.device_metadata.volumes;
    if (!fill_metadata_context(&metadata_context, device))
        goto fail_with_device_context;

    // Find volume
    uint16_t volume_idx = find_volume_name(volumes, volume_name);
    if (volume_idx) {
        printf("ERROR: Volume %s already exists\n", volume_name);
        goto fail_with_device_context;
    }
    for (volume_idx = 0; volume_idx < DBS_MAX_VOLUMES || volumes[volume_idx].snapshot_id != 0; volume_idx++);
    if (volume_idx == DBS_MAX_VOLUMES) {
        printf("ERROR: Max volume count reached\n");
        goto fail_with_device_context;
    }

    // Create volume
    uint16_t snapshot_id = add_snapshot(&(metadata_context.device_metadata), 0);
    if (!snapshot_id) {
        printf("ERROR: Max snapshot count reached\n");
        goto fail_with_device_context;
    }
    volumes[volume_idx].snapshot_id = snapshot_id;
    volumes[volume_idx].volume_size = volume_size;
    strncpy(volumes[volume_idx].volume_name, volume_name, DBS_VOLUME_NAME_SIZE - 1);
    if (!write_device_metadata(&(metadata_context.device_context), &(metadata_context.device_metadata)))
        goto fail_with_device_context;

    close(metadata_context.device_context.fd);
    return DBS_SUCCESS;

fail_with_device_context:
    close(metadata_context.device_context.fd);
    return DBS_FAILURE;
}

dbs_bool dbs_create_snapshot(char *device, char *volume_name) {
    dbs_metadata_context metadata_context;
    dbs_volume_metadata *volumes = metadata_context.device_metadata.volumes;
    if (!fill_metadata_context(&metadata_context, device))
        goto fail_with_device_context;

    // Find volume
    uint16_t volume_idx = find_volume_name(volumes, volume_name);
    if (!volume_idx) {
        printf("ERROR: Volume not found\n");
        goto fail_with_device_context;
    }

    // Create snapshot
    uint16_t snapshot_id = add_snapshot(&(metadata_context.device_metadata), volumes[volume_idx].snapshot_id);
    if (!snapshot_id) {
        printf("ERROR: Max snapshot count reached\n");
        goto fail_with_device_context;
    }
    volumes[volume_idx].snapshot_id = snapshot_id;
    if (!write_device_metadata(&(metadata_context.device_context), &(metadata_context.device_metadata)))
        goto fail_with_device_context;

    close(metadata_context.device_context.fd);
    return DBS_SUCCESS;

fail_with_device_context:
    close(metadata_context.device_context.fd);
    return DBS_FAILURE;
}

dbs_bool dbs_clone_snapshot(char *device, char *volume_name, uint16_t snapshot_id) {
    printf("ERROR: Not implemented\n");
    return DBS_FAILURE;
}

dbs_bool dbs_delete_volume(char *device, char *volume_name) {
    dbs_metadata_context metadata_context;
    dbs_volume_metadata *volumes = metadata_context.device_metadata.volumes;
    dbs_snapshot_metadata *snapshots = metadata_context.device_metadata.snapshots;
    if (!fill_metadata_context(&metadata_context, device))
        goto fail_with_device_context;

    uint16_t volume_idx = find_volume_name(volumes, volume_name);
    if (!volume_idx) {
        printf("ERROR: Volume not found\n");
        goto fail_with_device_context;
    }
    for (uint16_t snapshot_idx = volumes[volume_idx].snapshot_id - 1; snapshot_idx >= 0; snapshot_idx = snapshots[snapshot_idx].parent_snapshot_id - 1) {
        dbs_extent_map *extent_map = get_extent_map(&(metadata_context.device_context), volumes[volume_idx].volume_size, snapshot_idx + 1);
        if (!extent_map) {
            goto fail_with_device_context;
        }
        for (uint32_t extent_idx = 0; extent_idx <= extent_map->max_extent_idx; ) {
            // If the region is empty, skip it completely
            if (bitmap_region_is_empty(extent_map->extent_bitmap, extent_idx)) {
                extent_idx += 32;
                continue;
            }
            for (int i = 0; i < 32; i++) {
                if (extent_map->extents[extent_idx].snapshot_id == 0)
                    continue;

                // Delete extent
                extent_map->extents[extent_idx].snapshot_id = 0;
                if (!write_extent_metadata(&(metadata_context.device_context), extent_map, extent_idx)) {
                    printf("ERROR: Failed writing metadata for extent %u: %s\n", extent_idx, strerror(errno));
                    free_extent_map(extent_map);
                    goto fail_with_device_context;
                }
                extent_idx++;
            }
        }
        free_extent_map(extent_map);
        snapshots[snapshot_idx].created_at = 0;
    }
    volumes[volume_idx].snapshot_id = 0;
    if (!write_device_metadata(&(metadata_context.device_context), &(metadata_context.device_metadata)))
        goto fail_with_device_context;

    close(metadata_context.device_context.fd);
    return DBS_SUCCESS;

fail_with_device_context:
    close(metadata_context.device_context.fd);
    return DBS_FAILURE;
}

dbs_bool dbs_delete_snapshot(char *device, uint16_t snapshot_id) {
    printf("ERROR: Not implemented\n");
    return DBS_FAILURE;
}

// Block API

dbs_context dbs_open(char *device, char *volume_name) {
    dbs_metadata_context metadata_context;
    dbs_volume_metadata *volumes = metadata_context.device_metadata.volumes;
    dbs_snapshot_metadata *snapshots = metadata_context.device_metadata.snapshots;
    if (!fill_metadata_context(&metadata_context, device))
        return NULL;

    // Find volume
    uint16_t volume_idx = find_volume_name(volumes, volume_name);
    if (!volume_idx) {
        printf("ERROR: Volume not found\n");
        goto fail_with_device_context;
    }

    // Prepare context
    dbs_volume_context *volume_context = (dbs_volume_context *)malloc(sizeof(dbs_volume_context));
    if (!volume_context) {
        printf("ERROR: Cannot allocate volume info\n");
        goto fail_with_device_context;
    }
    memcpy(&(volume_context->device_context), &(metadata_context.device_context), sizeof(dbs_device_context));
    memcpy(&(volume_context->volume), &(volumes[volume_idx]), sizeof(dbs_volume_metadata));
    volume_context->extent_map = get_extent_map(&(metadata_context.device_context), volumes[volume_idx].volume_size, volumes[volume_idx].snapshot_id);
    if (!volume_context->extent_map) {
        free(volume_context);
        goto fail_with_device_context;
    }

    // Populate extents
    uint16_t snapshot_idx = volumes[volume_idx].snapshot_id - 1;
    snapshot_idx = snapshots[snapshot_idx].parent_snapshot_id - 1;
    for ( ; snapshot_idx >= 0; snapshot_idx = snapshots[snapshot_idx].parent_snapshot_id - 1) {
        dbs_extent_map *extent_map = get_extent_map(&(metadata_context.device_context), volumes[volume_idx].volume_size, snapshot_idx + 1);
        if (!extent_map) {
            free(volume_context);
            goto fail_with_device_context;
        }
        for (uint32_t extent_idx = 0; extent_idx <= extent_map->max_extent_idx; ) {
            // If the region is empty, skip it completely
            if (bitmap_region_is_empty(extent_map->extent_bitmap, extent_idx)) {
                extent_idx += 32;
                continue;
            }
            for (int i = 0; i < 32; i++) {
                if (extent_map->extents[extent_idx].snapshot_id != 0 && volume_context->extent_map->extents[extent_idx].snapshot_id == 0) {
                    memcpy(&(volume_context->extent_map->extents[extent_idx]), &(extent_map->extents[extent_idx]), sizeof(dbs_extent_metadata));
                    volume_context->extent_map->allocated_volume_extents++;
                    volume_context->extent_map->max_extent_idx = MAX(volume_context->extent_map->max_extent_idx, extent_idx);
                    bitmap_set_bit(volume_context->extent_map->extent_bitmap, extent_idx);
                }
                extent_idx++;
            }
        }
        free_extent_map(extent_map);
    }

    return (dbs_context)volume_context;

fail_with_device_context:
    close(metadata_context.device_context.fd);
    return NULL;

fail_with_volume_context:
    close(volume_context->device_context.fd);
    free_extent_map(volume_context->extent_map);
    free(volume_context);
    return NULL;
}

void dbs_close(dbs_context context) {
    dbs_volume_context *volume_context = (dbs_volume_context *)context;

    close(volume_context->device_context.fd);
    free_extent_map(volume_context->extent_map);
    free(volume_context);
}

dbs_bool dbs_read(dbs_context context, uint64_t block, void *data) {
    dbs_volume_context *volume_context = (dbs_volume_context *)context;

    uint32_t extent_idx = block >> DBS_BLOCK_BITS_IN_EXTENT;
    if (extent_idx > volume_context->extent_map->total_volume_extents) {
        return DBS_FAILURE;
    }

    // Unallocated extent or block
    dbs_extent_metadata *extent_metadata = &(volume_context->extent_map->extents[extent_idx]);
    if (extent_metadata->snapshot_id == 0 || !bitmap_check_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT)) {
        memset(data, 0, 512);
        return DBS_SUCCESS;
    }

    // Read data from device
    uint64_t block_data_offset = volume_context->device_context.data_offset + (extent_metadata->extent_pos * DBS_EXTENT_SIZE) + ((block & DBS_BLOCK_MASK_IN_EXTENT) * 512);
    if (pread(volume_context->device_context.fd, data, 512, block_data_offset) != 512)
        return DBS_FAILURE;

    return DBS_SUCCESS;
}

dbs_bool dbs_write(dbs_context context, uint64_t block, void *data) {
    dbs_volume_context *volume_context = (dbs_volume_context *)context;

    uint32_t extent_idx = block >> DBS_BLOCK_BITS_IN_EXTENT;
    if (extent_idx > volume_context->extent_map->total_volume_extents) {
        return DBS_FAILURE;
    }

    // Unallocated or previous snapshot extent
    dbs_extent_metadata *extent_metadata = &(volume_context->extent_map->extents[extent_idx]);
    if (extent_metadata->snapshot_id == 0 || extent_metadata->snapshot_id != volume_context->volume.snapshot_id) {
        // Allocate new extent
        extent_metadata->snapshot_id = volume_context->volume.snapshot_id;
        extent_metadata->extent_pos = volume_context->device_context.superblock.allocated_device_extents;
        if (!write_extent_metadata(&(volume_context->device_context), volume_context->extent_map, extent_idx)) {
            printf("ERROR: Failed writing metadata for extent %u: %s\n", extent_idx, strerror(errno));
            return DBS_FAILURE;
        }

        // Update allocation count
        volume_context->device_context.superblock.allocated_device_extents++;
        if (!write_superblock(&(volume_context->device_context))) {
            printf("ERROR: Failed writing superblock: %s\n", strerror(errno));
            return DBS_FAILURE;
        }
    }

    // Write data to device
    uint64_t block_data_offset = volume_context->device_context.data_offset + (extent_metadata->extent_pos * DBS_EXTENT_SIZE) + ((block & DBS_BLOCK_MASK_IN_EXTENT) * 512);
    if (pwrite(volume_context->device_context.fd, data, 512, block_data_offset) != 512) {
        printf("ERROR: Failed writing at block %llu: %s\n", block, strerror(errno));
        return DBS_FAILURE;
    }

    // Update metadata
    if (bitmap_check_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT))
        return DBS_SUCCESS;

    bitmap_set_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT);
    if (!write_extent_metadata(&(volume_context->device_context), volume_context->extent_map, extent_idx)) {
        printf("ERROR: Failed writing metadata for extent %u: %s\n", extent_idx, strerror(errno));
        return DBS_FAILURE;
    }

    return DBS_SUCCESS;
}

dbs_bool dbs_unmap(dbs_context context, uint64_t block) {
    dbs_volume_context *volume_context = (dbs_volume_context *)context;

    uint32_t extent_idx = block >> DBS_BLOCK_BITS_IN_EXTENT;
    if (extent_idx > volume_context->extent_map->total_volume_extents) {
        return DBS_FAILURE;
    }

    // Unallocated extent or block
    dbs_extent_metadata *extent_metadata = &(volume_context->extent_map->extents[extent_idx]);
    if (extent_metadata->snapshot_id == 0 || !bitmap_check_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT)) {
        return DBS_SUCCESS;
    }

    // Update metadata
    bitmap_unset_bit(extent_metadata->block_bitmap, block & DBS_BLOCK_MASK_IN_EXTENT);
    if (bitmap_is_empty(extent_metadata->block_bitmap, DBS_EXTENT_BITMAP_SIZE) == 0) {
        // Release if not used
        extent_metadata->snapshot_id = 0;
    }
    if (!write_extent_metadata(&(volume_context->device_context), volume_context->extent_map, extent_idx)) {
        printf("ERROR: Failed writing metadata for extent %u: %s\n", extent_idx, strerror(errno));
        return DBS_FAILURE;
    }

    return DBS_SUCCESS;
}
