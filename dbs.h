#include <time.h>

#ifndef _DBS_H_
#define _DBS_H_

#define DBS_SUCCESS 1
#define DBS_FAILURE 0

#define DBS_MAX_VOLUMES 256         // Named volumes
#define DBS_MAX_SNAPSHOTS 65536     // Total snapshots
#define DBS_MAX_VOLUME_NAME_SIZE 255

typedef uint8_t dbs_bool;

typedef struct {
    uint32_t version;
    uint64_t device_size;
    uint32_t total_device_extents;
    uint32_t allocated_device_extents;
    uint8_t volume_count;
} dbs_device_info;

typedef struct {
    char volume_name[DBS_MAX_VOLUME_NAME_SIZE + 1];
    uint64_t volume_size;
    time_t created_at;
    uint16_t snapshot_id;
    uint16_t snapshot_count;
} dbs_volume_info;

typedef struct {
    uint16_t snapshot_id;
    uint16_t parent_snapshot_id;
    time_t created_at;
} dbs_snapshot_info;

// Query API

dbs_bool dbs_fill_device_info(char *device, dbs_device_info *device_info);
dbs_bool dbs_fill_volume_info(char *device, dbs_volume_info *volume_info, uint8_t *count);
dbs_bool dbs_fill_snapshot_info(char *device, char *volume_name, dbs_snapshot_info *snapshot_info, uint16_t *count);

// Management API

dbs_bool dbs_init_device(char *device);
dbs_bool dbs_vacuum_device(char *device);

dbs_bool dbs_create_volume(char *device, char *volume_name, uint64_t volume_size);
dbs_bool dbs_rename_volume(char *device, char *volume_name, char *new_volume_name);
dbs_bool dbs_create_snapshot(char *device, char *volume_name);
dbs_bool dbs_clone_snapshot(char *device, char *new_volume_name, uint16_t snapshot_id);

dbs_bool dbs_delete_volume(char *device, char *volume_name);
dbs_bool dbs_delete_snapshot(char *device, uint16_t snapshot_id);

// Block API

typedef void *dbs_context;

dbs_context dbs_open_volume(char *device, char *volume_name);
void dbs_close_volume(dbs_context context);

dbs_bool dbs_read_block(dbs_context context, uint64_t block, void *data);
dbs_bool dbs_write_block(dbs_context context, uint64_t block, void *data);
dbs_bool dbs_unmap_block(dbs_context context, uint64_t block);

#endif
