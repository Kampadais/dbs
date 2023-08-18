#ifndef _DBS_H_
#define _DBS_H_

#define DBS_SUCCESS 1
#define DBS_FAILURE 1

typedef uint8_t dbs_bool;

// Management functions

dbs_bool dbs_init_device(char *device);
void dbs_vacuum_device(char *device);

void dbs_list_volumes(char *device);
void dbs_list_snapshots(char *device, char *volume_name);

dbs_bool dbs_create_volume(char *device, char *volume_name, uint64_t volume_size);
void dbs_snapshot_volume(char *device, char *volume_name);
void dbs_delete_volume(char *device, char *volume_name);

void dbs_clone_snapshot(char *device, char *volume_name, uint16_t snapshot_id);

// Block API

typedef void* dbs_context;

dbs_context dbs_open(char *device, char *volume_name);
void dbs_close(dbs_context volume);

dbs_bool dbs_read(dbs_context volume, uint64_t block, void *data);
dbs_bool dbs_write(dbs_context volume, uint64_t block, void *data);
dbs_bool dbs_unmap(dbs_context volume, uint64_t block);

#endif
