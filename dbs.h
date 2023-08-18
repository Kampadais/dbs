#ifndef _DBS_H_
#define _DBS_H_

typedef void* dbs_context;

dbs_context dbs_open(char *device, char *volume_name, uint64_t volume_size);
void dbs_close(dbs_context volume);

uint8_t dbs_read(dbs_context volume, uint64_t block, void *data);
uint8_t dbs_write(dbs_context volume, uint64_t block, void *data);
uint8_t dbs_unmap(dbs_context volume, uint64_t block);

#endif
