#!/usr/bin/env python

import os

import pydbs as dbs

MEGABYTE = 1024 * 1024
GIGABYTE = MEGABYTE * 1024

EMPTY_BLOCK = b'\x00' * 512
DUMMY_BLOCK = b'\xf0' * 512

DEVICE = 'test.img'
DEVICE_SIZE = MEGABYTE * 100

def prepare():
    if os.path.exists(DEVICE):
        if not os.path.isfile(DEVICE):
            print('ERROR: %s exists and is not a file' % DEVICE)
            os.exit(-1)
        if os.path.getsize(DEVICE) == DEVICE_SIZE:
            return
    with open(DEVICE, 'wb') as f:
        f.truncate(DEVICE_SIZE)

def test_device():
    # Init a device
    assert dbs.init_device(DEVICE)
    device_info = dbs.get_device_info(DEVICE)
    assert device_info.allocated_device_extents == 0
    assert device_info.volume_count == 0
    volume_info = dbs.get_volume_info(DEVICE)
    assert volume_info == []

def assert_volume(volume, volume_name, volume_size, snapshot_count):
    assert volume.volume_name == volume_name
    assert volume.volume_size == volume_size
    assert volume.created_at > 0
    assert volume.snapshot_count == snapshot_count

def test_volume():
    # Create a volume
    assert dbs.create_volume(DEVICE, 'vol1', GIGABYTE)
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 1
    assert_volume(volume_info[0], 'vol1', GIGABYTE, 1)

    # Create multiple volumes
    assert(dbs.create_volume(DEVICE, 'vol1', GIGABYTE) == False)
    assert(dbs.create_volume(DEVICE, 'vol2', 2 * GIGABYTE))
    assert(dbs.create_volume(DEVICE, 'vol3', 3 * GIGABYTE))
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 3
    assert_volume(volume_info[0], 'vol1', GIGABYTE, 1)
    assert_volume(volume_info[1], 'vol2', 2 * GIGABYTE, 1)
    assert_volume(volume_info[2], 'vol3', 3 * GIGABYTE, 1)

    # Delete a volume
    assert(dbs.delete_volume(DEVICE, 'vol2'))
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 2
    assert_volume(volume_info[0], 'vol1', GIGABYTE, 1)
    assert_volume(volume_info[1], 'vol3', 3 * GIGABYTE, 1)
    assert(dbs.delete_volume(DEVICE, 'vol2') == False)

    # Create volume again (goes in empty spot)
    assert(dbs.create_volume(DEVICE, 'vol2new', 2 * GIGABYTE))
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 3
    assert_volume(volume_info[0], 'vol1', GIGABYTE, 1)
    assert_volume(volume_info[1], 'vol2new', 2 * GIGABYTE, 1)
    assert_volume(volume_info[2], 'vol3', 3 * GIGABYTE, 1)

    # Rename volume
    assert(dbs.rename_volume(DEVICE, 'vol2new', 'vol2renamed'))
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 3
    assert_volume(volume_info[0], 'vol1', GIGABYTE, 1)
    assert_volume(volume_info[1], 'vol2renamed', 2 * GIGABYTE, 1)
    assert_volume(volume_info[2], 'vol3', 3 * GIGABYTE, 1)

    # Delete multiple volumes
    assert(dbs.delete_volume(DEVICE, 'vol2renamed'))
    assert(dbs.delete_volume(DEVICE, 'vol3'))
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 1
    assert_volume(volume_info[0], 'vol1', GIGABYTE, 1)
    assert(dbs.delete_volume(DEVICE, 'vol1'))
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 0

def test_snapshot():
    # Create a volume
    assert dbs.create_volume(DEVICE, 'vol1', GIGABYTE)
    volume_info = dbs.get_volume_info(DEVICE)
    volume_snapshot_id = next((v for v in volume_info if v.volume_name == 'vol1'), None).snapshot_id
    snapshot_info = dbs.get_snapshot_info(DEVICE, 'vol1')
    assert len(snapshot_info) == 1
    assert snapshot_info[0].snapshot_id > 0
    assert snapshot_info[0].parent_snapshot_id == None
    initial_snapshot_id = snapshot_info[0].snapshot_id
    assert volume_snapshot_id == initial_snapshot_id

    # Create a snapshot
    assert dbs.create_snapshot(DEVICE, 'vol1')
    volume_info = dbs.get_volume_info(DEVICE)
    volume_snapshot_id = next((v for v in volume_info if v.volume_name == 'vol1'), None).snapshot_id
    assert volume_snapshot_id != initial_snapshot_id
    snapshot_info = dbs.get_snapshot_info(DEVICE, 'vol1')
    assert len(snapshot_info) == 2
    initial_snapshot = next((s for s in snapshot_info if s.parent_snapshot_id == None), None)
    assert initial_snapshot != None
    assert initial_snapshot.snapshot_id == initial_snapshot_id
    current_snapshot = next((s for s in snapshot_info if s.parent_snapshot_id != None), None)
    assert current_snapshot != None
    assert current_snapshot.snapshot_id == volume_snapshot_id
    assert current_snapshot.parent_snapshot_id == initial_snapshot_id

    # Create multiple snapshots
    assert dbs.create_snapshot(DEVICE, 'vol1')
    assert dbs.create_snapshot(DEVICE, 'vol1')
    assert dbs.create_snapshot(DEVICE, 'vol1')
    volume_info = dbs.get_volume_info(DEVICE)
    volume_snapshot_id = next((v for v in volume_info if v.volume_name == 'vol1'), None).snapshot_id
    assert volume_snapshot_id != initial_snapshot_id
    snapshot_info = dbs.get_snapshot_info(DEVICE, 'vol1')
    assert len(snapshot_info) == 5
    initial_snapshot = next((s for s in snapshot_info if s.parent_snapshot_id == None), None)
    assert initial_snapshot != None
    assert initial_snapshot.snapshot_id == initial_snapshot_id
    current_snapshot = next((s for s in snapshot_info if s.parent_snapshot_id != volume_snapshot_id), None)
    assert current_snapshot != None
    assert current_snapshot.snapshot_id == volume_snapshot_id
    assert current_snapshot.parent_snapshot_id != initial_snapshot_id

    # Delete a snapshot
    assert dbs.delete_snapshot(DEVICE, current_snapshot.snapshot_id) == False
    snapshot_info = dbs.get_snapshot_info(DEVICE, 'vol1')
    assert len(snapshot_info) == 5
    assert dbs.delete_snapshot(DEVICE, initial_snapshot.snapshot_id)
    snapshot_info = dbs.get_snapshot_info(DEVICE, 'vol1')
    assert len(snapshot_info) == 4

    # Create snapshot again
    assert dbs.create_snapshot(DEVICE, 'vol1')
    volume_info = dbs.get_volume_info(DEVICE)
    volume_snapshot_id = next((v for v in volume_info if v.volume_name == 'vol1'), None).snapshot_id
    assert volume_snapshot_id != current_snapshot.snapshot_id
    snapshot_info = dbs.get_snapshot_info(DEVICE, 'vol1')
    assert len(snapshot_info) == 5

    # Delete multiple snapshots
    for snapshot in snapshot_info:
        if snapshot.snapshot_id == volume_snapshot_id:
            continue
        assert dbs.delete_snapshot(DEVICE, snapshot.snapshot_id)
    snapshot_info = dbs.get_snapshot_info(DEVICE, 'vol1')
    assert len(snapshot_info) == 1
    assert snapshot_info[0].snapshot_id == volume_snapshot_id
    assert snapshot_info[0].parent_snapshot_id == None

    # Clone latest snapshot
    dbs.clone_snapshot(DEVICE, 'vol2cloned', volume_snapshot_id)
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 2
    assert_volume(volume_info[0], 'vol1', GIGABYTE, 1)
    assert_volume(volume_info[1], 'vol2cloned', GIGABYTE, 1)
    assert(dbs.delete_volume(DEVICE, 'vol2cloned'))
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 1

    # Snapshot and clone both the previous snapshot and latest snapshot
    assert dbs.create_snapshot(DEVICE, 'vol1')
    snapshot_info = dbs.get_snapshot_info(DEVICE, 'vol1')
    for i, snapshot in enumerate(snapshot_info):
        dbs.clone_snapshot(DEVICE, 'vol2clone%d' % (i + 1), snapshot.snapshot_id)
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 3
    assert_volume(volume_info[0], 'vol1', GIGABYTE, 2)
    assert_volume(volume_info[1], 'vol2clone1', GIGABYTE, 1)
    assert_volume(volume_info[2], 'vol2clone2', GIGABYTE, 1)

    # Clean up
    assert(dbs.delete_volume(DEVICE, 'vol1'))
    assert(dbs.delete_volume(DEVICE, 'vol2clone1'))
    assert(dbs.delete_volume(DEVICE, 'vol2clone2'))
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 0

def load_blocks():
    with open(__file__, 'rb') as f:
        data = f.read()
    block_count = int(len(data) / 512)
    block_data = {}
    for i in range(block_count):
        block_data[i] = data[i * 512:(i + 1) * 512]
    return block_data

def read_blocks(context, block_indices, block_data):
    block_count = len(block_data)
    for i, block_index in enumerate(block_indices):
        assert dbs.read_block(context, block_index) == block_data[i % block_count]

def write_blocks(context, block_indices, block_data):
    block_count = len(block_data)
    for i, block_index in enumerate(block_indices):
        assert dbs.write_block(context, block_index, block_data[i % block_count])

def unmap_blocks(context, block_indices):
    for block_index in block_indices:
        assert dbs.unmap_block(context, block_index)

def test_volume_io():
    repeats = 10
    spread = 100
    positions = [0, 3, 43, 53, 92]

    block_data = load_blocks()
    block_indices = []
    i = 0
    for r in range(repeats):
        for p in positions:
            block_indices.append(p + (r * spread))
            i += 1

    # Create a volume and open it
    assert dbs.create_volume(DEVICE, 'vol1', GIGABYTE)
    context = dbs.open_volume(DEVICE, 'vol1')

    # Read (should get empty data)
    read_blocks(context, block_indices, [EMPTY_BLOCK])

    # Write and read back
    write_blocks(context, block_indices, block_data)
    read_blocks(context, block_indices, block_data)

    # Read other (should get empty data)
    other_block_indices = sorted([(i - 1) for i in block_indices if i > 0] + [(i + 1) for i in block_indices])
    read_blocks(context, other_block_indices, [EMPTY_BLOCK])

    # Unmap and read back
    unmap_blocks(context, block_indices)
    read_blocks(context, block_indices, [EMPTY_BLOCK])

    # Validate metadata and clean up
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 1
    assert_volume(volume_info[0], 'vol1', GIGABYTE, 1)
    assert(dbs.delete_volume(DEVICE, 'vol1'))

def test_snapshot_io():
    repeats = 10
    spread = 100
    positions = [0, 3, 43, 53, 92]

    block_data = load_blocks()
    block_indices = []
    i = 0
    for r in range(repeats):
        for p in positions:
            block_indices.append(p + (r * spread))
            i += 1

    # Create a volume and open it
    assert dbs.create_volume(DEVICE, 'vol1', GIGABYTE)
    context = dbs.open_volume(DEVICE, 'vol1')

    # Write
    write_blocks(context, block_indices, block_data)

    # Snapshot, open again and read back
    assert dbs.create_snapshot(DEVICE, 'vol1')
    context = dbs.open_volume(DEVICE, 'vol1')
    read_blocks(context, block_indices, block_data)

    # Overwrite and read back
    write_blocks(context, block_indices, [DUMMY_BLOCK])
    read_blocks(context, block_indices, [DUMMY_BLOCK])

    # Clone volume and open
    snapshot_info = dbs.get_snapshot_info(DEVICE, 'vol1')
    initial_snapshot_id = next((s for s in snapshot_info if s.parent_snapshot_id == None), None).snapshot_id
    assert dbs.clone_snapshot(DEVICE, 'vol1clone', initial_snapshot_id)
    context = dbs.open_volume(DEVICE, 'vol1clone')

    # Read original blocks from clone
    read_blocks(context, block_indices, block_data)

    # Delete initial snapshot, open again and read back
    assert dbs.delete_snapshot(DEVICE, initial_snapshot_id)
    context = dbs.open_volume(DEVICE, 'vol1')
    read_blocks(context, block_indices, [DUMMY_BLOCK])

    # Validate metadata and clean up
    volume_info = dbs.get_volume_info(DEVICE)
    assert len(volume_info) == 2
    assert_volume(volume_info[0], 'vol1', GIGABYTE, 1)
    assert_volume(volume_info[1], 'vol1clone', GIGABYTE, 1)
    assert(dbs.delete_volume(DEVICE, 'vol1'))
    assert(dbs.delete_volume(DEVICE, 'vol1clone'))

if __name__ == '__main__':
    prepare()
    test_device()
    test_volume()
    test_snapshot()
    test_volume_io()
    test_snapshot_io()
    print('PASS')
