#!/usr/bin/env python

import sys
import argparse

import pydbs as dbs

from datetime import datetime, timezone
from math import log

def human_readable_size(size):
    if size > 0:
        unit_list = ['bytes', 'kB', 'MB', 'GB', 'TB', 'PB']
        decimals = [0, 0, 1, 2, 2, 2]
        exponent = int(log(size, 1024))
        quotient = size / (1024 ** exponent)
        return f'{quotient:.{decimals[exponent]}f}{unit_list[exponent]}'
    return '0 bytes'

def present_info_vertically(info, keys, format_info):
    max_len = 0
    for key in keys:
        max_len = max(max_len, len(key))
    for key in keys:
        print(f'{key:>{max_len}}: {format_info(key, getattr(info, key))}')

def present_info_list_horizontally(info_list, keys, format_info):
    max_len = {}
    for key in keys:
        max_len[key] = len(key)
        for info in info_list:
            max_len[key] = max(max_len[key], len(str(format_info(key, getattr(info, key)))))
    # Header
    row = []
    for key in keys:
        row.append(f'{key:>{max_len[key]}}')
    print(' | '.join(row))
    # Separator
    row = []
    for key in keys:
        row.append('-' * max_len[key])
    print('-|-'.join(row))
    # Info rows
    for info in info_list:
        row = []
        for key in keys:
            row.append(f'{format_info(key, getattr(info, key)):>{max_len[key]}}')
        print(' | '.join(row))

def cmd_get_device_info(args):
    def format_device(key, value):
        if key == 'device_size':
            return f'{value} ({human_readable_size(value)})'
        return value

    device_info = dbs.get_device_info(args.device)
    keys = ('version', 'device_size', 'total_device_extents', 'allocated_device_extents', 'volume_count')
    present_info_vertically(device_info, keys, format_device)

def cmd_get_volume_info(args):
    def format_volume(key, value):
        if key == 'volume_size':
            return f'{value} ({human_readable_size(value)})'
        if key == 'created_at':
            return datetime.fromtimestamp(value, tz=timezone.utc).astimezone().isoformat()
        return value

    volume_info = dbs.get_volume_info(args.device)
    keys = ('volume_name', 'volume_size', 'created_at', 'snapshot_id', 'snapshot_count')
    present_info_list_horizontally(volume_info, keys, format_volume)

def cmd_get_snapshot_info(args):
    def format_snapshot(key, value):
        if key == 'created_at':
            return datetime.fromtimestamp(value, tz=timezone.utc).astimezone().isoformat()
        if key == 'parent_snapshot_id':
            if value is None:
                return '-'
        return value

    snapshot_info = dbs.get_snapshot_info(args.device, args.volume_name)
    keys = ('snapshot_id', 'parent_snapshot_id', 'created_at')
    present_info_list_horizontally(snapshot_info, keys, format_snapshot)

def cmd_init_device(args):
    dbs.init_device(args.device)

def cmd_vacuum_device(args):
    dbs.vacuum_device(args.device)

def cmd_create_volume(args):
    dbs.create_volume(args.device, args.volume_name, args.volume_size)

def cmd_rename_volume(args):
    dbs.rename_volume(args.device, args.volume_name, args.new_volume_name)

def cmd_create_snapshot(args):
    dbs.create_snapshot(args.device, args.volume_name)

def cmd_clone_snapshot(args):
    dbs.clone_snapshot(args.device, args.new_volume_name, args.snapshot_id)

def cmd_delete_volume(args):
    dbs.delete_volume(args.device, args.volume_name)

def cmd_delete_snapshot(args):
    dbs.delete_snapshot(args.device, args.snapshot_id)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DBS command line tool')
    parser.add_argument('device')
    subprasers = parser.add_subparsers(dest='command', title='commands')

    subpraser = subprasers.add_parser('get_device_info')
    subpraser.set_defaults(func=cmd_get_device_info)

    subpraser = subprasers.add_parser('get_volume_info')
    subpraser.set_defaults(func=cmd_get_volume_info)

    subpraser = subprasers.add_parser('get_snapshot_info')
    subpraser.add_argument('volume_name')
    subpraser.set_defaults(func=cmd_get_snapshot_info)

    subpraser = subprasers.add_parser('init_device')
    subpraser.set_defaults(func=cmd_init_device)

    subpraser = subprasers.add_parser('vacuum_device')
    subpraser.set_defaults(func=cmd_vacuum_device)

    subpraser = subprasers.add_parser('create_volume')
    subpraser.add_argument('volume_name')
    subpraser.add_argument('volume_size', type=int)
    subpraser.set_defaults(func=cmd_create_volume)

    subpraser = subprasers.add_parser('rename_volume')
    subpraser.add_argument('volume_name')
    subpraser.add_argument('new_volume_name')
    subpraser.set_defaults(func=cmd_rename_volume)

    subpraser = subprasers.add_parser('create_snapshot')
    subpraser.add_argument('volume_name')
    subpraser.set_defaults(func=cmd_create_snapshot)

    subpraser = subprasers.add_parser('clone_snapshot')
    subpraser.add_argument('new_volume_name')
    subpraser.add_argument('snapshot_id', type=int)
    subpraser.set_defaults(func=cmd_clone_snapshot)

    subpraser = subprasers.add_parser('delete_volume')
    subpraser.add_argument('volume_name')
    subpraser.set_defaults(func=cmd_delete_volume)

    subpraser = subprasers.add_parser('delete_snapshot')
    subpraser.add_argument('snapshot_id', type=int)
    subpraser.set_defaults(func=cmd_delete_snapshot)

    args = parser.parse_args()
    if args.command:
        args.func(args)
    else:
        parser.print_help(sys.stderr)
        sys.exit(1)
