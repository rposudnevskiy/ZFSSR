#!/usr/bin/env python

import re
from xapi.storage.libs.xcpng.utils import call
from xapi.storage import log

VOLBLOCKSIZE=8192
VOLMODE='dev'

def pool_create(dbg, pool_name, vdevs, mountpoint=None):
    log.debug("%s: zfs_utils.pool_create: pool_name: %s vdevs: %s mountpoint: %s" % (dbg, pool_name, vdevs, mountpoint))
    if mountpoint is not None:
        call(dbg, ['zpool', 'create', '-m', mountpoint, pool_name, vdevs])
    else:
        call(dbg, ['zpool', 'create', '-m', 'legacy', pool_name, vdevs])

def pool_destroy(dbg, pool_name):
    log.debug("%s: zfs_utils.pool_destroy: pool_name: %s" % (dbg, pool_name))
    call(dbg, ['zpool', 'destroy', pool_name])

def pool_import(dbg, pool_name, mountpoint=None):
    log.debug("%s: zfs_utils.pool_import: pool_name: %s mountpoint: %s" % (dbg, pool_name, mountpoint))
    cmd = ['zpool', 'import', pool_name]
    call(dbg, cmd)
    cmd = ['zfs', 'set']
    if mountpoint is not None:
        cmd.extend(["mountpoint=%s" % mountpoint])
    else:
        cmd.extend(['mountpoint=legacy'])
    cmd.extend([pool_name])
    call(dbg, cmd)

def pool_export(dbg, pool_name):
    log.debug("%s: zfs_utils.pool_export: pool_name: %s" % (dbg, pool_name))
    call(dbg, ['zfs', 'set', 'mountpoint=legacy', pool_name])
    call(dbg, ['zpool', 'export', pool_name])

def pool_set(dbg, pool_name, property, value):
    log.debug("%s: zfs_utils.pool_set: pool_name: %s property: $s value: %s" % (dbg, pool_name, value))
    call(dbg, ['zpool', 'set', '%s=%s' % (property, value), pool_name])

def pool_get(dbg, pool_name, property):
    retval = call(dbg, ['zpool', 'get', property, '-Hp', pool_name])
    regex = re.compile('(.*)\s+(\w+)\s+(\d+)\s+(.*)')
    result = regex.match(retval)
    value = result.group(3)
    log.debug("%s: zfs_utils.pool_get: pool_name: %s property: %s value: %s" % (dbg, pool_name, property, value))
    return value

def pool_list(dbg, imported=False):
    log.debug("%s: zfs_utils.pool_list: imported: %s" % (dbg, imported))
    pools = []
    if imported is False:
        regex = re.compile('\s+pool:\s+(.*)')
        for line in call(dbg, ['zpool', 'import']).split('\n'):
            result = regex.match(line)
            if result:
                pools.append(result.group(1))
        return pools
    else:
        return pools

def zfs_mount(dbg, fs_name):
    log.debug("%s: zfs_utils.zfs_mount: fs_name: %s" % (dbg, fs_name))
    call(dbg, ['zfs', 'mount', fs_name])

def zfs_umount(dbg, fs_name):
    log.debug("%s: zfs_utils.zfs_umount: fs_name: %s" % (dbg, fs_name))
    call(dbg, ['zfs', 'umount', fs_name])

def zvol_create(dbg, image_name, vsize, volmode=VOLMODE, volblocksize=VOLBLOCKSIZE):
    log.debug("%s: zfs_utils.zvol_create: image_name %s vsize: %s volmode: %s volblocksize=%s"
              % (dbg, image_name, vsize, volmode, volblocksize))
    call(dbg, ['zfs', 'create', '-o', "volmode=%s" % volmode, '-o', "volblocksize=%s" % volblocksize, '-V', str(vsize),
          image_name])

def zvol_destroy(dbg, image_name):
    log.debug("%s: zfs_utils.zvol_destroy: image_name %s" % (dbg, image_name))
    call(dbg, ['zfs', 'destroy', image_name])

def zvol_set(dbg, image_name, property, value):
    log.debug("%s: zfs_utils.zvol_set: image_name: %s property: %s value: %s" % (dbg, image_name, property, value))
    call(dbg, ['zfs', 'set', '%s=%s' % (property, value), image_name])

def zvol_rename(dbg, image_name, new_image_name):
    log.debug("%s: zfs_utils.zvol_rename: image_name: %s new_image_name: %s" % (dbg, image_name, new_image_name))
    call(dbg, ['zfs', 'rename', image_name, new_image_name])

def zvol_get(dbg, image_name, property):
    retval = call(dbg, ['zfs', 'get', property, '-Hp', image_name])
    regex = re.compile('(.*)\s+(\w+)\s+(\d+)\s+(.*)')
    result = regex.match(retval)
    value = result.group(3)
    log.debug("%s: zfs_utils.zvol_get: image_name: %s property: %s value: %s" % (dbg, image_name, property, value))
    return value

def zvol_list(dbg, pool_name):
    log.debug("%s: zfs_utils.zvol_list: pool_name: %s " % (dbg, pool_name))
    zvols = []
    regex = re.compile('.*/(\w+-.*)\s+(\d+)\s+(\d+)\s+(\d+)\s+-')
    for line in call(dbg, ['zfs', 'list', '-Hpr', pool_name]).split('\n'):
        result = regex.match(line)
        if result:
            zvols.append(result.group(1))
    return zvols
