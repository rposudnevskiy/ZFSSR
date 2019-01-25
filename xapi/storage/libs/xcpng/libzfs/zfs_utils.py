#!/usr/bin/env python

import re

from subprocess import call, Popen, PIPE, check_output

from xapi.storage import log

VOLBLOCKSIZE=8192
VOLMODE='dev'

def pool_create(dbg, pool_name, vdev):
    log.debug("%s: zfs_utils.pool_create: pool_name: %s device: %s" % (dbg, pool_name, vdev))
    call(['zpool', 'create', '-m', 'legacy', pool_name, vdev])

def pool_destroy(dbg, pool_name):
    log.debug("%s: zfs_utils.pool_destroy: pool_name: %s" % (dbg, pool_name))
    call(['zpool', 'destroy', pool_name])

def pool_import(dbg, pool_name, mountpoint=None):
    log.debug("%s: zfs_utils.pool_import: pool_name: %s mountpoint: %s" % (dbg, pool_name, mountpoint))
    cmd = ['zpool', 'import', pool_name]
    call(cmd)
    cmd = ['zfs', 'set']
    if mountpoint is not None:
        cmd.extend(["mountpoint=%s" % mountpoint])
    else:
        cmd.extend(['mountpoint=legacy'])
    cmd.extend([pool_name])
    call(cmd)
    log.debug("%s: zfs_utils.pool_import: cmd %s" % (dbg, cmd))

def pool_export(dbg, pool_name):
    log.debug("%s: zfs_utils.pool_export: pool_name: %s" % (dbg, pool_name))
    call(['zfs', 'set', 'mountpoint=legacy', pool_name])
    call(['zpool', 'export', pool_name])

def pool_set(dbg, pool_name, property, value):
    log.debug("%s: zfs_utils.pool_set: pool_name: %s property: $s value: %s" % (dbg, pool_name, value))
    call(['zpool', 'set', '%s=%s' % (property, value), pool_name])

def pool_get(dbg, pool_name, property):
    retval = check_output(['zpool', 'get', property, '-Hp', pool_name])
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
        #proc = Popen(['zpool', 'import', '-d', '/root'], stdout=PIPE)  # for pool in file
        proc = Popen(['zpool', 'import'], stdout=PIPE)  # for pool on disk device
        for line in iter(proc.stdout.readline,''):
            result = regex.match(line)
            if result:
                pools.append(result.group(1))
        return pools
    else:
        return pools

def zfs_mount(dbg, fs_name):
    log.debug("%s: zfs_utils.zfs_mount: fs_name: %s" % (dbg, fs_name))
    call(['zfs', 'mount', fs_name])

def zfs_umount(dbg, fs_name):
    log.debug("%s: zfs_utils.zfs_umount: fs_name: %s" % (dbg, fs_name))
    call(['zfs', 'umount', fs_name])

def zvol_create(dbg, image_name, vsize, volmode=VOLMODE, volblocksize=VOLBLOCKSIZE):
    log.debug("%s: zfs_utils.zvol_create: image_name %s vsize: %s volmode: %s volblocksize=%s"
              % (dbg, image_name, vsize, volmode, volblocksize))
    call(['zfs', 'create', '-o', "volmode=%s" % volmode, '-o', "volblocksize=%s" % volblocksize, '-V', str(vsize),
          image_name])

def zvol_destroy(dbg, image_name):
    log.debug("%s: zfs_utils.zvol_destroy: image_name %s" % (dbg, image_name))
    call(['zfs', 'destroy', image_name])

def zvol_set(dbg, image_name, property, value):
    log.debug("%s: zfs_utils.zvol_set: image_name: %s property: %s value: %s" % (dbg, image_name, property, value))
    call(['zfs', 'set', '%s=%s' % (property, value), image_name])

def zvol_rename(dbg, image_name, new_image_name):
    log.debug("%s: zfs_utils.zvol_rename: image_name: %s new_image_name: %s" % (dbg, image_name, new_image_name))
    call(['zfs', 'rename', image_name, new_image_name])

def zvol_get(dbg, image_name, property):
    retval = check_output(['zfs', 'get', property, '-Hp', image_name])
    regex = re.compile('(.*)\s+(\w+)\s+(\d+)\s+(.*)')
    result = regex.match(retval)
    value = result.group(3)
    log.debug("%s: zfs_utils.zvol_get: image_name: %s property: %s value: %s" % (dbg, image_name, property, value))
    return value

def zvol_list(dbg, pool_name):
    log.debug("%s: zfs_utils.zvol_list: pool_name: %s " % (dbg, pool_name))
    zvols = []
    regex = re.compile('.*/(\w+-.*)\s+(\d+)\s+(\d+)\s+(\d+)\s+-')
    proc = Popen(['zfs', 'list', '-Hpr', pool_name], stdout=PIPE)  # for pool on disk device
    for line in iter(proc.stdout.readline, ''):
        result = regex.match(line)
        if result:
            zvols.append(result.group(1))
    return zvols
