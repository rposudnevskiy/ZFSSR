#!/usr/bin/env python

from xapi.storage.libs.xcpng.sr import SROperations, SR, Implementation
from xapi.storage.libs.xcpng.libzfs.zfs_utils import pool_list, pool_import, pool_export, pool_create, pool_destroy, \
                                                     zvol_list, pool_get
from xapi.storage.libs.xcpng.utils import POOL_PREFIX, SR_PATH_PREFIX, VDI_PREFIXES, get_pool_name_by_uri, \
                                          get_sr_uuid_by_uri, get_vdi_type_by_uri
from xapi.storage.libs.xcpng.libzfs.meta import ZFSMetadataHandler

class ZFSSROperations(SROperations):

    def __init__(self):
        super(ZFSSROperations, self).__init__()
        self.DEFAULT_SR_NAME = '<ZFS Zvol SR>'
        self.DEFAULT_SR_DESCRIPTION = '<ZFS Zvol SR>'

    def create(self, dbg, uri, configuration):
        pool_create(dbg,
                    get_pool_name_by_uri(dbg, uri),
                    configuration['vdev'],
                    mountpoint="%s/%s" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri)))

    def destroy(self, dbg, uri):
        self.sr_import(dbg, uri, {})
        pool_destroy(dbg, get_pool_name_by_uri(dbg, uri))

    def get_sr_list(self, dbg, configuration):
        zfs_pools = []
        for zfs_pool in pool_list(dbg):
            if zfs_pool.startswith("%s%s" % ('ZFS', POOL_PREFIX)):
                zfs_pools.append(zfs_pool)
        return zfs_pools

    def get_vdi_list(self, dbg, uri):
        zvols = []
        for zvol in zvol_list(dbg, get_pool_name_by_uri(dbg, uri)):
            if zvol.startswith(VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)]):
                zvols.append(zvol)
        return zvols

    def sr_import(self, dbg, uri, configuration):
        pool_import(dbg, get_pool_name_by_uri(dbg, uri))

    def sr_export(self, dbg, uri):
        pool_export(dbg, get_pool_name_by_uri(dbg, uri))

    def get_free_space(self, dbg, uri):
        return int(pool_get(dbg, get_pool_name_by_uri(dbg, uri), 'free'))

    def get_size(self, dbg, uri):
        return int(pool_get(dbg, get_pool_name_by_uri(dbg, uri), 'size'))

class ZFSSR(SR):

    def __init__(self):
        super(ZFSSR, self).__init__()
        self.sr_type = 'zfs'
        self.MetadataHandler = ZFSMetadataHandler
        self.SROpsHendler = ZFSSROperations()


class ZFSImplementation(Implementation):

    def __init__(self):
        super(ZFSImplementation, self).__init__()
        self.SR = ZFSSR()