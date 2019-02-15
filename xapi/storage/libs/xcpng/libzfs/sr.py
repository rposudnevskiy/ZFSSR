#!/usr/bin/env python

from xapi.storage import log
from xapi.storage.libs.xcpng.sr import SROperations as _SROperations_
from xapi.storage.libs.xcpng.libzfs.zfs_utils import pool_list, pool_import, pool_export, pool_create, pool_destroy, \
                                                     zvol_list, pool_get
from xapi.storage.libs.xcpng.utils import POOL_PREFIX, VDI_PREFIXES, get_sr_name_by_uri, get_sr_uuid_by_name, \
                                          get_vdi_type_by_uri, get_sr_type_by_uri


class SROperations(_SROperations_):

    def __init__(self):
        self.DEFAULT_SR_NAME = '<ZFS Zvol SR>'
        self.DEFAULT_SR_DESCRIPTION = '<ZFS Zvol SR>'
        super(SROperations, self).__init__()

    def create(self, dbg, uri, configuration):
        log.debug("%s: xcpng.libzfs.sr.SROperations.create: uri: %s configuration %s" % (dbg, uri, configuration))
        pool_create(dbg,
                    get_sr_name_by_uri(dbg, uri),
                    configuration['vdevs'],
                    configuration['mountpoint'])

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.sr.SROperations.destroy: uri: %s" % (dbg, uri))
        pool_destroy(dbg, get_sr_name_by_uri(dbg, uri))

    def get_sr_list(self, dbg, uri, configuration):
        log.debug("%s: xcpng.libzfs.sr.SROperations.get_sr_list: uri: %s configuration %s" % (dbg, uri, configuration))
        srs = []
        for zfs_pool in pool_list(dbg):
            if zfs_pool.startswith("%s%s" % (get_sr_type_by_uri(dbg,uri), POOL_PREFIX)):
                srs.append("%s/%s" % (uri, get_sr_uuid_by_name(dbg, zfs_pool)))
        return srs

    def get_vdi_list(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.sr.SROperations.get_vdi_list: uri: %s" % (dbg, uri))
        zvols = []
        for zvol in zvol_list(dbg, get_sr_name_by_uri(dbg, uri)):
            if zvol.startswith(VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)]):
                zvols.extend(zvol)
        return zvols

    def sr_import(self, dbg, uri, configuration):
        log.debug("%s: xcpng.libzfs.sr.SROperations.sr_import: uri: %s configuration %s" % (dbg, uri, configuration))
        pool_import(dbg,
                    get_sr_name_by_uri(dbg, uri),
                    configuration['mountpoint'])

    def sr_export(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.sr.SROperations.sr_export: uri: %s" % (dbg, uri))
        pool_export(dbg, get_sr_name_by_uri(dbg, uri))

    def get_free_space(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.sr.SROperations.get_free_space: uri: %s" % (dbg, uri))
        return int(pool_get(dbg, get_sr_name_by_uri(dbg, uri), 'free'))

    def get_size(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.sr.SROperations.get_size: uri: %s" % (dbg, uri))
        return int(pool_get(dbg, get_sr_name_by_uri(dbg, uri), 'size'))
