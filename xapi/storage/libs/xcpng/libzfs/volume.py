#!/usr/bin/env python

from xapi.storage.libs.xcpng.utils import POOL_PREFIX, VDI_PREFIXES, get_sr_uuid_by_uri, get_vdi_type_by_uri, \
                                          get_vdi_uuid_by_uri, get_sr_type_by_uri
from xapi.storage.libs.xcpng.utils import roundup

from xapi.storage.libs.xcpng.volume import VOLUME_TYPES, Implementation
from xapi.storage.libs.xcpng.volume import VolumeOperations as _VolumeOperations_
from xapi.storage.libs.xcpng.volume import RAWVolume as _RAWVolume_
from xapi.storage.libs.xcpng.volume import QCOW2Volume as _QCOW2Volume_

from xapi.storage.libs.xcpng.libzfs.zfs_utils import VOLBLOCKSIZE
from xapi.storage.libs.xcpng.libzfs.zfs_utils import zvol_create, zvol_destroy, zvol_set, zvol_get, zvol_rename
from xapi.storage.libs.xcpng.libzfs.meta import MetadataHandler
from xapi.storage.libs.xcpng.libzfs.datapath import DATAPATHES

from xapi.storage import log

class VolumeOperations(_VolumeOperations_):

    def create(self, dbg, uri, size):
        log.debug("%s: xcpng.libzfs.volume.VolumeOperations.create: uri: %s size: %s" % (dbg, uri, size))
        image_name = "%s%s%s/%s%s" % (get_sr_type_by_uri(dbg, uri),
                                      POOL_PREFIX,
                                      get_sr_uuid_by_uri(dbg, uri),
                                      VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                      get_vdi_uuid_by_uri(dbg, uri))
        zvol_create(dbg, image_name, size)

    def destroy(cls, dbg, uri):
        log.debug("%s: xcpng.libzfs.volume.VolumeOperations.destroy: uri: %s" % (dbg, uri))
        image_name = "%s%s%s/%s%s" % (get_sr_type_by_uri(dbg, uri),
                                      POOL_PREFIX,
                                      get_sr_uuid_by_uri(dbg, uri),
                                      VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                      get_vdi_uuid_by_uri(dbg, uri))
        zvol_destroy(dbg, image_name)

    def resize(self, dbg, uri, size):
        log.debug("%s: xcpng.libzfs.volume.VolumeOperations.resize: uri: %s size: %s" % (dbg, uri, size))
        image_name = "%s%s%s/%s%s" % (get_sr_type_by_uri(dbg, uri),
                                      POOL_PREFIX,
                                      get_sr_uuid_by_uri(dbg, uri),
                                      VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                      get_vdi_uuid_by_uri(dbg, uri))

        zvol_set(dbg, image_name, 'volsize', size)

    def swap(self, dbg, uri1, uri2):
        log.debug("%s: xcpng.libzfs.volume.VolumeOperations.swap: uri1: %s uri2: %s" % (dbg, uri1, uri2))
        tmp_image_name = "%s%s%s/%s" % (get_sr_type_by_uri(dbg, uri1),
                                        POOL_PREFIX,
                                        get_sr_uuid_by_uri(dbg, uri1),
                                        'tmp')
        image_name1 = "%s%s%s/%s%s" % (get_sr_type_by_uri(dbg, uri1),
                                       POOL_PREFIX,
                                       get_sr_uuid_by_uri(dbg, uri1),
                                       VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri1)],
                                       get_vdi_uuid_by_uri(dbg, uri1))
        image_name2 = "%s%s%s/%s%s" % (get_sr_type_by_uri(dbg, uri2),
                                       POOL_PREFIX,
                                       get_sr_uuid_by_uri(dbg, uri2),
                                       VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri2)],
                                       get_vdi_uuid_by_uri(dbg, uri2))
        zvol_rename(dbg, image_name1, tmp_image_name)
        zvol_rename(dbg, image_name2, image_name1)
        zvol_rename(dbg, tmp_image_name, image_name2)

    def get_phisical_utilization(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.volume.VolumeOperations.get_phisical_utilization: uri: %s" % (dbg, uri))
        image_name = "%s%s%s/%s%s" % (get_sr_type_by_uri(dbg, uri),
                                      POOL_PREFIX,
                                      get_sr_uuid_by_uri(dbg, uri),
                                      VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                      get_vdi_uuid_by_uri(dbg, uri))

        return int(zvol_get(dbg, image_name, 'referenced'))

    def roundup_size(self, dbg, size):
        return roundup(VOLBLOCKSIZE, size)


class RAWVolume(_RAWVolume_):

    def __init__(self):
        super(RAWVolume, self).__init__()
        self.MetadataHandler = MetadataHandler()
        self.VolOpsHendler = VolumeOperations()
        self.Datapathes = {}
        for k, v in DATAPATHES.iteritems():
            self.Datapathes[k] = v()

class QCOW2Volume(_QCOW2Volume_):

    def __init__(self):
        super(QCOW2Volume, self).__init__()
        self.MetadataHandler = MetadataHandler()
        self.VolOpsHendler = VolumeOperations()
        self.Datapathes = {}
        for k, v in DATAPATHES.iteritems():
            self.Datapathes[k] = v()

VOLUME_TYPES['raw'] = RAWVolume
VOLUME_TYPES['qcow2'] = QCOW2Volume
