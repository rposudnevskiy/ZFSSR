#!/usr/bin/env python

from subprocess import call

from xapi.storage.libs.xcpng.utils import POOL_PREFIX, VDI_PREFIXES, get_sr_uuid_by_uri, get_vdi_type_by_uri, \
                                          get_vdi_uuid_by_uri
from xapi.storage.libs.xcpng.utils import roundup

from xapi.storage.libs.xcpng.volume import VOLUME_TYPES, Implementation
from xapi.storage.libs.xcpng.volume import VolumeOperations as _VolumeOperations_
from xapi.storage.libs.xcpng.volume import RAWVolume as _RAWVolume_
from xapi.storage.libs.xcpng.volume import QCOW2Volume as _QCOW2Volume_

from xapi.storage.libs.xcpng.libzfs.zfs_utils import VOLBLOCKSIZE
from xapi.storage.libs.xcpng.libzfs.zfs_utils import zvol_create, zvol_destroy, zvol_set, zvol_get, zvol_rename
from xapi.storage.libs.xcpng.libzfs.meta import MetadataHandler
from xapi.storage.libs.xcpng.libzfs.datapath import DATAPATHES


class VolumeOperations(_VolumeOperations_):

    def create(self, dbg, uri, size):
        image_name = "ZFS%s%s/%s%s" % (POOL_PREFIX,
                                       get_sr_uuid_by_uri(dbg, uri),
                                       VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                       get_vdi_uuid_by_uri(dbg, uri))
        zvol_create(dbg, image_name, size)

    def destroy(cls, dbg, uri):
        image_name = "ZFS%s%s/%s%s" % (POOL_PREFIX,
                                       get_sr_uuid_by_uri(dbg, uri),
                                       VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                       get_vdi_uuid_by_uri(dbg, uri))
        zvol_destroy(dbg, image_name)

    def resize(self, dbg, uri, size):
        image_name = "ZFS%s%s/%s%s" % (POOL_PREFIX,
                                       get_sr_uuid_by_uri(dbg, uri),
                                       VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                       get_vdi_uuid_by_uri(dbg, uri))

        zvol_set(dbg, image_name, 'volsize', size)

    def swap(self, dbg, uri1, uri2):
        tmp_image_name = "ZFS%s%s/%s" % (POOL_PREFIX,
                                           get_sr_uuid_by_uri(dbg, uri1),
                                           'tmp')
        image_name1 = "ZFS%s%s/%s%s" % (POOL_PREFIX,
                                       get_sr_uuid_by_uri(dbg, uri1),
                                       VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri1)],
                                       get_vdi_uuid_by_uri(dbg, uri1))
        image_name2 = "ZFS%s%s/%s%s" % (POOL_PREFIX,
                                       get_sr_uuid_by_uri(dbg, uri2),
                                       VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri2)],
                                       get_vdi_uuid_by_uri(dbg, uri2))
        zvol_rename(dbg, image_name1, tmp_image_name)
        zvol_rename(dbg, image_name2, image_name1)
        zvol_rename(dbg, tmp_image_name, image_name2)

    def get_phisical_utilization(self, dbg, uri):
        image_name = "ZFS%s%s/%s%s" % (POOL_PREFIX,
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
        self.Datapathes = DATAPATHES

class QCOW2Volume(_QCOW2Volume_):

    def __init__(self):
        super(QCOW2Volume, self).__init__()
        self.MetadataHandler = MetadataHandler()
        self.VolOpsHendler = VolumeOperations()
        self.Datapathes = DATAPATHES

VOLUME_TYPES['raw'] = RAWVolume
VOLUME_TYPES['qcow2'] = QCOW2Volume
