#!/usr/bin/env python

from subprocess import call

from xapi.storage.libs.xcpng.utils import POOL_PREFIX, VDI_PREFIXES, SR_PATH_PREFIX, get_sr_uuid_by_uri, \
                                          get_vdi_type_by_uri, get_vdi_uuid_by_uri
from xapi.storage.libs.xcpng.utils import roundup
from xapi.storage.libs.xcpng.volume import VolumeOperations, RAWVolume, QCOW2Volume, Implementation
from xapi.storage.libs.xcpng.libzfs.zfs_utils import zvol_create, zvol_destroy, zvol_set, zvol_get, VOLBLOCKSIZE
from xapi.storage.libs.xcpng.libzfs.meta import ZFSMetadataHandler


class ZFSVolumeOperations(VolumeOperations):

    def create(self, dbg, uri, size, path):
        image_name = "ZFS%s%s/%s%s" % (POOL_PREFIX,
                                       get_sr_uuid_by_uri(dbg, uri),
                                       VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                       get_vdi_uuid_by_uri(dbg, uri))

        zvol_create(dbg, image_name, size)

        device = "/dev/zvol/%s" % image_name

        call(['ln', '-s', device, "%s/%s/%s" % (SR_PATH_PREFIX,
                                                 get_sr_uuid_by_uri(dbg, uri),
                                                 get_vdi_uuid_by_uri(dbg, uri))])

    def destroy(cls, dbg, uri, path):
        image_name = "ZFS%s%s/%s%s" % (POOL_PREFIX,
                                       get_sr_uuid_by_uri(dbg, uri),
                                       VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                       get_vdi_uuid_by_uri(dbg, uri))

        call(['unlink', "%s/%s/%s" % (SR_PATH_PREFIX,
                                      get_sr_uuid_by_uri(dbg, uri),
                                      get_vdi_uuid_by_uri(dbg, uri))])

        zvol_destroy(dbg, image_name)

    def resize(self, dbg, uri, size):
        image_name = "ZFS%s%s/%s%s" % (POOL_PREFIX,
                                       get_sr_uuid_by_uri(dbg, uri),
                                       VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                       get_vdi_uuid_by_uri(dbg, uri))

        zvol_set(dbg, image_name, 'volsize', size)

    def map(self, dbg, uri, path):
        pass

    def unmap(self, dbg, uri, path):
        pass

    def get_phisical_utilization(self, dbg, uri):
        image_name = "ZFS%s%s/%s%s" % (POOL_PREFIX,
                                       get_sr_uuid_by_uri(dbg, uri),
                                       VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                       get_vdi_uuid_by_uri(dbg, uri))

        return int(zvol_get(dbg, image_name, 'referenced'))

    def roundup_size(self, dbg, size):
        return roundup(VOLBLOCKSIZE, size)


class ZFSRAWVolume(RAWVolume):

    def __init__(self):
        super(ZFSRAWVolume, self).__init__()
        self.MetadataHandler = ZFSMetadataHandler
        self.VolOpsHendler = ZFSVolumeOperations()

class ZFSQCOW2Volume(QCOW2Volume):

    def __init__(self):
        super(ZFSQCOW2Volume, self).__init__()
        self.MetadataHandler = ZFSMetadataHandler
        self.VolOpsHendler = ZFSVolumeOperations()

class ZFSImplementation(Implementation):

    def __init__(self):
        super(ZFSImplementation, self).__init__()
        self.RAWVolume = ZFSRAWVolume()
        self.QCOW2Volume = ZFSQCOW2Volume()
