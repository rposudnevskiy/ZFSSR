#!/usr/bin/env python

from xapi.storage.libs.xcpng.utils import POOL_PREFIX, VDI_PREFIXES, get_sr_uuid_by_uri, get_vdi_type_by_uri, \
                                          get_sr_type_by_uri
from xapi.storage.libs.xcpng.utils import roundup
from xapi.storage.libs.xcpng.meta import IMAGE_UUID_TAG
from xapi.storage.libs.xcpng.volume import VolumeOperations as _VolumeOperations_

from xapi.storage.libs.xcpng.libzfs.zfs_utils import VOLBLOCKSIZE
from xapi.storage.libs.xcpng.libzfs.zfs_utils import zvol_create, zvol_destroy, zvol_set, zvol_get

from xapi.storage import log


class VolumeOperations(_VolumeOperations_):

    def create(self, dbg, uri, size):
        log.debug("%s: xcpng.libzfs.volume.VolumeOperations.create: uri: %s size: %s" % (dbg, uri, size))
        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        image_name = "%s%s%s/%s%s" % (get_sr_type_by_uri(dbg, uri),
                                      POOL_PREFIX,
                                      get_sr_uuid_by_uri(dbg, uri),
                                      VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                      volume_meta[IMAGE_UUID_TAG])
        zvol_create(dbg, image_name, size)

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.volume.VolumeOperations.destroy: uri: %s" % (dbg, uri))
        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        image_name = "%s%s%s/%s%s" % (get_sr_type_by_uri(dbg, uri),
                                      POOL_PREFIX,
                                      get_sr_uuid_by_uri(dbg, uri),
                                      VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                      volume_meta[IMAGE_UUID_TAG])
        zvol_destroy(dbg, image_name)

    def resize(self, dbg, uri, size):
        log.debug("%s: xcpng.libzfs.volume.VolumeOperations.resize: uri: %s size: %s" % (dbg, uri, size))
        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        image_name = "%s%s%s/%s%s" % (get_sr_type_by_uri(dbg, uri),
                                      POOL_PREFIX,
                                      get_sr_uuid_by_uri(dbg, uri),
                                      VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                      volume_meta[IMAGE_UUID_TAG])

        zvol_set(dbg, image_name, 'volsize', size)

    def get_phisical_utilization(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.volume.VolumeOperations.get_phisical_utilization: uri: %s" % (dbg, uri))
        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        image_name = "%s%s%s/%s%s" % (get_sr_type_by_uri(dbg, uri),
                                      POOL_PREFIX,
                                      get_sr_uuid_by_uri(dbg, uri),
                                      VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                      volume_meta[IMAGE_UUID_TAG])

        return int(zvol_get(dbg, image_name, 'referenced'))

    def roundup_size(self, dbg, size):
        return roundup(VOLBLOCKSIZE, size)
