#!/usr/bin/env python

from xapi.storage.libs.xcpng.datapath import DatapathOperations as _DatapathOperations_
from xapi.storage.libs.xcpng.meta import IMAGE_UUID_TAG
from xapi.storage.libs.xcpng.utils import POOL_PREFIX, VDI_PREFIXES, get_sr_type_by_uri, \
                                          get_sr_uuid_by_uri, get_vdi_type_by_uri


class DatapathOperations(_DatapathOperations_):

    def map_vol(self, dbg, uri, chained=False):
        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        self.blkdev = "/dev/zvol/%s%s%s/%s%s" % (get_sr_type_by_uri(dbg, uri),
                                                 POOL_PREFIX,
                                                 get_sr_uuid_by_uri(dbg, uri),
                                                 VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                                 volume_meta[IMAGE_UUID_TAG])
        super(DatapathOperations, self).map_vol(dbg, uri, chained)
