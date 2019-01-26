#!/usr/bin/env python

from xapi.storage.libs.xcpng.datapath import DATAPATHES, Implementation
from xapi.storage.libs.xcpng.datapath import QdiskDatapath as _QdiskDatapath_
from xapi.storage.libs.xcpng.libzfs.qemudisk import Qemudisk
from xapi.storage.libs.xcpng.libzfs.meta import MetadataHandler
from xapi.storage.libs.xcpng.utils import POOL_PREFIX, VDI_PREFIXES, SR_PATH_PREFIX, \
                                          get_sr_uuid_by_uri, get_vdi_uuid_by_uri, get_vdi_type_by_uri


class QdiskDatapath(_QdiskDatapath_):

    def __init__(self):
        super(QdiskDatapath, self).__init__()
        self.MetadataHandler = MetadataHandler()
        self.qemudisk = Qemudisk

    def map_vol(self, dbg, uri, chained=False):
        self.blkdev = "/dev/zvol/ZFS%s%s/%s%s" % (POOL_PREFIX,
                                                  get_sr_uuid_by_uri(dbg, uri),
                                                  VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)],
                                                  get_vdi_uuid_by_uri(dbg, uri))
        super(QdiskDatapath, self).map_vol(dbg, uri, chained)


DATAPATHES['qdisk'] = QdiskDatapath()
