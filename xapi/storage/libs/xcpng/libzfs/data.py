#!/usr/bin/env python

from xapi.storage.libs.xcpng.data import QdiskData, Implementation
from xapi.storage.libs.xcpng.libzfs.qemudisk import ZFSQemudisk
from xapi.storage.libs.xcpng.libzfs.meta import ZFSMetadataHandler


class ZFSQdiskData(QdiskData):

    def __init__(self):
        super(ZFSQdiskData, self).__init__()
        self.MetadataHandler = ZFSMetadataHandler
        self.qemudisk = ZFSQemudisk


class ZFSImplementation(Implementation):

    def __init__(self):
        super(ZFSImplementation, self).__init__()
        self.Datapath = ZFSQdiskData()
