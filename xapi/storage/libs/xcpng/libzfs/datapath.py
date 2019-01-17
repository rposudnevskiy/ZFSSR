#!/usr/bin/env python

from xapi.storage.libs.xcpng.datapath import QdiskDatapath, Implementation
from xapi.storage.libs.xcpng.libzfs.qemudisk import ZFSQemudisk
from xapi.storage.libs.xcpng.libzfs.meta import ZFSMetadataHandler


class ZFSQdiskDatapath(QdiskDatapath):

    def __init__(self):
        super(ZFSQdiskDatapath, self).__init__()
        self.MetadataHandler = ZFSMetadataHandler
        self.qemudisk = ZFSQemudisk


class ZFSImplementation(Implementation):

    def __init__(self):
        super(ZFSImplementation, self).__init__()
        self.Datapath = ZFSQdiskDatapath()
