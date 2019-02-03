#!/usr/bin/env python

from xapi.storage import log
from xapi.storage.libs.xcpng.qemudisk import Qemudisk as _Qemudisk_
from xapi.storage.libs.xcpng.qemudisk import ROOT_NODE_NAME


class Qemudisk(_Qemudisk_):

    def _set_open_args(self, dbg):
        log.debug("%s: xcpng.libzfs.qemudisk.Qemudisk._set_open_args" % dbg)

        self.open_args = {'driver': self.vdi_type,
                          'cache': {'direct': True, 'no-flush': True},
                          # 'discard': 'unmap',
                          'file': {'driver': 'file',
                                   'aio': 'native',
                                   'filename': self.path},
                          # 'node-name': RBD_NODE_NAME},
                          'node-name': ROOT_NODE_NAME}
