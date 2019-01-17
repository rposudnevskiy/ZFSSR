#!/usr/bin/env python

from xapi.storage.libs.xcpng.qemudisk import Qemudisk
from xapi.storage.libs.xcpng.qemudisk import ROOT_NODE_NAME


class ZFSQemudisk(Qemudisk):

    def _set_open_args_(self):

        self.open_args = {'driver': self.vdi_type,
                          'cache': {'direct': True, 'no-flush': True},
                          # 'discard': 'unmap',
                          'file': {'driver': 'file',
                                   'aio': 'native',
                                   'filename': self.path},
                          # 'node-name': RBD_NODE_NAME},
                          'node-name': ROOT_NODE_NAME}