#!/usr/bin/env python

from subprocess import Popen, PIPE
from xapi.storage.libs.libzfs import qmp
from xapi.storage.libs.libzfs.qemudisk import QEMU_DP_SOCKET_DIR
from xapi.storage import log
import sys
from time import sleep

VAR_RUN_PREFIX = '/var/run'
QMP_CONNECT_MAX_ATTEMPTS = 5


def read(path):
    proc = Popen(["/usr/bin/xenstore-read", path], stdout=PIPE)
    return proc.stdout.readline().strip()


def watch(path):
    proc = Popen(["/usr/bin/xenstore-watch", path], stdout=PIPE)
    proc.stdout.readline()
    return proc


def found_new_qdisk(domid, devid, vdi_uuid):
    qmp_sock = QEMU_DP_SOCKET_DIR + "/qmp_sock.{}".format(vdi_uuid)
    _qmp_ = qmp.QEMUMonitorProtocol(qmp_sock)

    path = "%s/%s" % (VAR_RUN_PREFIX, vdi_uuid)
    with open(path, 'w') as fd:
        fd.write("/local/domain/%s/device/vbd/%s/state" % (domid, devid))

    params = {}
    params['domid'] = domid
    params['devid'] = devid
    params['type'] = 'qdisk'
    params['blocknode'] = 'qemu_node'
    params['devicename'] = vdi_uuid

    connected = False
    count = 0
    while not connected:
        try:
            _qmp_.connect()
            connected = True
        except:
            if count > QMP_CONNECT_MAX_ATTEMPTS:
                log.debug("%s: ERROR: qemu-dp on socket %s couldn't be connected for vdi_uuid"
                          % (sys.argv[0], qmp_sock))
                return
            sleep(1)
            count += 1

    _qmp_.command('xen-watch-device', **params)



if __name__ == "__main__":
    proc = watch("/local/domain/0/backend")

    while True:
        path = proc.stdout.readline().strip()  # block until we get an event
        tokens = path.split('/')

        if len(tokens) > 8 and tokens[5] == 'qdisk' and tokens[8] == 'qemu-params':
            domid = int(tokens[6])
            devid = int(tokens[7])
            contents = read(path)
            log.debug("%s: Found new qdisk with domid=%d devid=%d contents=%s" % (sys.argv[0], domid, devid, contents))
            (prefix, vdi_uuid) = contents.split(':')
            if (prefix == 'vdi'):
                found_new_qdisk(domid, devid, vdi_uuid)
