#!/usr/bin/env python

import utils
import platform
from xapi.storage.libs.libzfs import qemudisk, meta
from xapi.storage import log
from xapi.storage.libs.libzfs import utils
from xapi.storage.libs.libzfs.utils import get_current_host_uuid


class Datapath(object):

    @classmethod
    def _open(cls, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    @classmethod
    def open(cls, dbg, uri, persistent):
        log.debug("%s: libzfs.Datapath.open: uri: %s persistent: %s"
                  % (dbg, uri, persistent))

        image_meta = meta.ZFSMetadataHandler.load(dbg, uri)

        if meta.NON_PERSISTENT_TAG in image_meta:
            vdi_non_persistent = image_meta[meta.NON_PERSISTENT_TAG]
        else:
            vdi_non_persistent = False

        if persistent:
            log.debug("%s: libzfs.Datapath.open: uri: %s will be marked as persistent" % (dbg, uri))
            if vdi_non_persistent:
                # unmark as non-peristent
                image_meta = {
                    meta.NON_PERSISTENT_TAG: None,
                }
                meta.ZFSMetadataHandler.update(dbg, uri, image_meta)
                # on detach remove special snapshot to rollback to
        elif vdi_non_persistent:
            log.debug("%s: libzfs.Datapath.open: uri: %s already marked as non-persistent" % (dbg, uri))
        else:
            log.debug("%s: libzfs.Datapath.open: uri: %s will be marked as non-persistent" % (dbg, uri))
            # mark as non-peristent
            image_meta = {
                meta.NON_PERSISTENT_TAG: True,
            }
            meta.ZFSMetadataHandler.update(dbg, uri, image_meta)
            # on attach create special snapshot to rollback to on detach

        cls._open(dbg, uri, persistent)

    @classmethod
    def _close(cls, dbg, uri):
        raise NotImplementedError('Override in Datapath specifc class')

    @classmethod
    def close(cls, dbg, uri):
        log.debug("%s: libzfs.Datapath.close: uri: %s"
                  % (dbg, uri))

        image_meta = meta.ZFSMetadataHandler.load(dbg, uri)

        if meta.NON_PERSISTENT_TAG in image_meta:
            vdi_non_persistent = image_meta[meta.NON_PERSISTENT_TAG]
        else:
            vdi_non_persistent = False

        log.debug("%s: libzfs.Datapath.close: uri: %s will be marked as persistent" % (dbg, uri))
        if vdi_non_persistent:
            # unmark as non-peristent
            image_meta = {
                meta.NON_PERSISTENT_TAG: None,
            }
            meta.ZFSMetadataHandler.update(dbg, uri, image_meta)

        cls._close(dbg, uri)

    @classmethod
    def _attach(cls, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    @classmethod
    def attach(cls, dbg, uri, domain):
        log.debug("%s: libzfs.Datapath.attach: uri: %s domain: %s"
                  % (dbg, uri, domain))

        if platform.linux_distribution()[1] == '7.5.0':
            protocol, params = cls._attach(dbg, uri, domain)
            return {
                'domain_uuid': '0',
                'implementation': [protocol, params]
            }
        elif platform.linux_distribution()[1] == '7.6.0':
            return {
                'implementations': cls._attach(dbg, uri, domain)
            }


    @classmethod
    def _detach(cls, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    @classmethod
    def detach(cls, dbg, uri, domain):
        log.debug("%s: libzfs.Datapath.detach: uri: %s domain: %s"
                  % (dbg, uri, domain))

        cls._detach(dbg, uri, domain)

    @classmethod
    def _activate(cls, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    @classmethod
    def activate(cls, dbg, uri, domain):
        log.debug("%s: libzfs.Datapath.activate: uri: %s domain: %s"
                  % (dbg, uri, domain))

        # TODO: Check that VDI is not active on other host

        cls._activate(dbg, uri, domain)

        image_meta = {
            meta.ACTIVE_ON_TAG: get_current_host_uuid()
        }

        meta.ZFSMetadataHandler.update(dbg, uri, image_meta)

    @classmethod
    def _deactivate(cls, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    @classmethod
    def deactivate(cls, dbg, uri, domain):
        log.debug("%s: libzfs.Datapath.deactivate: uri: %s domain: %s"
                  % (dbg, uri, domain))

        cls._deactivate(dbg, uri, domain)

        image_meta = {
            meta.ACTIVE_ON_TAG: None
        }

        meta.ZFSMetadataHandler.update(dbg, uri, image_meta)

    @classmethod
    def _suspend(cls, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    @classmethod
    def suspend(cls, dbg, uri, domain):
        log.debug("%s: libzfs.Datapath.suspend: uri: %s domain: %s"
                  % (dbg, uri, domain))

        cls._suspend(dbg, uri, domain)

    @classmethod
    def _resume(cls, dbg, uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    @classmethod
    def resume(cls, dbg, uri, domain):
        log.debug("%s: libzfs.Datapath.resume: uri: %s domain: %s"
                  % (dbg, uri, domain))

        cls._resume(dbg, uri, domain)

    @classmethod
    def _snapshot(cls, dbg, base_uri, snap_uri, domain):
        raise NotImplementedError('Override in Datapath specifc class')

    @classmethod
    def snapshot(cls, dbg, base_uri, snap_uri, domain):
        log.debug("%s: libzfs.Datapath.snapshot: base_uri: %s snap_uri: %s domain: %s"
                  % (dbg, base_uri, snap_uri, domain))

        cls._snapshot(dbg, base_uri, snap_uri, domain)


class QdiskDatapath(Datapath):

    @classmethod
    def _load_qemu_dp(cls, dbg, uri, domain):
        log.debug("%s: libzfs.QdiskDatapath._load_qemu_dp: uri: %s domain: %s"
                  % (dbg, uri, domain))

        image_meta = meta.ZFSMetadataHandler.load(dbg, uri)

        return qemudisk.introduce(dbg,
                                  utils.get_sr_uuid_by_uri(dbg, uri),
                                  image_meta[meta.UUID_TAG],
                                  utils.get_vdi_type_by_uri(dbg, uri),
                                  image_meta[meta.QEMU_PID_TAG],
                                  image_meta[meta.QEMU_QMP_SOCK_TAG],
                                  image_meta[meta.QEMU_NBD_SOCK_TAG],
                                  image_meta[meta.QEMU_QMP_LOG_TAG])

    @classmethod
    def _open(cls, dbg, uri, persistent):
        log.debug("%s: libzfs.QdiskDatapath._open: uri: %s persistent: %s"
                  % (dbg, uri, persistent))

    @classmethod
    def _close(cls, dbg, uri):
        log.debug("%s: libzfs.QdiskDatapath._close: uri: %s"
                  % (dbg, uri))

    @classmethod
    def _attach(cls, dbg, uri, domain):
        log.debug("%s: libzfs.QdiskDatapath._attach: uri: %s domain: %s"
                  % (dbg, uri, domain))

        protocol = 'Qdisk'

        qemu_dp = qemudisk.create(dbg, uri)

        image_meta = {
            meta.QEMU_PID_TAG: qemu_dp.pid,
            meta.QEMU_QMP_SOCK_TAG: qemu_dp.qmp_sock,
            meta.QEMU_NBD_SOCK_TAG: qemu_dp.nbd_sock,
            meta.QEMU_QMP_LOG_TAG: qemu_dp.qmp_log
        }

        meta.ZFSMetadataHandler.update(dbg, uri, image_meta)

        if platform.linux_distribution()[1] == '7.5.0':
            return (protocol, qemu_dp.params)
        elif platform.linux_distribution()[1] == '7.6.0':
            implementations = [
                [
                    'XenDisk',
                    {
                        'backend_type': 'qdisk',
                        'params': "vdi:{}".format(qemu_dp.vdi_uuid),
                        'extra': {}
                    }
                ],
                [
                    'Nbd',
                    {
                        'uri': 'nbd:unix:{}:exportname={}'
                            .format(qemu_dp.nbd_sock, qemudisk.ROOT_NODE_NAME)
                    }
                ]
            ]
            return (implementations)

    @classmethod
    def _detach(cls, dbg, uri, domain):
        log.debug("%s: libzfs.QdiskDatapath._detach: uri: %s domain: %s"
                  % (dbg, uri, domain))

        qemu_dp = cls._load_qemu_dp(dbg, uri, domain)

        qemu_dp.quit(dbg)

        image_meta = {
            meta.QEMU_PID_TAG: None,
            meta.QEMU_QMP_SOCK_TAG: None,
            meta.QEMU_NBD_SOCK_TAG: None,
            meta.QEMU_QMP_LOG_TAG: None
        }

        meta.ZFSMetadataHandler.update(dbg, uri, image_meta)

    @classmethod
    def _activate(cls, dbg, uri, domain):
        log.debug("%s: libzfs.QdiskDatapath._activate: uri: %s domain: %s"
                  % (dbg, uri, domain))

        qemu_dp = cls._load_qemu_dp(dbg, uri, domain)

        qemu_dp.open(dbg)

    @classmethod
    def _deactivate(cls, dbg, uri, domain):
        log.debug("%s: libzfs.QdiskDatapath._deactivate: uri: %s domain: %s"
                  % (dbg, uri, domain))

        qemu_dp = cls._load_qemu_dp(dbg, uri, domain)

        qemu_dp.close(dbg)

    @classmethod
    def _suspend(cls, dbg, uri, domain):
        log.debug("%s: libzfs.QdiskDatapath._suspend: uri: %s domain: %s"
                  % (dbg, uri, domain))

        qemu_dp = cls._load_qemu_dp(dbg, uri, domain)

        qemu_dp.suspend(dbg)

    @classmethod
    def _resume(cls, dbg, uri, domain):
        log.debug("%s: libzfs.QdiskDatapath._resume: uri: %s domain: %s"
                  % (dbg, uri, domain))

        qemu_dp = cls._load_qemu_dp(dbg, uri, domain)

        qemu_dp.resume(dbg)

    @classmethod
    def _snapshot(cls, dbg, base_uri, snap_uri, domain):
        log.debug("%s: libzfs.QdiskDatapath._snapshot: base_uri: %s snap_uri: %s domain: %s"
                  % (dbg, base_uri, snap_uri, domain))

        qemu_dp = cls._load_qemu_dp(dbg, base_uri, domain)

        qemu_dp.snap(dbg, snap_uri)


class Data(object):

    def _copy(cls, dbg, uri, domain, remote, blocklist):
        raise NotImplementedError('Override in Datapath specifc class')

    def copy(cls, dbg, uri, domain, remote, blocklist):
        cls._copy(dbg, uri, domain, remote, blocklist)

    def _mirror(cls, dbg, uri, domain, remote):
        raise NotImplementedError('Override in Datapath specifc class')

    def mirror(cls, dbg, uri, domain, remote):
        cls._mirror(dbg, uri, domain, remote)

    def _stat(cls, dbg, operation):
        raise NotImplementedError('Override in Datapath specifc class')

    def stat(cls, dbg, operation):
        cls._stat(dbg, operation)

    def _cancel(cls, dbg, operation):
        raise NotImplementedError('Override in Datapath specifc class')

    def cancel(cls, dbg, operation):
        cls._cancel(dbg, operation)

    def _destroy(cls, dbg, operation):
        raise NotImplementedError('Override in Datapath specifc class')

    def destroy(cls, dbg, operation):
        cls._destroy(dbg, operation)

    def _ls(cls, dbg):
        raise NotImplementedError('Override in Datapath specifc class')

    def ls(cls, dbg):
        cls._ls(dbg)


class QdiskData(Data):

    def _copy(cls, dbg, uri, domain, remote, blocklist):
        raise NotImplementedError('QdiskData._copy')

    def _mirror(cls, dbg, uri, domain, remote):
        raise NotImplementedError('QdiskData._mirror')

    def _stat(cls, dbg, operation):
        raise NotImplementedError('QdiskData._stat')

    def _cancel(cls, dbg, operation):
        raise NotImplementedError('QdiskData._cancel')

    def _destroy(cls, dbg, operation):
        raise NotImplementedError('QdiskData._destroy')

    def _ls(cls, dbg):
        raise NotImplementedError('QdiskData._ls')

