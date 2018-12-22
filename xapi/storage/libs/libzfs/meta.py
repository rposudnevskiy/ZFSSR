#!/usr/bin/env python

import json

from tinydb import TinyDB, Query, where
from tinydb.operations import delete

#from xapi.storage.libs.librbd import utils, ceph_utils, rbd_utils
from xapi.storage.libs.libzfs import utils, zfs_utils
from xapi.storage import log

import platform

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import Volume_does_not_exist
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.volume import Volume_does_not_exist

# define tags for metadata
UUID_TAG = 'uuid'
SR_UUID_TAG = 'sr_uuid'
TYPE_TAG = 'vdi_type'
KEY_TAG = 'key'
NAME_TAG = 'name'
DESCRIPTION_TAG = 'description'
CONFIGURATION_TAG = 'configuration'
READ_WRITE_TAG = 'read_write'
VIRTUAL_SIZE_TAG = 'virtual_size'
PHYSICAL_UTILISATION_TAG = 'physical_utilisation'
URI_TAG = 'uri'
CUSTOM_KEYS_TAG = 'keys'
SHARABLE_TAG = 'sharable'
NON_PERSISTENT_TAG = 'nonpersistent'
QEMU_PID_TAG = 'qemu_pid'
QEMU_QMP_SOCK_TAG = 'qemu_qmp_sock'
QEMU_NBD_SOCK_TAG = 'qemu_nbd_sock'
QEMU_QMP_LOG_TAG = 'qemu_qmp_log'
ACTIVE_ON_TAG = 'active_on'
SNAPSHOT_OF_TAG = 'snapshot_of'
IMAGE_FORMAT_TAG = 'image-format'
CEPH_CLUSTER_TAG = 'cluster'

# define tag types
TAG_TYPES = {
    UUID_TAG: str,
    SR_UUID_TAG: str,
    TYPE_TAG: str,
    KEY_TAG: str,
    NAME_TAG: str,
    DESCRIPTION_TAG: str,
    CONFIGURATION_TAG: eval, # dict
    READ_WRITE_TAG: eval, # boolean
    VIRTUAL_SIZE_TAG: int,
    PHYSICAL_UTILISATION_TAG: int,
    URI_TAG: eval, # string list
    CUSTOM_KEYS_TAG: eval, # dict
    SHARABLE_TAG: eval, # boolean
    NON_PERSISTENT_TAG: eval,
    QEMU_PID_TAG: int,
    QEMU_QMP_SOCK_TAG: str,
    QEMU_NBD_SOCK_TAG: str,
    QEMU_QMP_LOG_TAG: str,
    ACTIVE_ON_TAG: str,
    SNAPSHOT_OF_TAG: str,
    IMAGE_FORMAT_TAG: str,
    CEPH_CLUSTER_TAG: str
}

class MetadataHandler(object):

    @staticmethod
    def _create(dbg, uri):
        raise NotImplementedError('Override in MetadataHandler specifc class')

    @staticmethod
    def _remove(dbg, uri):
        raise NotImplementedError('Override in MetadataHandler specifc class')

    @staticmethod
    def _load(dbg, uri):
        raise NotImplementedError('Override in MetadataHandler specifc class')

    @staticmethod
    def _update(dbg, uri, image_meta):
        raise NotImplementedError('Override in MetadataHandler specifc class')

    @classmethod
    def create(cls, dbg, uri):
        log.debug("%s: meta.MetadataHandler.create: uri: %s "
                  % (dbg, uri))

        return cls._create(dbg, uri)

    @classmethod
    def remove(cls, dbg, uri):
        log.debug("%s: meta.MetadataHandler.remove: uri: %s "
                  % (dbg, uri))

        return cls._remove(dbg, uri)

    @classmethod
    def load(cls, dbg, uri):
        log.debug("%s: meta.MetadataHandler.load: uri: %s "
                  % (dbg, uri))

        return cls._load(dbg, uri)

    @classmethod
    def update(cls, dbg, uri, image_meta):
        log.debug("%s: meta.MetadataHandler.update: uri: %s "
                  % (dbg, uri))

        cls._update(dbg, uri, image_meta)

class ZFSMetadataHandler(MetadataHandler):

    @staticmethod
    def _create(dbg, uri):
        log.debug("%s: meta.ZFSMetadataHandler._create: uri: %s"
                  % (dbg, uri))

        sr_uuid = utils.get_sr_uuid_by_uri(dbg, uri)
        log.debug("%s: meta.ZFSMetadataHandler._load: metapath: %s" % (dbg, '%s/%s/__meta__' % (utils.SR_PATH_PREFIX, sr_uuid)))
        db = TinyDB('%s/%s/__meta__' % (utils.SR_PATH_PREFIX, sr_uuid), default_table='pool')

    @staticmethod
    def _remove(dbg, uri):
        log.debug("%s: meta.ZFSMetadataHandler._remove: uri: %s"
                  % (dbg, uri))

        sr_uuid = utils.get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = utils.get_vdi_uuid_by_uri(dbg, uri)

        try: # ignore exception if metadata doesn't exist. Fix it later
            db = TinyDB('%s/%s/__meta__' % (utils.SR_PATH_PREFIX, sr_uuid), default_table='pool')

            if vdi_uuid is not None:
                table = db.table('vdis')
            else:
                table = db.table('pool')

            try:
                table.remove(where('key') == vdi_uuid)
            except Exception:
                raise Volume_does_not_exist(uri)
        except:
            pass

    @staticmethod
    def _load(dbg, uri):
        log.debug("%s: meta.ZFSMetadataHandler._load: uri: %s"
                  % (dbg, uri))

        sr_uuid = utils.get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = utils.get_vdi_uuid_by_uri(dbg, uri)

        try: # ignore exception if metadata doesn't exist. Fix it later
            db = TinyDB('%s/%s/__meta__' % (utils.SR_PATH_PREFIX, sr_uuid), default_table='pool')

            if vdi_uuid != '':
                table = db.table('vdis')
            else:
                table = db.table('pool')

            try:
                image_meta = table.all()[0]

                log.debug("%s: meta.ZFSMetadataHandler._load: Pool_name: %s Metadata: %s "
                          % (dbg, sr_uuid, image_meta))
            except Exception:
                raise Volume_does_not_exist(uri)

            return image_meta
        except:
            return {}

    @staticmethod
    def _update(dbg, uri, image_meta):
        log.debug("%s: meta.ZFSMetadataHandler._update_meta: uri: %s image_meta: %s"
                  % (dbg, uri, image_meta))

        sr_uuid = utils.get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = utils.get_vdi_uuid_by_uri(dbg, uri)

        log.debug("%s: meta.ZFSMetadataHandler._update_meta: sr_uuid: %s vdi_uuid: '%s'"
                 % (dbg, sr_uuid, vdi_uuid))

        try: # ignore exception if metadata doesn't exist. Fix it later
            db = TinyDB('%s/%s/__meta__' % (utils.SR_PATH_PREFIX, sr_uuid), default_table='pool')

            if vdi_uuid != '':
                table = db.table('vdis')
                uuid_tag = UUID_TAG
                uuid = vdi_uuid
            else:
                table = db.table('pool')
                uuid_tag = SR_UUID_TAG
                uuid = sr_uuid

            if table.search(Query()[uuid_tag] == uuid):
                try:
                    for tag, value in image_meta.iteritems():
                        if value is None:
                            log.debug("%s: meta.ZFSMetadataHandler._update_meta: tag: %s remove value" % (dbg, tag))
                            table.update(delete(tag), Query()[uuid_tag] == uuid)
                        else:
                            log.debug("%s: meta.ZFSMetadataHandler._update_meta: tag: %s set value: %s" % (dbg, tag, value))
                            table.update({tag: value}, Query()[uuid_tag] == uuid)
                except Exception:
                    raise Volume_does_not_exist(uri)
            else:
                try:
                    table.insert(image_meta)
                except Exception:
                    raise Volume_does_not_exist(uri)
        except:
            pass
