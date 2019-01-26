#!/usr/bin/env python

from subprocess import call

from xapi.storage import log

from tinydb import TinyDB, Query, where
from tinydb.operations import delete

from xapi.storage.libs.xcpng.utils import get_sr_uuid_by_uri, get_vdi_uuid_by_uri, SR_PATH_PREFIX
from xapi.storage.libs.xcpng.meta import MetadataHandler as _MetadataHandler_
from xapi.storage.libs.xcpng.meta import UUID_TAG, SR_UUID_TAG, PARENT_URI_TAG

class MetadataHandler(_MetadataHandler_):

    def _create(self, dbg, uri):
        log.debug("%s: meta.ZFSMetadataHandler._create: uri: %s"
                  % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        log.debug("%s: meta.ZFSMetadataHandler._load: metapath: %s" % (dbg, '%s/%s/__meta__' % (SR_PATH_PREFIX, sr_uuid)))
        db = TinyDB("%s/%s/__meta__" % (SR_PATH_PREFIX, sr_uuid), default_table='pool')

    def _destroy(self, dbg, uri):
        log.debug("%s: meta.ZFSMetadataHandler._destroy: uri: %s"
                  % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)

        call(['rm', '-f', "%s/%s/__meta__" % (SR_PATH_PREFIX, sr_uuid)])

    def _remove(self, dbg, uri):
        log.debug("%s: meta.ZFSMetadataHandler._remove: uri: %s"
                  % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)

        try: # ignore exception if metadata doesn't exist. Fix it later
            db = TinyDB('%s/%s/__meta__' % (SR_PATH_PREFIX, sr_uuid), default_table='pool')

            if vdi_uuid != '':
                table = db.table('vdis')
                uuid_tag = UUID_TAG
                uuid = vdi_uuid
            else:
                table = db.table('pool')
                uuid_tag = SR_UUID_TAG
                uuid = sr_uuid

            try:
                table.remove(where(uuid_tag) == uuid)
            except Exception:
                raise Exception("Failed to remove metadata for uri %s" % uri)
        except:
            pass

    def _load(self, dbg, uri):
        log.debug("%s: meta.ZFSMetadataHandler._load: uri: %s"
                  % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)

        try: # ignore exception if metadata doesn't exist. Fix it later
            db = TinyDB('%s/%s/__meta__' % (SR_PATH_PREFIX, sr_uuid), default_table='pool')

            if vdi_uuid != '':
                table = db.table('vdis')
                uuid_tag = UUID_TAG
                uuid = vdi_uuid
            else:
                table = db.table('pool')
                uuid_tag = SR_UUID_TAG
                uuid = sr_uuid

            try:
                image_meta = table.search(where(uuid_tag) == uuid)[0]

                log.debug("%s: meta.ZFSMetadataHandler._load: Pool/Image_name: %s Metadata: %s "
                          % (dbg, uuid, image_meta))
            except Exception:
                raise Exception("Failed to load metadata for uri %s" % uri)

            return image_meta
        except:
            return {}

    def _update(self, dbg, uri, image_meta):
        log.debug("%s: meta.ZFSMetadataHandler._update_meta: uri: %s image_meta: %s"
                  % (dbg, uri, image_meta))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)

        log.debug("%s: meta.ZFSMetadataHandler._update_meta: sr_uuid: %s vdi_uuid: '%s'"
                 % (dbg, sr_uuid, vdi_uuid))

        try: # ignore exception if metadata doesn't exist. Fix it later
            db = TinyDB('%s/%s/__meta__' % (SR_PATH_PREFIX, sr_uuid), default_table='pool')

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
                    raise Exception("Failed to update metadata for uri %s" % uri)
            else:
                try:
                    table.insert(image_meta)
                except Exception:
                    raise Exception("Failed to update metadata for uri %s" % uri)
        except:
            pass

    def _get_vdi_chain(self, dbg, uri):
        log.debug("%s: meta.ZFSMetadataHandler._get_vdi_chain: uri: %s"
                  % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)

        vdi_chain = []

        try:
            db = TinyDB('%s/%s/__meta__' % (SR_PATH_PREFIX, sr_uuid), default_table='vdis')
        except:
            raise Exception("Failed to open metadata db")

        while True:
            try:
                image_meta = db.search(where(UUID_TAG) == vdi_uuid)[0]
            except Exception:
                raise Exception("Failed to load metadata for uri %s" % uri)

            if PARENT_URI_TAG in image_meta:
                vdi_chain.append(image_meta[PARENT_URI_TAG])
                vdi_uuid = get_vdi_uuid_by_uri(dbg, image_meta[PARENT_URI_TAG])
            else:
                break

        return vdi_chain


