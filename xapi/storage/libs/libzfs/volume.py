#!/usr/bin/env python

import os

import uuid

from subprocess import call, Popen, PIPE, check_output

from xapi.storage.libs.libzfs import utils, zfs_utils, meta
import copy

from xapi.storage import log

import platform

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import Volume_does_not_exist, Activated_on_another_host
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.volume import Volume_does_not_exist, Activated_on_another_host

#from xapi.storage.libs.libzfs import get_current_host_uuid

# TODO: We should import correct datapath depend on image uri
# from xapi.storage.libs.libzfs.datapath import QdiskDatapath as Datapath


class Volume(object):

    @classmethod
    def _create(cls, dbg, sr, name, description, size, sharable, image_meta):
        # Override in Volume specifc class
        return image_meta

    @classmethod
    def create(cls, dbg, sr, name, description, size, sharable):
        log.debug("%s: libzfs.Volume.create: SR: %s Name: %s Description: %s Size: %s, Sharable: %s"
                  % (dbg, sr, name, description, size, sharable))

        vdi_uuid = str(uuid.uuid4())
        vdi_uri = "%s/%s" % (sr, vdi_uuid)
        vsize = size
        psize = size

        image_meta = {
            meta.KEY_TAG: vdi_uuid,
            meta.UUID_TAG: vdi_uuid,
            meta.NAME_TAG: name,
            meta.DESCRIPTION_TAG: description,
            meta.READ_WRITE_TAG: True,
            meta.VIRTUAL_SIZE_TAG: vsize,
            meta.PHYSICAL_UTILISATION_TAG: psize,
            meta.URI_TAG: [vdi_uri],
            meta.SHARABLE_TAG: sharable, # False,
            meta.CUSTOM_KEYS_TAG: {}
        }

        return cls._create(dbg, sr, name, description, size, sharable, image_meta)


    @classmethod
    def _set(cls, dbg, sr, key, k, v, image_meta):
        # Override in Volume specifc class
        pass

    @classmethod
    def set(cls, dbg, sr, key, k, v):
        log.debug("%s: libzfs.Volume.set: SR: %s Key: %s Custom_key: %s Value: %s"
                  % (dbg, sr, key, k, v))

        uri = "%s/%s" % (sr, key)

        try:
            image_meta = meta.ZFSMetadataHandler.load(dbg, uri)
            image_meta['keys'][k] = v
            meta.ZFSMetadataHandler.update(dbg, uri, image_meta)
            cls._set(dbg, sr, key, k, v, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)


    @classmethod
    def _unset(cls, dbg, sr, key, k, image_meta):
        # Override in Volume specifc class
        pass

    @classmethod
    def unset(cls, dbg, sr, key, k):
        log.debug("%s: libzfs.Volume.unset: SR: %s Key: %s Custom_key: %s"
                  % (dbg, sr, key, k))

        uri = "%s/%s" % (sr, key)

        try:
            image_meta = meta.ZFSMetadataHandler.load(dbg, uri)
            image_meta['keys'].pop(k, None)
            meta.ZFSMetadataHandler.update(dbg, uri, image_meta)
            cls._unset(dbg, sr, key, k, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    @classmethod
    def _stat(cls, dbg, sr, key, image_meta):
        # Override in Volume specific class
        return image_meta

    @classmethod
    def stat(cls, dbg, sr, key):
        log.debug("%s: libzfs.Volume.stat: SR: %s Key: %s"
                  % (dbg, sr, key))

        uri = "%s/%s" % (sr, key)
        image_name = "ZFS%s%s/%s%s" % (utils.POOL_PREFIX,
                                       utils.get_sr_uuid_by_uri(dbg, sr),
                                       utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)],
                                       key)

        try:
            image_meta = meta.ZFSMetadataHandler.load(dbg, uri)
            image_meta[meta.PHYSICAL_UTILISATION_TAG] = int(zfs_utils.zvol_get(dbg, image_name, 'referenced'))
            #meta.ZFSMetadataHandler.update(dbg, uri, image_meta)
            log.debug("%s: libzfs.Volume.stat: SR: %s Key: %s Metadata: %s"
                      % (dbg, sr, key, image_meta))
            return cls._stat(dbg, sr, key, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    @classmethod
    def _destroy(cls, dbg, sr, key):
        # Override in Volume specifc class
        pass

    @classmethod
    def destroy(cls, dbg, sr, key):
        log.debug("%s: libzfs.Volume.destroy: SR: %s Key: %s"
                  % (dbg, sr, key))

        uri = "%s/%s" % (sr, key)
        image_name = "ZFS%s%s/%s%s" % (utils.POOL_PREFIX,
                                       utils.get_sr_uuid_by_uri(dbg, sr),
                                       utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)],
                                       key)

        try:
            meta.ZFSMetadataHandler.remove(dbg, uri)
            zfs_utils.zvol_destroy(dbg, image_name)
            call(['unlink', "%s/%s/%s" % (utils.SR_PATH_PREFIX,
                                          utils.get_sr_uuid_by_uri(dbg, sr),
                                          key)])
            cls._destroy(dbg, sr, key)
        except Exception:
           raise Volume_does_not_exist(key)

    @classmethod
    def _set_description(cls, dbg, sr, key, new_description, image_meta):
        # Override in Volume specifc class
        pass

    @classmethod
    def set_description(cls, dbg, sr, key, new_description):
        log.debug("%s: libzfs.Volume.set_description: SR: %s Key: %s New_description: %s"
                  % (dbg, sr, key, new_description))

        uri = "%s/%s" % (sr, key)

        image_meta = {
            'description': new_description,
        }

        try:
            meta.ZFSMetadataHandler.update(dbg, uri, image_meta)
            cls._set_description(dbg, sr, key, new_description, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    @classmethod
    def _set_name(cls, dbg, sr, key, new_name, image_meta):
        # Override in Volume specifc class
        pass

    @classmethod
    def set_name(cls, dbg, sr, key, new_name):
        log.debug("%s: libzfs.Volume.set_name: SR: %s Key: %s New_name: %s"
                  % (dbg, sr, key, new_name))

        uri = "%s/%s" % (sr, key)

        image_meta = {
            'name': new_name,
        }

        try:
            meta.ZFSMetadataHandler.update(dbg, uri, image_meta)
            cls._set_name(dbg, sr, key, new_name, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    @classmethod
    def _resize(cls, dbg, sr, key, new_size, image_meta):
        # Override in Volume specifc class
        pass

    @classmethod
    def resize(cls, dbg, sr, key, new_size):
        log.debug("%s: libzfs.Volume.resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        uri = "%s/%s" % (sr, key)

        image_meta = {
            'virtual_size': new_size,
        }

        try:
            cls._resize(dbg, sr, key, new_size, image_meta)
            meta.ZFSMetadataHandler.update(dbg, uri, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)


class RAWVolume(Volume):

    @classmethod
    def _create(cls, dbg, sr, name, description, size, sharable, image_meta):
        log.debug("%s: libzfs.RAWVolume.create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))

        image_meta[meta.TYPE_TAG] = utils.get_vdi_type_by_uri(dbg, image_meta[meta.URI_TAG][0])

        image_name = "ZFS%s%s/%s%s" % (utils.POOL_PREFIX,
                                       utils.get_sr_uuid_by_uri(dbg, sr),
                                       utils.VDI_PREFIXES[image_meta[meta.TYPE_TAG]],
                                       image_meta[meta.UUID_TAG])

        zvol_size = utils.roundup(zfs_utils.VOLBLOCKSIZE, size)

        try:
            zfs_utils.zvol_create(dbg, image_name, zvol_size)
            meta.ZFSMetadataHandler.update(dbg, image_meta[meta.URI_TAG][0], image_meta)
            call(['ln', '-s', "/dev/zvol/%s" % image_name, "%s/%s/%s" % (utils.SR_PATH_PREFIX,
                                                                         utils.get_sr_uuid_by_uri(dbg, sr),
                                                                         image_meta[meta.UUID_TAG])])
        except Exception:
            try:
                zfs_utils.zvol_destroy(dbg, image_name)
            except Exception:
                pass
            finally:
                raise Volume_does_not_exist(image_meta[meta.UUID_TAG])

        return image_meta

    @classmethod
    def _resize(cls, dbg, sr, key, new_size, image_meta):
        log.debug("%s: libzfs.RAWVolume._resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        uri = "%s/%s" % (sr, key)
        image_name = "ZFS%s%s/%s%s" % (utils.POOL_PREFIX,
                                       utils.get_sr_uuid_by_uri(dbg, sr),
                                       utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)],
                                       key)

        new_zvol_size = utils.roundup(zfs_utils.VOLBLOCKSIZE, new_size)

        try:
            zfs_utils.zvol_set(dbg, image_name, 'volsize', new_zvol_size)
        except Exception:
            raise Volume_does_not_exist(key)


class QCOW2Volume(Volume):

    @classmethod
    def _create(cls, dbg, sr, name, description, size, sharable, image_meta):
        log.debug("%s: libzfs.QCOW2Volume._create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))

        image_meta[meta.TYPE_TAG] = utils.get_vdi_type_by_uri(dbg, image_meta[meta.URI_TAG][0])

        image_name = "ZFS%s%s/%s%s" % (utils.POOL_PREFIX,
                                       utils.get_sr_uuid_by_uri(dbg, sr),
                                       utils.VDI_PREFIXES[image_meta[meta.TYPE_TAG]],
                                       image_meta[meta.UUID_TAG])

        # TODO: Implement overhead calculation for QCOW2 format
        size = utils.validate_and_round_vhd_size(size)
        zvol_size = utils.roundup(zfs_utils.VOLBLOCKSIZE, utils.fullSizeVHD(size))

        #try:
        zfs_utils.zvol_create(dbg, image_name, zvol_size)
        meta.ZFSMetadataHandler.update(dbg, image_meta[meta.URI_TAG][0], image_meta)
        call(['ln', '-s', "/dev/zvol/%s" % image_name, "%s/%s/%s" % (utils.SR_PATH_PREFIX,
                                                                     utils.get_sr_uuid_by_uri(dbg, sr),
                                                                     image_meta[meta.UUID_TAG])])
        #except Exception:
        #    try:
        #        zfs_utils.zvol_destroy(dbg, image_name)
        #    except Exception:
        #        pass
        #    finally:
        #        raise Volume_does_not_exist(image_meta[meta.UUID_TAG])

        devnull = open(os.devnull, 'wb')
        call(["/usr/lib64/qemu-dp/bin/qemu-img",
              "create",
              "-f", image_meta[meta.TYPE_TAG],
              "/dev/zvol/%s" % image_name,
              str(size)], stdout=devnull, stderr=devnull)

        return image_meta

    @classmethod
    def _resize(cls, dbg, sr, key, new_size, image_meta):
        log.debug("%s: libzfs.QCOW2Volume._resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        # TODO: Implement overhead calculation for QCOW2 format
        new_size = utils.validate_and_round_vhd_size(new_size)
        new_zvol_size = utils.roundup(zfs_utils.VOLBLOCKSIZE, utils.fullSizeVHD(new_size))

        uri = "%s/%s" % (sr, key)
        image_name = "ZFS%s%s/%s%s" % (utils.POOL_PREFIX,
                                       utils.get_sr_uuid_by_uri(dbg, sr),
                                       utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)],
                                       key)

        try:
            zfs_utils.zvol_set(dbg, image_name, 'volsize', new_zvol_size)
        except Exception:
            raise Volume_does_not_exist(key)

        devnull = open(os.devnull, 'wb')
        call(["/usr/lib64/qemu-dp/bin/qemu-img",
              "resize",
              "/dev/zvol/%s" % image_name,
              str(new_size)], stdout=devnull, stderr=devnull)
