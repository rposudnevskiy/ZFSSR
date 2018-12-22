#!/usr/bin/env python

from __future__ import division

import os.path
import sys
import copy
import json
import platform

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import SR_skeleton, Sr_not_attached, Volume_does_not_exist,\
        SR_commandline, Unimplemented
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.volume import SR_skeleton, Sr_not_attached, Volume_does_not_exist,\
        SR_commandline, Unimplemented

from xapi.storage import log

from xapi.storage.libs.libzfs import utils, zfs_utils, meta

from subprocess import call, Popen, PIPE, check_output


class Implementation(SR_skeleton):

    def probe(self, dbg, configuration):
        log.debug("{}: SR.probe: configuration={}".format(dbg, configuration))

        uri="zfs+%s://" % configuration[meta.IMAGE_FORMAT_TAG] if meta.IMAGE_FORMAT_TAG in configuration else 'zfs://'
        _uri_ = uri
        uri="%s/%s" % (uri, configuration[meta.SR_UUID_TAG]) if meta.SR_UUID_TAG in configuration else uri

        log.debug("{}: SR.probe: uri to probe: {}".format(dbg, uri))

        result = []

        log.debug("%s: SR.probe: Available Pools" % dbg)
        log.debug("%s: SR.probe: ---------------------------------------------------" % dbg)

        pools = zfs_utils.pool_list(dbg)

        for pool in pools:
            log.debug("%s: SR.probe: %s" % (dbg, pool))

            pool_meta = {}
            sr_uuid = utils.get_sr_uuid_by_name(dbg, pool)


            if pool.startswith("%s%s" % ('ZFS', utils.POOL_PREFIX)):
                zfs_utils.pool_import(dbg, pool)
                pool_meta = meta.ZFSMetadataHandler.load(dbg, "%s%s" % (_uri_, sr_uuid))

                if (meta.IMAGE_FORMAT_TAG in configuration and
                        ((meta.CONFIGURATION_TAG in pool_meta and
                            meta.IMAGE_FORMAT_TAG in pool_meta[meta.CONFIGURATION_TAG] and
                            configuration[meta.IMAGE_FORMAT_TAG] != pool_meta[meta.CONFIGURATION_TAG][meta.IMAGE_FORMAT_TAG]) or
                         (meta.CONFIGURATION_TAG in pool_meta and
                            meta.IMAGE_FORMAT_TAG not in pool_meta[meta.CONFIGURATION_TAG]) or
                         meta.CONFIGURATION_TAG not in pool_meta)):

                    pool = None

                if (meta.SR_UUID_TAG in configuration and
                        ((meta.CONFIGURATION_TAG in pool_meta and
                          meta.SR_UUID_TAG in pool_meta[meta.CONFIGURATION_TAG] and
                              configuration[meta.SR_UUID_TAG] != pool_meta[meta.CONFIGURATION_TAG][
                              meta.SR_UUID_TAG]) or
                         (meta.CONFIGURATION_TAG in pool_meta and
                              meta.SR_UUID_TAG not in pool_meta[meta.CONFIGURATION_TAG] and
                              configuration[meta.SR_UUID_TAG] != sr_uuid) or
                         (meta.CONFIGURATION_TAG not in pool_meta and
                              configuration[meta.SR_UUID_TAG] != sr_uuid))):

                    pool = None

                if pool is not None:

                    _result_ = {}
                    _result_['complete'] = True
                    _result_['configuration'] = {}
                    _result_['configuration'] = copy.deepcopy(configuration)
                    _result_['extra_info'] = {}


                    sr = {}
                    sr['sr'] = "zfs://%s" % sr_uuid
                    sr['name'] = pool_meta[meta.NAME_TAG] if meta.NAME_TAG in pool_meta else '<ZFS Zvol SR>'
                    sr['description'] = pool_meta[meta.DESCRIPTION_TAG] if meta.DESCRIPTION_TAG in pool_meta else '<ZFS Zvol SR>'

                    sr['free_space'] = int(zfs_utils.pool_get(dbg, pool, 'free'))
                    sr['total_space'] = int(zfs_utils.pool_get(dbg, pool, 'size'))
                    sr['datasources'] = []
                    sr['clustered'] = False
                    sr['health'] = ['Healthy', '']

                    _result_['sr'] = sr
                    _result_['configuration']['sr_uuid'] = sr_uuid

                    result.append(_result_)

                zfs_utils.pool_export(dbg, pool)

        return result

    def create(self, dbg, sr_uuid, configuration, name, description):
        log.debug("%s: SR.create: sr_uuid %s configuration %s name '%s' description: '%s'" % (dbg, sr_uuid, configuration,
                                                                                          name, description))

        uri = "zfs+%s+%s://%s" % (configuration['image-format'],
                                  configuration['datapath'],
                                  sr_uuid)

        pool_name = utils.get_pool_name_by_uri(dbg, uri)
        vdev=configuration['vdev']

        if not vdev:
            log.debug("%s: SR.create: Failed to create SR - sr_uuid: %s. Device name[s] is/are not specified" % (dbg, sr_uuid))
            raise Exception("Failed to create ZFS pool %s. Device name[s] is/are not specified" % pool_name)

        #if ceph_cluster.pool_exists(ceph_pool_name):
        #    raise Exception("Pool %s already exists" % ceph_pool_name)

        try:
            zfs_utils.pool_create(dbg, pool_name, vdev, mountpoint="%s/%s" % (utils.SR_PATH_PREFIX, sr_uuid))
        except Exception:
            log.debug("%s: SR.create: Failed to create SR - sr_uuid: %s" % (dbg, sr_uuid))
            raise Exception("Failed to create ZFS pool %s" % pool_name)

        try:
            meta.ZFSMetadataHandler.create(dbg, uri)
        except Exception:
            try:
                zfs_utils.pool_destroy(dbg, pool_name)
            except Exception:
                raise Exception
            log.debug("%s: SR.create: Failed to create SR metadata - sr_uuid: %s" % (dbg, sr_uuid))
            raise Exception("Failed to create pool metadata %s" % pool_name)

        configuration['sr_uuid'] = sr_uuid

        pool_meta = {
            meta.SR_UUID_TAG: sr_uuid,
            meta.NAME_TAG: name,
            meta.DESCRIPTION_TAG: description,
            meta.CONFIGURATION_TAG: json.dumps(configuration)
        }

        try:
            meta.ZFSMetadataHandler.update(dbg, uri, pool_meta)
        except Exception:
            raise Exception("Failed to update pool metadata %s" % pool_name)

        zfs_utils.pool_export(dbg, pool_name)

        return configuration

    def destroy(self, dbg, uri):
        log.debug("%s: SR.destroy: uri: %s" % (dbg, uri))

        pool_name = utils.get_pool_name_by_uri(dbg, uri)

#        if not ceph_cluster.pool_exists(ceph_pool_name):
#            raise Exception("Ceph pool %s does not exist ")

        try:
            zfs_utils.pool_destroy(dbg, pool_name)
            call(['rm', '-rf', "%s/%s" % (utils.SR_PATH_PREFIX,
                                          utils.get_sr_uuid_by_uri(dbg, uri))])
        except Exception:
            raise Exception("Failed to destroy ZFS pool %s" % pool_name)

    def attach(self, dbg, configuration):
        log.debug("%s: SR.attach: configuration: %s" % (dbg, configuration))

        uri = "zfs+%s+%s://%s" % (configuration['image-format'],
                                     configuration['datapath'],
                                     configuration['sr_uuid'])

        log.debug("%s: SR.attach: sr_uuid: %s uri: %s" % (dbg, configuration['sr_uuid'], uri))

        pool_name = utils.get_pool_name_by_uri(dbg, uri)

        #if not ceph_cluster.pool_exists(utils.get_pool_name_by_uri(dbg, uri)):
        #    raise Sr_not_attached(configuration['sr_uuid'])

        try:
            zfs_utils.pool_import(dbg, pool_name)
        except Exception:
            raise Sr_not_attached(configuration['sr_uuid'])

        # Create pool metadata image if it doesn't exist
        #log.debug("%s: SR.attach: name: %s/%s" % (dbg, utils.get_pool_name_by_uri(dbg, uri), utils.SR_METADATA_IMAGE_NAME))
        #if not rbd_utils.if_image_exist(dbg, ceph_cluster, '%s/%s' % (utils.get_pool_name_by_uri(dbg, uri), utils.SR_METADATA_IMAGE_NAME)):
        #    rbd_utils.create(dbg, ceph_cluster, '%s/%s' % (utils.get_pool_name_by_uri(dbg, uri), utils.SR_METADATA_IMAGE_NAME), 0)

        return uri

    def detach(self, dbg, uri):
        log.debug("%s: SR.detach: uri: %s" % (dbg, uri))

        #sr_uuid=utils.get_sr_uuid_by_uri(dbg,uri)
        #log.debug("%s: SR.detach: sr_uuid: %s" % (dbg, sr_uuid))

        pool_name = utils.get_pool_name_by_uri(dbg, uri)
        sr_uuid = utils.get_sr_uuid_by_uri(dbg, uri)

        #if not ceph_cluster.pool_exists(utils.get_pool_name_by_uri(dbg, uri)):
        #    raise Sr_not_attached(sr_uuid)

        try:
            zfs_utils.pool_export(dbg, pool_name)
            call(['unlink', "%s/%s" % (utils.SR_PATH_PREFIX, sr_uuid)])

        except Exception:
            raise Exception("Failed to detach ZFS pool %s" % pool_name)

    def stat(self, dbg, uri):
        log.debug("%s: SR.stat: uri: %s" % (dbg, uri))

        pool_name = utils.get_pool_name_by_uri(dbg, uri)

        pool_meta = meta.ZFSMetadataHandler.load(dbg, uri)

        log.debug("%s: SR.stat: pool_meta: %s" % (dbg, pool_meta))

        # Get the sizes
        tsize = int(zfs_utils.pool_get(dbg, pool_name, 'size'))
        fsize = int(zfs_utils.pool_get(dbg, pool_name, 'free'))
        log.debug("%s: SR.stat total_space = %Ld free_space = %Ld" % (dbg, tsize, fsize))

        overprovision = 0

        return {
            'sr': uri,
            'uuid': utils.get_sr_uuid_by_uri(dbg, uri),
            'name': pool_meta[meta.NAME_TAG] if meta.NAME_TAG in pool_meta else '<ZFS Zvol SR>',
            'description': pool_meta[meta.DESCRIPTION_TAG] if meta.DESCRIPTION_TAG in pool_meta else '<ZFS Zvol SR>',
            'total_space': tsize,
            'free_space': fsize,
            'overprovision': overprovision,
            'datasources': [],
            'clustered': False,
            'health': ['Healthy', '']
        }

    def set_name(self, dbg, uri, new_name):
        log.debug("%s: SR.set_name: SR: %s New_name: %s"
                  % (dbg, uri, new_name))

        pool_meta = {
            meta.NAME_TAG: new_name,
        }

        try:
            meta.ZFSMetadataHandler.update(dbg, uri, pool_meta)
        except Exception:
            raise Volume_does_not_exist(uri)

    def set_description(self, dbg, uri, new_description):
        log.debug("%s: SR.set_description: SR: %s New_description: %s"
                  % (dbg, uri, new_description))

        pool_meta = {
            meta.DESCRIPTION_TAG: new_description,
        }

        try:
            meta.ZFSMetadataHandler.update(dbg, uri, pool_meta)
        except Exception:
            raise Volume_does_not_exist(uri)

    def ls(self, dbg, uri):
        log.debug("%s: SR.ls: uri: %s" % (dbg, uri))
        results = []
        key = ''

        pool_name = utils.get_pool_name_by_uri(dbg, uri)

        try:
            zvols = zfs_utils.zvol_list(dbg, pool_name)
            for zvol in zvols:
                if zvol.startswith(utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)]):
                    log.debug("%s: SR.ls: SR: %s zvol: %s" % (dbg, uri, zvol))

                    key = utils.get_vdi_uuid_by_name(dbg, zvol)

                    log.debug("%s: SR.ls: SR: %s Image: %s" % (dbg, uri, key))

                    image_meta = meta.ZFSMetadataHandler.load(dbg, "%s/%s" % (uri, key))
                    #log.debug("%s: SR.ls: SR: %s image: %s Metadata: %s" % (dbg, uri, rbd, image_meta))

                    results.append({
                            meta.UUID_TAG: image_meta[meta.UUID_TAG],
                            meta.KEY_TAG: image_meta[meta.KEY_TAG],
                            meta.NAME_TAG: image_meta[meta.NAME_TAG],
                            meta.DESCRIPTION_TAG: image_meta[meta.DESCRIPTION_TAG],
                            meta.READ_WRITE_TAG: image_meta[meta.READ_WRITE_TAG],
                            meta.VIRTUAL_SIZE_TAG: image_meta[meta.VIRTUAL_SIZE_TAG],
                            meta.PHYSICAL_UTILISATION_TAG: image_meta[meta.PHYSICAL_UTILISATION_TAG],
                            meta.URI_TAG: image_meta[meta.URI_TAG],
                            meta.CUSTOM_KEYS_TAG: image_meta[meta.CUSTOM_KEYS_TAG],
                            meta.SHARABLE_TAG: image_meta[meta.SHARABLE_TAG]
                    })
                #log.debug("%s: SR.ls: Result: %s" % (dbg, results))
            return results
        except Exception:
            raise Volume_does_not_exist(key)


if __name__ == "__main__":
    log.log_call_argv()
    cmd = SR_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == 'SR.probe':
        cmd.probe()
    elif base == 'SR.attach':
        cmd.attach()
    elif base == 'SR.create':
        cmd.create()
    elif base == 'SR.destroy':
        cmd.destroy()
    elif base == 'SR.detach':
        cmd.detach()
    elif base == 'SR.ls':
        cmd.ls()
    elif base == 'SR.stat':
        cmd.stat()
    elif base == 'SR.set_name':
        cmd.set_name()
    elif base == 'SR.set_description':
        cmd.set_description()
    else:
        raise Unimplemented(base)

