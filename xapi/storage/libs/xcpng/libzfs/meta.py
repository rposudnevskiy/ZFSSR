#!/usr/bin/env python

import fcntl

from xapi.storage.libs.xcpng.utils import call
from xapi.storage import log

from json import dumps, loads

from tinydb import TinyDB
from tinydb.storages import MemoryStorage

from xapi.storage.libs.xcpng.utils import get_sr_uuid_by_uri, SR_PATH_PREFIX
from xapi.storage.libs.xcpng.meta import MetadataHandler as _MetadataHandler_
from xapi.storage.libs.xcpng.meta import MetaDBOperations as _MetaDBOperations_

class MetaDBOperations(_MetaDBOperations_):

    def __init__(self):
        self.lh = None

    def create(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.meta.MetaDBOpeations.create: uri: %s" % (dbg, uri))
        db = TinyDB(storage=MemoryStorage, default_table='sr')
        self.dump(dbg, uri, db)

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.meta.MetaDBOpeations.destroy: uri: %s" % (dbg, uri))
        call(dbg, ['rm', '-f', "%s/%s/__meta__" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri))])

    def load(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.meta.MetaDBOpeations.load: uri: %s" % (dbg, uri))
        fd = open("%s/%s/__meta__" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri)), 'r')
        db = TinyDB(storage=MemoryStorage, default_table='sr')
        db._storage.write(loads(fd.read()))
        fd.close()
        return db

    def dump(self, dbg, uri, db):
        log.debug("%s: xcpng.libzfs.meta.MetaDBOpeations.dump: uri: %s" % (dbg, uri))
        fd = open("%s/%s/__meta__" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri)), 'w')
        fd.write(dumps(db._storage.read()))
        fd.close()

    def lock(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.meta.MetaDBOpeations.lock: uri: %s" % (dbg, uri))
        self.lh = open("%s/%s/__lock__" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri)), 'w')
        fcntl.flock(self.lh, fcntl.LOCK_EX)

    def unlock(self, dbg, uri):
        log.debug("%s: xcpng.libzfs.meta.MetaDBOpeations.unlock: uri: %s" % (dbg, uri))
        fcntl.flock(self.lh, fcntl.LOCK_UN)
        self.lh.close()

class MetadataHandler(_MetadataHandler_):

    def __init__(self):
        self.MetaDBOpsHendler = MetaDBOperations()
