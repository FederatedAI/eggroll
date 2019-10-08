#  Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import grpc
import threading
from eggroll.api.proto import kv_pb2, kv_pb2_grpc
from eggroll.api.utils import log_utils
from eggroll.api.utils import eggroll_serdes
log_utils.setDirectory()
LOGGER = log_utils.getLogger()

try:
    import rocksdb
except:
    LOGGER.info("WRAN: failed to import rocksdb")

try:
    import lmdb
except:
    LOGGER.info("WRAN: failed to import lmdb")

LMDB_MAP_SIZE = 16 * 4_096 * 244_140        # follows storage-service-cxx's config here
DEFAULT_DB = b'main'
DELIMETER = '-'
DELIMETER_ENCODED = DELIMETER.encode()

class AdapterManager:
    pass

class SkvAdapter(object):
    """
    Sorted key value store adapter
    """
    def __init__(self, options):
        pass

    def __del__(self):
        pass

    def close(self):
        raise NotImplementedError()

    def iteritems(self):
        raise NotImplementedError()

    def new_batch(self):
        raise NotImplementedError()

    def get(self, key):
        raise NotImplementedError()

    def put(self, key, value):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class SkvWriteBatch:
    def put(self, k, v):
        raise NotImplementedError()

    def write(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class SkvIterator:
    def close(self):
        raise NotImplementedError()

    def __iter__(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class LmdbIterator(SkvIterator):
    def __init__(self, adapter, cursor):
        LOGGER.info("create lmdb iterator")
        self.adapter = adapter
        self.cursor = cursor
    
    #move cursor to the first key position
    #return True if success or False if db is empty
    def first(self):
        return self.cursor.first()
    
    #same as first() but last key position
    def last(self):
        return self.cursor.last()
    
    #return the current key
    def key(self):
        return self.cursor.key()

    def close(self):
        pass

    def __iter__(self):
        return self.cursor.__iter__()

class LmdbWriteBatch(SkvWriteBatch):

    def __init__(self, adapter, txn):
        self.adapter = adapter
        self.txn = txn

    def put(self, k, v):
        self.txn.put(k, v)

    def delete(self, k, v):
        self.txn.delete(k, v)

    def write(self):
        pass

    def close(self):
        pass


class LmdbAdapter(SkvAdapter):
    env_lock = threading.Lock()
    env_dict = dict()
    count_dict = dict()
    sub_env_dict = dict()
    txn_dict = dict()

    def get(self, key):
        return self.cursor.get(key)

    def put(self, key, value):
        return self.txn.put(key, value)

    def __init__(self, options):
        with LmdbAdapter.env_lock:
            LOGGER.info("lmdb adapter init")
            super().__init__(options)
            self.path = options["path"]
            create_if_missing = bool(options.get("create_if_missing", "True"))
            if self.path not in LmdbAdapter.env_dict:
                if create_if_missing:
                    os.makedirs(self.path, exist_ok=True)
                LOGGER.info("path not in dict db path:{}".format(self.path))
                self.env = lmdb.open(self.path, create=create_if_missing, max_dbs=128, sync=False, map_size=LMDB_MAP_SIZE, writemap=True)
                self.sub_db = self.env.open_db(DEFAULT_DB)
                self.txn = self.env.begin(db=self.sub_db, write=True)
                LmdbAdapter.count_dict[self.path] = 0
                LmdbAdapter.env_dict[self.path] = self.env
                LmdbAdapter.sub_env_dict[self.path] = self.sub_db
                LmdbAdapter.txn_dict[self.path] = self.txn
            else:
                LOGGER.info("path in dict:{}".format(self.path))
                self.env = LmdbAdapter.env_dict[self.path]
                self.sub_db = LmdbAdapter.sub_env_dict[self.path]
                self.txn = LmdbAdapter.txn_dict[self.path]
            self.cursor = self.txn.cursor()
            LmdbAdapter.count_dict[self.path] = LmdbAdapter.count_dict[self.path] + 1
            
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        with LmdbAdapter.env_lock:
            if self.env:
                count = LmdbAdapter.count_dict[self.path]
                if not count or count - 1 <= 0:
                    LmdbAdapter.txn_dict[self.path].commit()
                    LmdbAdapter.env_dict[self.path].close()
                    del LmdbAdapter.txn_dict[self.path]
                    del LmdbAdapter.env_dict[self.path]
                    del LmdbAdapter.sub_env_dict[self.path]
                    del LmdbAdapter.count_dict[self.path]
                else:
                    LmdbAdapter.count_dict[self.path] = count - 1
                self.env = None

    def __del__(self): 
        with LmdbAdapter.env_lock:
            if self.env:
                count = LmdbAdapter.count_dict[self.path]
                if not count or count - 1 <= 0:
                    del LmdbAdapter.env_dict[self.path]
                    del LmdbAdapter.sub_env_dict[self.path]
                    del LmdbAdapter.txn_dict[self.path]
                    del LmdbAdapter.count_dict[self.path]
                else:
                    LmdbAdapter.count_dict[self.path] = count - 1

    def get_sub_db(self):
        return self.env.open_db(DEFAULT_DB)

    def close(self):
        self.cursor.close()

    def iteritems(self):
        return LmdbIterator(self, self.cursor)

    def new_batch(self):
        return LmdbWriteBatch(self, self.txn)


class RocksdbWriteBatch(SkvWriteBatch):
    def __init__(self, adapter, chunk_size=100000):
        self.chunk_size = chunk_size
        self.batch = rocksdb.WriteBatch()
        self.adapter = adapter
        self.key = None
        self.value = None
        self.serde = None

    def put(self, k, v):
        from eggroll.api.utils import eggroll_serdes  
        self.serde = eggroll_serdes.get_serdes()
        self.key = k
        self.value = v
        self.batch.put(k, v)
        self.write()
    
    def delete(self, k, v):
        self.adapter.db.delete(k, v)

    def write(self):
        self.adapter.db.write(self.batch)
        self.batch.clear()

    def close(self):
        if self.batch:
            self.write()
            del self.batch
            self.batch = None

class RocksdbIterator(SkvIterator):
    def __init__(self, adapter):
        self.adapter = adapter
        self.it = adapter.db.iteritems()
        self.it.seek_to_first()
    
    def first(self):
        count = 0
        self.it.seek_to_first()
        for k, v in self.it:
            count += 1
        self.it.seek_to_first()
        return (count != 0)

    def last(self):
        count = 0
        self.it.seek_to_last()
        for k, v in self.it:
            count += 1
        self.it.seek_to_last()
        return (count != 0)

    def key(self):
        return self.it.get()[0]

    def close(self):
        pass

    def __iter__(self):
        return self.it

class RocksdbAdapter(SkvAdapter):
    env_lock = threading.Lock()
    env_dict = dict()
    count_dict = dict()

    def __init__(self, options):
        """
        :param options:
            path: absolute local fs path
            create_if_missing: default true
        """
        with RocksdbAdapter.env_lock:
            super().__init__(options)
            self.path = options["path"]
            opts = rocksdb.Options()
            opts.create_if_missing = bool(options.get("create_if_missing", "True"))
            opts.compression = rocksdb.CompressionType.no_compression
            if self.path not in RocksdbAdapter.env_dict:
                if opts.create_if_missing:
                    os.makedirs(self.path, exist_ok=True)
                self.db = rocksdb.DB(self.path, opts)
                LOGGER.info("path not in dict db path:{}".format(self.path))
                RocksdbAdapter.count_dict[self.path] = 0
                RocksdbAdapter.env_dict[self.path] = self.db
            else:
                LOGGER.info("path in dict:{}".format(self.path))
                self.db = RocksdbAdapter.env_dict[self.path]
        RocksdbAdapter.count_dict[self.path] = RocksdbAdapter.count_dict[self.path] + 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        LOGGER.info("exit")
        with RocksdbAdapter.env_lock:
            if self.db:
                count = RocksdbAdapter.count_dict[self.path]
                if not count or count - 1 <= 0:
                    del RocksdbAdapter.env_dict[self.path]
                    del RocksdbAdapter.count_dict[self.path]
                else:
                    RocksdbAdapter.count_dict[self.path] = RocksdbAdapter.count_dict[self.path] - 1
        self.close()

    def __del__(self):
        with RocksdbAdapter.env_lock:
            if self.db:
                count = RocksdbAdapter.count_dict[self.path]
                if not count or count - 1 <= 0:
                    del RocksdbAdapter.env_dict[self.path]
                    del RocksdbAdapter.count_dict[self.path]
                else:
                    RocksdbAdapter.count_dict[self.path] = count - 1

    def get(self, key):
        return self.db.get(key)

    def close(self):
        if self.db:
            del self.db

    def iteritems(self):
        return RocksdbIterator(self)

    def new_batch(self):
        return RocksdbWriteBatch(self)

class SkvNetworkWriteBatch(SkvWriteBatch):

    def __init__(self, adapter):
        self.adapter = adapter
        self.kv_stub = self.adapter.kv_stub
        self.cache = []

    def write(self):
        if self.cache:
            self.kv_stub.putAll(iter(self.cache), metadata=self.adapter.get_stream_meta())
            self.cache.clear()

    def close(self):
        # write last
        self.write()

    def put(self, k, v):
        self.cache.append(kv_pb2.Operand(key=k, value=v))
        if len(self.cache) > 100000:
            self.write()


class SkvNetworkIterator(SkvIterator):
    def __init__(self,adapter):
        self.adapter = adapter
        self.kv_stub = adapter.kv_stub
        self._start = None
        self._end = None
        self._min_chunk_size = 0
        self._cache = None
        self._index = 0
        self._next_item = None
    def close(self):
        pass

    def __iter__(self):
        return self

    def _fetch(self):
        start = self._start if self._next_item is None else self._next_item.key
        self._cache = list(self.kv_stub.iterate(
            kv_pb2.Range(start=start, end=self._end, minChunkSize=self._min_chunk_size),
            metadata=self.adapter.get_stream_meta()))
        if len(self._cache) == 0:
            raise StopIteration
        self._index = 0

    def __next__(self):
        if self._cache is None or self._index >= len(self._cache):
            self._fetch()
        self._next_item = self._cache[self._index]
        self._index += 1
        return self._next_item.key, self._next_item.value

class SkvNetworkAdapter(SkvAdapter):
    # options:
    #   host
    #   port
    #   store_type
    #   name
    #   namespace
    #   fragment

    def get_stream_meta(self):
        return ('store_type', self.options["store_type"]),\
               ('table_name', self.options["name"]), \
               ('name_space', self.options["namespace"]), \
               ('fragment', self.options["fragment"])

    def __init__(self, options):
        super().__init__(options)
        self.options = options
        host = options["host"]
        port = options["port"]
        self.channel = grpc.insecure_channel(target="{}:{}".format(host, port),
                                             options=[('grpc.max_send_message_length', -1),
                                                      ('grpc.max_receive_message_length', -1)])
        self.kv_stub = kv_pb2_grpc.KVServiceStub(self.channel)

    def close(self):
        self.channel.close()

    def iteritems(self):
        return SkvNetworkIterator(self)

    def new_batch(self):
        return SkvNetworkWriteBatch(self)

    def get(self, key):
        item = self.kv_stub.get(kv_pb2.Operand(key=key))
        return item.value

    def put(self, key, value):
        item = kv_pb2.Operand(key=key, value=value)
        self.kv_stub.put(item)
