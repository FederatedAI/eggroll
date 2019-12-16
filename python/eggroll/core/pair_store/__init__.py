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
from eggroll.core.pair_store.adapter import FileAdapter, MmapAdapter, CacheAdapter

STORE_TYPE_LMDB = "rollpair.lmdb"
STORE_TYPE_ROCKSDB = "rollpair.leveldb"
STORE_TYPE_FILE = "rollpair.file"
STORE_TYPE_MMAP = "rollpair.mmap"
STORE_TYPE_CACHE = "rollpair.cache"

def create_pair_adapter(options: dict):
    ret = None
    # TODO:0: rename type name?
    if options["store_type"] == STORE_TYPE_LMDB:
      from eggroll.core.pair_store.lmdb import LmdbAdapter
      ret = LmdbAdapter(options=options)
    elif options["store_type"] == STORE_TYPE_ROCKSDB:
      from eggroll.core.pair_store.rocksdb import RocksdbAdapter
      ret = RocksdbAdapter(options=options)
    elif options["store_type"] == STORE_TYPE_FILE:
        ret = FileAdapter(options=options)
    elif options["store_type"] == STORE_TYPE_MMAP:
        ret = MmapAdapter(options=options)
    elif options["store_type"] == STORE_TYPE_CACHE:
        ret = CacheAdapter(options=options)
    else:
        raise NotImplementedError(options)
    return ret
