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
from eggroll.core.constants import StoreTypes
from eggroll.core.pair_store.adapter import FileAdapter, MmapAdapter, CacheAdapter
from eggroll.core.conf_keys import RollPairConfKeys


def create_pair_adapter(options: dict):
    ret = None
    # TODO:0: rename type name?
    if options["store_type"] == StoreTypes.ROLLPAIR_IN_MEMORY:
        actual_store_type = RollPairConfKeys.EGGROLL_ROLLPAIR_DEFAULT_STORE_TYPE.get()
        if actual_store_type == "ROLLPAIR_IN_MEMORY":
            raise ValueError('default store type cannot be IN_MEMORY')
        duplicate_options = options.copy()
        duplicate_options["store_type"] = getattr(StoreTypes, actual_store_type)
        ret = create_pair_adapter(options=duplicate_options)
    elif options["store_type"] == StoreTypes.ROLLPAIR_LMDB:
      from eggroll.core.pair_store.lmdb import LmdbAdapter
      ret = LmdbAdapter(options=options)
    elif options["store_type"] == StoreTypes.ROLLPAIR_LEVELDB:
      from eggroll.core.pair_store.rocksdb import RocksdbAdapter
      ret = RocksdbAdapter(options=options)
    elif options["store_type"] == StoreTypes.ROLLFRAME_FILE:
        ret = FileAdapter(options=options)
    elif options["store_type"] == StoreTypes.ROLLPAIR_MMAP:
        ret = MmapAdapter(options=options)
    elif options["store_type"] == StoreTypes.ROLLPAIR_CACHE:
        ret = CacheAdapter(options=options)
    elif options["store_type"] == StoreTypes.ROLLPAIR_ROLLSITE:
        from eggroll.core.pair_store.roll_site_adapter import RollSiteAdapter
        ret = RollSiteAdapter(options=options)
    elif options["store_type"] == StoreTypes.ROLLPAIR_QUEUE:
        from eggroll.core.pair_store.queue_adapter import QueueAdapter
        ret = QueueAdapter(options=options)
    else:
        raise NotImplementedError(options)
    return ret
