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
from eggroll.core.conf_keys import RollPairConfKeys
from eggroll.core.constants import StoreTypes
from eggroll.core.pair_store.adapter import FileAdapter, MmapAdapter, \
    CacheAdapter
from eggroll.utils.log_utils import get_logger

L = get_logger()


def create_pair_adapter(options: dict):
    ret = None

    replica_count = int(RollPairConfKeys.EGGROLL_ROLLPAIR_STORAGE_REPLICA_COUNT.get_with(options))
    is_replication_enabled = bool(RollPairConfKeys.EGGROLL_ROLLPAIR_STORAGE_REPLICATE_ENABLED.get_with(options))
    L.info(f"options={options}")
    L.info(f"is_replication_enabled={is_replication_enabled} replica_count={replica_count}")
    # TODO:0: rename type name?
    if is_replication_enabled and replica_count > 1:
        from eggroll.core.pair_store.ha_adapter import HaAdapter
        ret = HaAdapter(options)
        L.info(f"return HaAdapter={ret}")
    elif options["store_type"] == StoreTypes.ROLLPAIR_IN_MEMORY:
        actual_store_type = RollPairConfKeys.EGGROLL_ROLLPAIR_DEFAULT_STORE_TYPE.get()
        if actual_store_type == "ROLLPAIR_IN_MEMORY":
            raise ValueError('default store type cannot be IN_MEMORY')
        duplicate_options = options.copy()
        duplicate_options["store_type"] = getattr(StoreTypes, actual_store_type)
        ret = create_pair_adapter(options=duplicate_options)
    elif options["store_type"] == StoreTypes.ROLLPAIR_LMDB:
      from eggroll.core.pair_store.lmdb import LmdbAdapter
      ret = LmdbAdapter(options=options)
      L.info(f"return LmdbAdapter={ret}")
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
