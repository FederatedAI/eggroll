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
#
#
from eggroll.core.conf_keys import RollPairConfKeys
from eggroll.core.constants import StoreTypes
from eggroll.core.pair_store import create_pair_adapter
from eggroll.core.pair_store.adapter import PairAdapter, PairIterator, \
    PairWriteBatch
from eggroll.core.pair_store.remote_adapter import RemoteRollPairAdapter
from eggroll.core.utils import get_runtime_storage, add_runtime_storage, \
    calculate_rank_in_node
from eggroll.utils.log_utils import get_logger

L = get_logger()


class HaAdapter(PairAdapter):
    is_ha_eggs_inited = False

    def __init__(self, options: dict = None):
        super().__init__(options)
        L.info(f"HaAdapter init calling")
        if options is None:
            options = {}
        self.options = options
        self.replica_count = max(int(RollPairConfKeys.EGGROLL_ROLLPAIR_STORAGE_REPLICA_COUNT.get_with(options)), 1)

        self.er_partition = options['er_partition']
        self.partition_id = self.er_partition._id
        self.total_partitions = self.er_partition._store_locator._total_partitions

        self.concrete_adapters = list()

        main_adapter_options = options.copy()
        main_adapter_options[RollPairConfKeys.EGGROLL_ROLLPAIR_STORAGE_REPLICATE_ENABLED.key] = False
        self.main_adapter = create_pair_adapter(main_adapter_options)
        self.path = self.main_adapter.path
        L.info(f"main_adapter={self.main_adapter}")
        self.concrete_adapters.append(self.main_adapter)

        self.init_ha_egg_order(self.er_partition._processor._server_node_id)
        ha_egg_list = get_runtime_storage("__ha_eggs_list")
        eggs = get_runtime_storage("__eggs")
        if self.er_partition._store_locator._store_type != StoreTypes.ROLLPAIR_IN_MEMORY \
                or RollPairConfKeys.EGGROLL_ROLLPAIR_STORAGE_REPLICATE_TEMP_FILES.get_with(options):
            for i in range(1, self.replica_count):
                replica_egg_id = (self.partition_id + i) % self.total_partitions
                if replica_egg_id == self.partition_id:
                    L.debug(f"replica_egg_id equals to self.partition_id={self.partition_id}. skipping replication from i={i}")
                    break
                L.info(f"replica_egg_id={replica_egg_id}")
                replica_egg_tuple = ha_egg_list[i % self.total_partitions]
                L.info(f"replica_egg_tuple={replica_egg_tuple}")
                if replica_egg_tuple[0] == self.er_partition._processor._server_node_id:
                    L.debug(f"replica_egg's processor._server_node_id equals to my processor._id={replica_egg_tuple[0]}. skipping replication from i={i}")
                    break

                rank_in_node = calculate_rank_in_node(
                        self.partition_id, len(eggs), len(replica_egg_tuple[1]))
                replica_processor = replica_egg_tuple[1][rank_in_node]
                L.info(f"origin processor={self.er_partition._processor}")
                L.info(f"replica processor={replica_processor}")

                replica_adapter_options = options.copy()
                replica_adapter_options['replica_number'] = i
                replica_adapter_options['replica_processor'] = replica_processor
                replica_adapter = RemoteRollPairAdapter(remote_cmd_endpoint=replica_processor._command_endpoint,
                                                        remote_transfer_endpoint=replica_processor._transfer_endpoint,
                                                        options=replica_adapter_options)
                self.concrete_adapters.append(replica_adapter)
                L.info(f"concrete_adapters len={len(self.concrete_adapters) } {self.concrete_adapters}")

    def init_ha_egg_order(self, origin_server_node_id):
        if HaAdapter.is_ha_eggs_inited:
            return
        L.info(f"origin_server_node_id={origin_server_node_id}, get_runtime_storage={get_runtime_storage()}")
        eggs_dict = get_runtime_storage("__eggs")
        eggs_list = sorted(list(eggs_dict.items()), key=lambda e: e[0])
        ha_eggs_list = None
        for i in range(len(eggs_list)):
            if eggs_list[i][0] == origin_server_node_id:
                ha_eggs_list = eggs_list[i:] + eggs_list[:i]
        add_runtime_storage("__ha_eggs_list", ha_eggs_list)
        L.info(f"__ha_eggs_list={get_runtime_storage('__ha_eggs_list')}")
        HaAdapter.is_ha_eggs_inited = True

    def __del__(self):
        super().__del__()

    def close(self):
        for adapter in self.concrete_adapters:
            adapter.close()

    def count(self):
        return self.main_adapter.count()

    def iteritems(self):
        return HaIterator(self)

    def new_batch(self):
        return HaWriteBatch(self, self.options)

    def get(self, key):
        return self.main_adapter.get(key)

    def put(self, key, value):
        for adapter in self.concrete_adapters:
            adapter.put(key, value)

    def is_sorted(self):
        return self.main_adapter.is_sorted()

    def destroy(self, options: dict = None):
        for adapter in self.concrete_adapters:
            adapter.destroy(options)

    def __enter__(self):
        result = None
        for adapter in self.concrete_adapters:
            if result is None:
                result = adapter.__enter__()
            else:
                adapter.__enter__()
        L.info(f"return result={result}")
        # return result
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)


# reads from main adapter only
class HaIterator(PairIterator):

    def __init__(self, adapter: HaAdapter):
        self.adapter = adapter
        self.main_adapter = adapter.main_adapter
        self.main_iterator = self.main_adapter.iteritems()

    def close(self):
        self.main_iterator.close()

    def __iter__(self):
        return self.main_iterator

    def __enter__(self):
        return self.main_iterator

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.main_iterator.__exit__(exc_type, exc_val, exc_tb)


class HaWriteBatch(PairWriteBatch):
    def __init__(self, adapter: HaAdapter, options: dict = None):
        if options is None:
            options = {}
        self.adapter = adapter
        self.main_adapter = self.adapter.main_adapter
        self.concrete_adapters = self.adapter.concrete_adapters
        self.concrete_adapters_count = len(self.concrete_adapters)
        self.concrete_write_batches = []
        # L.info(f"self.concrete_adapters={self.concrete_adapters} len={len(self.concrete_adapters)}")
        for adapter in self.concrete_adapters:
            # L.info(f"iterate adapter={adapter}")
            self.concrete_write_batches.append(adapter.new_batch())

        self.options = options
        L.info(f"HaWriteBatch inited")

    def get(self, k):
        exceptions = {}
        for i in range(self.concrete_adapters_count):
            try:
                return self.concrete_write_batches[i].get(k)
            except Exception as e:
                exceptions[i] = e
                if i == self.concrete_adapters_count - 1:
                    raise RuntimeError(f"cannot get k={k}. exceptions={exceptions}", e)
                else:
                    continue

    def put(self, k, v):
        L.info(f"HaWriteBatch put calling")
        for wb in self.concrete_write_batches:
            wb.put(k, v)

    def merge(self, merge_func, k, v):
        for wb in self.concrete_write_batches:
            wb.merge(merge_func, k, v)

    def write(self):
        for wb in self.concrete_write_batches:
            wb.write()

    def close(self):
        for wb in self.concrete_write_batches:
            wb.close()

    def __enter__(self):
        result = None
        for wb in self.concrete_write_batches:
            if result is None:
                result = wb.__enter__()
            else:
                wb.__enter__()
        # return result
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for i in range(self.concrete_adapters_count):
            wb = self.concrete_write_batches[self.concrete_adapters_count - 1 - i]
            wb.__exit__(exc_type, exc_val, exc_tb)
