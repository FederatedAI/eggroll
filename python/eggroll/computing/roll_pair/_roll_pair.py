# -*- coding: utf-8 -*-
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
import logging
import typing
from typing import Callable, Tuple, Iterable, List

from eggroll.computing import tasks
from eggroll.computing.tasks.store import StoreTypes
from eggroll.core.command.command_client import CommandClient
from eggroll.core.meta_model import (
    ErStore,
)
from eggroll.trace import roll_pair_method_trace

if typing.TYPE_CHECKING:
    from ._roll_pair_context import RollPairContext


L = logging.getLogger(__name__)

T = typing.TypeVar("T")


class RollPair(object):
    def __getstate__(self):
        raise NotImplementedError(
            "pickling RollPair is not expected behavior, if you really need it, please create an issue at"
        )

    def __init__(self, er_store: ErStore, rp_ctx: "RollPairContext", gc_enabled=None):
        self._ctx = rp_ctx
        self._store = er_store
        self._session_id = self.ctx.session_id
        self._command_client = CommandClient(config=self.ctx.session.config)

        if gc_enabled is None:
            gc_enabled = rp_ctx.is_rpc_gc_enabled
        self._is_gc_enabled = gc_enabled
        if self._store.store_locator.store_type != StoreTypes.ROLLPAIR_IN_MEMORY:
            self._is_gc_enabled = False

        if self._is_gc_enabled:
            rp_ctx.increase_store_gc_count(er_store)

        self._is_destroyed = False

    def register_gc(self):
        self._is_gc_enabled = True
        self.ctx.increase_store_gc_count(self._store)

    @property
    def is_destroyed(self):
        return self._is_destroyed

    @is_destroyed.setter
    def is_destroyed(self, value):
        self._is_destroyed = value

    @property
    def is_gc_enabled(self):
        return self._is_gc_enabled

    @property
    def command_client(self):
        return self._command_client

    @property
    def session_id(self):
        return self._session_id

    @property
    def config(self):
        return self.ctx.session.config

    @property
    def ctx(self):
        return self._ctx

    def __del__(self):
        if self.ctx.session.is_stopped():
            # when session stopped, gc_recorder will be cleared, so we just return.
            # notice that, when this happens, log will be disabled, so we can't log anything
            # L.exception(f"try to cleanup store={self._store} but session stopped")
            return
        if self.is_destroyed:
            L.debug(f"store={self._store} has been marked as destroyed before")
            return
        if not self.is_gc_enabled:
            L.debug(f"store={self._store} gc disabled, will not be cleaned up")
            return

        L.debug(
            f"{self} is being cleaned up, store reference of {self._store} will be decreased"
        )
        self.ctx.decrease_store_gc_count(self._store)

    def __repr__(self):
        return f"<{self.__class__.__name__}(_store={self._store}) at {hex(id(self))}>"

    def get_partitions(self):
        return self._store.num_partitions

    @property
    def num_partitions(self) -> int:
        return self._store.num_partitions

    def get_name(self):
        return self._store.store_locator.name

    def get_namespace(self):
        return self._store.store_locator.namespace

    def get_store(self):
        return self._store

    def get_store_type(self):
        return self._store.store_locator.store_type

    """
      storage api
    """

    @roll_pair_method_trace
    def get(self, k: bytes, partitioner: Callable[[bytes, int], int]):
        return tasks.Get.submit(self, k, partitioner)

    @roll_pair_method_trace
    def put(self, k, v, partitioner: Callable[[bytes, int], int]) -> bool:
        return tasks.Put.submit(self, k, v, partitioner)

    @roll_pair_method_trace
    def delete(self, k: bytes, partitioner: Callable[[bytes, int], int]) -> bool:
        return tasks.Delete.submit(self, k, partitioner)

    @roll_pair_method_trace
    def get_all(self, limit=None):
        return tasks.GetAll.submit(self, limit)

    @roll_pair_method_trace
    def put_all(
        self,
        kv_list: Iterable[Tuple[bytes, bytes]],
        partitioner: Callable[[bytes, int], int],
    ):
        return tasks.PutAll.submit(self, kv_list, partitioner)

    @roll_pair_method_trace
    def count(self) -> int:
        return tasks.Count.submit(self)

    @roll_pair_method_trace
    def take(self, num: int, options: dict = None):
        return tasks.Take.submit(self, num, options)

    @roll_pair_method_trace
    def destroy(self):
        if self.is_destroyed:
            L.exception(f"store={self._store} has been destroyed before")
            raise ValueError(f"store:{self.get_store()} has been destroyed before")
        response = tasks.Destroy.submit(self)
        self.is_destroyed = True
        return response

    def copy_as(self, name: str, namespace: str, store_type: str):
        key_serdes_type = self.get_store().key_serdes_type
        value_serdes_type = self.get_store().value_serdes_type
        partitioner_type = self.get_store().partitioner_type
        num_partitions = self.num_partitions
        return self.map_reduce_partitions_with_index(
            map_partition_op=lambda i, v: v,
            reduce_partition_op=None,
            shuffle=False,
            input_key_serdes=None,
            input_key_serdes_type=key_serdes_type,
            input_value_serdes=None,
            input_value_serdes_type=value_serdes_type,
            input_partitioner=None,
            input_partitioner_type=partitioner_type,
            output_key_serdes=None,
            output_key_serdes_type=key_serdes_type,
            output_value_serdes=None,
            output_value_serdes_type=value_serdes_type,
            output_partitioner=None,
            output_partitioner_type=partitioner_type,
            output_num_partitions=num_partitions,
            output_name=name,
            output_namespace=namespace,
            output_store_type=store_type,
        )

    """
        computing api
    """

    @roll_pair_method_trace
    def map_reduce_partitions_with_index(
        self,
        map_partition_op,
        reduce_partition_op,
        shuffle,
        input_key_serdes,
        input_key_serdes_type,
        input_value_serdes,
        input_value_serdes_type,
        input_partitioner,
        input_partitioner_type,
        output_key_serdes,
        output_key_serdes_type,
        output_value_serdes,
        output_value_serdes_type,
        output_partitioner,
        output_partitioner_type,
        output_num_partitions,
        output_namespace=None,
        output_name=None,
        output_store_type=None,
    ):
        return tasks.MapReducePartitionsWithIndex.submit(
            self,
            map_partition_op=map_partition_op,
            reduce_partition_op=reduce_partition_op,
            shuffle=shuffle,
            input_key_serdes=input_key_serdes,
            input_key_serdes_type=input_key_serdes_type,
            input_value_serdes=input_value_serdes,
            input_value_serdes_type=input_value_serdes_type,
            input_partitioner=input_partitioner,
            input_partitioner_type=input_partitioner_type,
            output_key_serdes=output_key_serdes,
            output_key_serdes_type=output_key_serdes_type,
            output_value_serdes=output_value_serdes,
            output_value_serdes_type=output_value_serdes_type,
            output_partitioner=output_partitioner,
            output_partitioner_type=output_partitioner_type,
            output_num_partitions=output_num_partitions,
            output_namespace=output_namespace,
            output_name=output_name,
            output_store_type=output_store_type,
        )

    @roll_pair_method_trace
    def reduce(self, func):
        return tasks.Reduce.reduce(self, func)

    @roll_pair_method_trace
    def binary_sorted_map_partitions_with_index(
        self,
        other: "RollPair",
        binary_map_partitions_with_index_op: Callable[
            [int, Iterable, Iterable], Iterable
        ],
        key_serdes,
        key_serdes_type,
        partitioner,
        partitioner_type,
        first_input_value_serdes,
        first_input_value_serdes_type,
        second_input_value_serdes,
        second_input_value_serdes_type,
        output_value_serdes,
        output_value_serdes_type,
    ):
        return tasks.BinarySortedMapPartitionsWithIndex.submit(
            left=self, right=other, func=binary_map_partitions_with_index_op
        )

    @roll_pair_method_trace
    def with_stores(
        self,
        func,
        others: List["RollPair"] = None,
        options: dict = None,
        description: str = None,
    ):
        return tasks.WithStores.submit(
            self, func, others=others, options=options, description=description
        )

    @roll_pair_method_trace
    def pull_get_header(self, tag: str, timeout: float, description: str = None):
        return tasks.PullGetHeader.submit(self, tag, timeout, description=description)

    @roll_pair_method_trace
    def pull_get_partition_status(
        self, tag: str, timeout: float, description: str = None
    ):
        return tasks.PullGetPartitionStatus.submit(
            self, tag, timeout, description=description
        )

    @roll_pair_method_trace
    def pull_clear_status(self, tag: str, description: str = None):
        return tasks.PullClearStatus.submit(self, tag, description=description)
