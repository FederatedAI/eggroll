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
import uuid
from typing import Callable, Iterable

from eggroll.computing import tasks
from eggroll.computing.tasks.store import StoreTypes
from eggroll.core.command.command_client import CommandClient
from eggroll.core.meta_model import (
    ErStoreLocator,
    ErStore,
    ErPartition,
)
from eggroll.session import ErSession
from ._gc import GcRecorder
from ._roll_pair import RollPair

L = logging.getLogger(__name__)


class RollPairContext(object):
    def __init__(self, session: ErSession):
        if not session.is_active():
            raise Exception(
                f"session_id={session.get_session_id()} is not ACTIVE. current status={session.get_session_meta().status}"
            )
        self._session = session
        self._session.add_exit_task(self.context_gc)
        self.rpc_gc_enable = True
        self.gc_recorder = GcRecorder(self)
        self._command_client = CommandClient(config=session.config)

        eggs = session.get_eggs()

    #     self._broadcast_eggs(eggs, session.get_eggs_count())
    #
    # def _broadcast_eggs(self, eggs, count):
    #     rp = self.create_rp(
    #         id=-1,
    #         name=self.session_id,
    #         namespace=f"er_session_meta",
    #         total_partitions=count,
    #         store_type=StoreTypes.ROLLPAIR_CACHE,
    #         key_serdes_type=0,
    #         value_serdes_type=0,
    #         partitioner_type=0,
    #         options={},
    #     )
    #
    #     def _bc_eggs(_data_dir, _task: ErTask):
    #
    #         # add_runtime_storage("__eggs", eggs)
    #
    #     rp.with_stores(func=_bc_eggs, description="broadcast eggs")

    @property
    def config(self):
        return self._session.config

    @property
    def session(self):
        return self._session

    @property
    def command_client(self):
        return self._command_client

    @property
    def session_id(self):
        return self._session.get_session_id()

    def context_gc(self):
        self.gc_recorder.stop()
        if (
            self.gc_recorder.gc_recorder is None
            or len(self.gc_recorder.gc_recorder) == 0
        ):
            return

        for (namespace, name), v in dict(self.gc_recorder.gc_recorder.items()).items():
            L.debug(f"gc: namespace={namespace}, name={name}, v={v}")
            # TODO: add api to check if store exists?
            rp = self.create_rp(
                id=-1,
                namespace=namespace,
                name=name,
                total_partitions=1,
                store_type=StoreTypes.ROLLPAIR_IN_MEMORY,
                key_serdes_type=0,
                value_serdes_type=0,
                partitioner_type=0,
                options={},
            )
            try:
                rp.destroy()
            except Exception as e:
                raise RuntimeError(f"fail to destroy store={rp.get_store()}, error={e}")

    def route_to_egg(self, partition: ErPartition):
        return self._session.route_to_egg(partition)

    def populate_processor(self, store: ErStore):
        return self._session.populate_processor(store)

    def create_rp(
        self,
        id,
        name: str,
        namespace: str,
        total_partitions: int,
        store_type: str,
        key_serdes_type: int,
        value_serdes_type: int,
        partitioner_type: int,
        options: dict,
    ):
        store = self.create_store(
            id=id,
            name=name,
            namespace=namespace,
            total_partitions=total_partitions,
            store_type=store_type,
            key_serdes_type=key_serdes_type,
            value_serdes_type=value_serdes_type,
            partitioner_type=partitioner_type,
            options=options,
        )
        return RollPair(store, self)

    def create_store(
        self,
        id,
        name: str,
        namespace: str,
        total_partitions: int,
        store_type: str,
        key_serdes_type: int,
        value_serdes_type: int,
        partitioner_type: int,
        options: dict,
    ):
        store = ErStore(
            store_locator=ErStoreLocator(
                id=id,
                store_type=store_type,
                namespace=namespace,
                name=name,
                total_partitions=total_partitions,
                key_serdes_type=key_serdes_type,
                value_serdes_type=value_serdes_type,
                partitioner_type=partitioner_type,
            ),
            partitions=[],
            options=options,
        )
        return self._session.get_or_create_store(store)

    def load_store(
        self,
        name: str,
        namespace: str,
        store_type: str,
    ):
        store = ErStore(
            store_locator=ErStoreLocator(
                store_type=store_type,
                namespace=namespace,
                name=name,
            ),
            options={},
        )
        result = self._session.cluster_manager_client.get_store(store)
        if result.num_partitions == 0:
            raise ValueError(f"store not found: {name}, {namespace}, {store_type}")
        return self.populate_processor(result)

    def load_rp(
        self,
        name: str,
        namespace: str,
        store_type: str,
    ):
        store = self.load_store(
            name=name,
            namespace=namespace,
            store_type=store_type,
        )
        return RollPair(store, self)

    def parallelize(
        self,
        data: Iterable,
        total_partitions: int,
        partitioner: Callable[[bytes], int],
        partitioner_type: int,
        key_serdes_type: int,
        value_serdes_type: int,
        store_type: str = StoreTypes.ROLLPAIR_IN_MEMORY,
        namespace=None,
        name=None,
    ):
        namespace = namespace or self.session_id
        name = name or str(uuid.uuid1())
        store = self.create_store(
            id=-1,
            name=name,
            namespace=namespace,
            store_type=store_type,
            total_partitions=total_partitions,
            key_serdes_type=key_serdes_type,
            value_serdes_type=value_serdes_type,
            partitioner_type=partitioner_type,
            options={},
        )
        rp = RollPair(store, self)
        return rp.put_all(data, partitioner)

    def cleanup(self, name, namespace, options: dict = None):
        tasks.Destroy.submit_cleanup(self, name, namespace, options)
