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

import uuid

from eggroll.core.client import CommandClient
from eggroll.core.command.command_model import CommandURI
from eggroll.core.conf_keys import RollPairConfKeys
from eggroll.core.constants import StoreTypes, PartitionerTypes
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErTask, ErPartition, ErJobIO
from eggroll.core.session import ErSession
from eggroll.core.utils import generate_job_id, generate_task_id, get_runtime_storage
from eggroll.roll_pair._roll_pair import RollPair
from eggroll.roll_pair._gc import GcRecorder
from typing import Callable, Iterable
import logging

L = logging.getLogger(__name__)


def runtime_init(session: ErSession):
    rpc = RollPairContext(session=session)
    return rpc


class RollPairContext(object):
    def __init__(self, session: ErSession):
        if not session.get_session_meta().is_active():
            raise Exception(
                f"session_id={session.get_session_id()} is not ACTIVE. current status={session.get_session_meta().status}"
            )
        self.__session = session
        self.session_id = session.get_session_id()
        default_store_type_str = RollPairConfKeys.EGGROLL_ROLLPAIR_DEFAULT_STORE_TYPE.get_with(
            session.get_all_options()
        )
        self.default_store_type = getattr(StoreTypes, default_store_type_str, None)
        if not self.default_store_type:
            raise ValueError(f'store type "{default_store_type_str}" not found for roll pair')
        self.in_memory_output = RollPairConfKeys.EGGROLL_ROLLPAIR_IN_MEMORY_OUTPUT.get_with(session.get_all_options())
        if not self.default_store_type:
            raise ValueError(f'in_memory_output "{self.in_memory_output}" not found for roll pair')

        self.__session_meta = session.get_session_meta()
        self.__session.add_exit_task(self.context_gc)
        self.rpc_gc_enable = True
        self.gc_recorder = GcRecorder(self)
        self.__command_client = CommandClient()

        eggs = session.get_eggs()
        self._broadcast_eggs(eggs, session.get_eggs_count())

    def _broadcast_eggs(self, eggs, count):
        rp = self.create_rp(
            id=-1,
            name=self.session_id,
            namespace=f"er_session_meta",
            total_partitions=count,
            store_type=StoreTypes.ROLLPAIR_CACHE,
            key_serdes_type=0,
            value_serdes_type=0,
            partitioner_type=0,
            options={},
        )

        def _bc_eggs(_data_dir, _task: ErTask):
            from eggroll.core.utils import add_runtime_storage

            add_runtime_storage("__eggs", eggs)
            L.debug(f"runtime_storage={get_runtime_storage('__eggs')}")

        rp.with_stores(func=_bc_eggs, description="broadcast eggs")

    def set_store_type(self, store_type: str):
        self.default_store_type = store_type

    def set_session_gc_enable(self):
        self.rpc_gc_enable = True

    def set_session_gc_disable(self):
        self.rpc_gc_enable = False

    def get_session(self):
        return self.__session

    def get_roll(self):
        ret = self.__session._rolls[0]
        if not ret._command_endpoint._host or not ret._command_endpoint._port:
            L.exception(f"invalid roll processor={ret}, session_meta={self.__session_meta}")
            raise ValueError(f"invalid roll endpoint={ret}")
        return ret

    def context_gc(self):
        self.gc_recorder.stop()
        if self.gc_recorder.gc_recorder is None or len(self.gc_recorder.gc_recorder) == 0:
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
        return self.__session.route_to_egg(partition)

    def populate_processor(self, store: ErStore):
        return self.__session.populate_processor(store)

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
            no_gc: bool = False,
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
        return RollPair(store, self, no_gc=no_gc)

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
        return self.__session.get_or_create_store(store)

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
        result = self.__session.cluster_manager_client.get_store(store)
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
        """store name only supports full name and reg: *, *abc ,abc* and a*c"""
        if not namespace:
            raise ValueError("namespace cannot be blank")
        L.debug(f"cleaning up namespace={namespace}, name={name}")
        if options is None:
            options = {}
        total_partitions = options.get("total_partitions", 1)

        if name == "*":
            store_type = options.get("store_type", "*")
            L.debug(f"cleaning up whole store_type={store_type}, namespace={namespace}, name={name}")
            er_store = ErStore(store_locator=ErStoreLocator(namespace=namespace, name=name, store_type=store_type))
            job_id = generate_job_id(namespace, tag=RollPair.CLEANUP)
            job = ErJob(id=job_id, name=RollPair.DESTROY, inputs=[ErJobIO(er_store)], options=options)

            args = list()
            cleanup_partitions = [ErPartition(id=-1, store_locator=er_store._store_locator)]

            for server_node, eggs in self.__session._eggs.items():
                egg = eggs[0]
                task = ErTask(
                    id=generate_task_id(job_id, egg._command_endpoint._host),
                    name=job.name,
                    inputs=cleanup_partitions,
                    job=job,
                )
                args.append(([task], egg._command_endpoint))

            futures = self.__command_client.async_call(
                args=args,
                output_types=[ErTask],
                command_uri=CommandURI(f"{RollPair.EGG_PAIR_URI_PREFIX}/{RollPair.RUN_TASK}"),
            )

            for future in futures:
                result = future.result()

            self.get_session()._cluster_manager_client.delete_store(er_store)
        else:
            # todo:1: add combine options to pass it through
            store_options = self.__session.get_all_options()
            store_options.update(options)
            final_options = store_options.copy()

            store = ErStore(
                store_locator=ErStoreLocator(
                    store_type=StoreTypes.ROLLPAIR_LMDB,
                    namespace=namespace,
                    name=name,
                    total_partitions=total_partitions,
                ),
                options=final_options,
            )
            task_results = self.__session._cluster_manager_client.get_store_from_namespace(store)
            L.trace("res={}".format(task_results._stores))
            if task_results._stores is not None:
                L.trace("item count={}".format(len(task_results._stores)))
                for item in task_results._stores:
                    L.trace(
                        "item namespace={} name={}".format(item._store_locator._namespace, item._store_locator._name)
                    )
                    rp = RollPair(er_store=item, rp_ctx=self)
                    rp.destroy()
