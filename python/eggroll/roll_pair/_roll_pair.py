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
import functools
import logging
import os
import queue
import time
import typing
import uuid
from concurrent.futures import wait, FIRST_EXCEPTION
from threading import Thread
from typing import Callable, Tuple, Iterable, List, Type

from eggroll.core.aspects import _method_profile_logger
from eggroll.core.client import CommandClient
from eggroll.core.command.command_model import CommandURI
from eggroll.core.constants import StoreTypes
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.meta_model import ErJob, ErStore, ErFunctor, ErTask, ErPartitioner, ErJobIO, ErSerdes
from eggroll.core.model.task import (
    CountResponse,
    GetRequest,
    GetResponse,
    PutRequest,
    PutResponse,
    GetAllRequest,
    DeleteRequest,
    DeleteResponse,
    MapPartitionsWithIndexRequest,
    ReduceResponse,
    WithStoresResponse,
)
from eggroll.core.utils import generate_job_id, generate_task_id
from eggroll.roll_pair.transfer_pair import TransferPair, BatchBroker

if typing.TYPE_CHECKING:
    from .roll_pair import RollPairContext

L = logging.getLogger(__name__)

T = typing.TypeVar("T")


class RollPair(object):
    ROLL_PAIR_URI_PREFIX = "v1/roll-pair"
    EGG_PAIR_URI_PREFIX = "v1/eggs-pair"

    RUN_JOB = "runJob"
    RUN_TASK = "runTask"

    CLEANUP = "cleanup"
    COUNT = "count"
    DELETE = "delete"
    DESTROY = "destroy"
    GET = "get"
    GET_ALL = "getAll"
    MAP_REDUCE_PARTITIONS_WITH_INDEX = "mapReducePartitionsWithIndex"
    BINARY_SORTED_MAP_PARTITIONS_WITH_INDEX = "binarySortedMapPartitionsWithIndex"
    PUT = "put"
    PUT_ALL = "putAll"
    REDUCE = "reduce"

    WITH_STORES = "withStores"
    RUN_TASK_URI = CommandURI(f"{EGG_PAIR_URI_PREFIX}/{RUN_TASK}")

    def __setstate__(self, state):
        self.gc_enable = None
        pass

    def __getstate__(self):
        pass

    def __init__(self, er_store: ErStore, rp_ctx: "RollPairContext", no_gc=False):
        if not rp_ctx:
            raise ValueError("rp_ctx cannot be None")
        self._store = er_store
        self.ctx = rp_ctx
        self._command_client = CommandClient(config=self.ctx.session.config)
        self._session_id = self.ctx.session_id
        self.gc_enable = rp_ctx.rpc_gc_enable
        self.gc_recorder = rp_ctx.gc_recorder
        self.gc_recorder.record(er_store)
        self.destroyed = False
        self._partitioner = None

    @property
    def config(self):
        return self.ctx.session.config

    def __del__(self):
        if "EGGROLL_GC_DISABLE" in os.environ and os.environ["EGGROLL_GC_DISABLE"] == "1":
            L.trace("global RollPair gc is disable")
            return
        if not hasattr(self, "gc_enable") or not hasattr(self, "ctx"):
            return
        if not self.gc_enable:
            L.debug("GC not enabled session={}".format(self._session_id))
            return

        if self.get_store_type() != StoreTypes.ROLLPAIR_IN_MEMORY:
            return

        if self.destroyed:
            return
        if self.ctx.get_session().is_stopped():
            L.trace("session={} has already been stopped".format(self._session_id))
            return

        self.ctx.gc_recorder.decrease_ref_count(self._store)

    def __repr__(self):
        return f"<{self.__class__.__name__}(_store={self._store}) at {hex(id(self))}>"

    def enable_gc(self):
        self.gc_enable = True

    def disable_gc(self):
        self.gc_enable = False

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

    def _run_unary_unit_job(
            self,
            job: ErJob,
            output_types: List[Type[T]] = None,
    ) -> List[List[T]]:
        if output_types is None:
            output_types = [ErTask]
        tasks = job.decompose_tasks()
        futures = self._command_client.async_call(args=tasks, output_types=output_types, command_uri=self.RUN_TASK_URI)
        done, not_done = wait(futures, return_when=FIRST_EXCEPTION)
        results = [future.result() for future in done]
        if len(not_done) > 0:
            for future in not_done:
                future.cancel()
        return results

    """
      storage api
    """

    def _process_task_on_key(self, partition_id: int, job_name, request, response_type: typing.Type[T]) -> T:
        partition = self._store.get_partition(partition_id)
        job_id = generate_job_id(self._session_id, job_name)
        task = ErTask(
            id=generate_task_id(job_id, partition_id),
            name=job_name,
            inputs=[partition],
            job=ErJob(
                id=job_id,
                name=job_name,
                inputs=[ErJobIO(self._store)],
                functors=[ErFunctor(name=job_name, body=request.to_proto_string())],
            ),
        )
        task_resp = self._command_client.simple_sync_send(
            input=task,
            output_type=response_type,
            endpoint=partition.processor.command_endpoint,
            command_uri=RollPair.RUN_TASK_URI,
        )
        return task_resp

    @_method_profile_logger
    def get(self, k: bytes, partitioner: Callable[[bytes, int], int]):
        partition_id = partitioner(k, self.num_partitions)
        response = self._process_task_on_key(
            partition_id=partition_id,
            job_name=RollPair.GET,
            request=GetRequest(key=k),
            response_type=GetResponse,
        )
        return response.value if response.exists else None

    @_method_profile_logger
    def put(self, k, v, partitioner: Callable[[bytes, int], int]) -> bool:
        partition_id = partitioner(k, self.num_partitions)
        response = self._process_task_on_key(
            partition_id=partition_id,
            job_name=RollPair.PUT,
            request=PutRequest(key=k, value=v),
            response_type=PutResponse,
        )
        return response.success

    @_method_profile_logger
    def delete(self, k: bytes, partitioner: Callable[[bytes, int], int]) -> bool:
        partitioner_id = partitioner(k, self.num_partitions)
        response = self._process_task_on_key(
            partition_id=partitioner_id,
            job_name=RollPair.DELETE,
            request=DeleteRequest(key=k),
            response_type=DeleteResponse,
        )
        return response.success

    @_method_profile_logger
    def get_all(self, limit=None):
        if limit is not None and not isinstance(limit, int) and limit <= 0:
            raise ValueError(f"limit:{limit} must be positive int")
        if limit is None:
            limit = -1
        job_id = generate_job_id(self._session_id, RollPair.GET_ALL)
        self._run_unary_unit_job(
            job=ErJob(
                id=job_id,
                name=RollPair.GET_ALL,
                inputs=[ErJobIO(self._store)],
                functors=[ErFunctor(name=RollPair.GET_ALL, body=GetAllRequest(limit=limit).to_proto_string())],
            )
        )
        transfer_pair = TransferPair(config=self.config, transfer_id=job_id)
        done_cnt = 0
        for k, v in transfer_pair.gather(config=self.config, store=self._store):
            done_cnt += 1
            yield k, v

    @_method_profile_logger
    def put_all(self, kv_list: Iterable[Tuple[bytes, bytes]], partitioner: Callable[[bytes, int], int]):
        job_id = generate_job_id(self._session_id, RollPair.PUT_ALL)

        # TODO:1: consider multiprocessing scenario. parallel size should be sent to egg_pair to set write signal count
        def send_command():
            job = ErJob(
                id=job_id,
                name=RollPair.PUT_ALL,
                inputs=[ErJobIO(self._store)],
                outputs=[ErJobIO(self._store)],
                functors=[],
            )
            self._run_unary_unit_job(job)

        th = DaemonThreadWithExceptionPropagate.thread(target=send_command, name=f"put_all-send_command-{job_id}")
        th.start()
        shuffler = TransferPair(config=self.config, transfer_id=job_id)
        fifo_broker = FifoBroker()
        bb = BatchBroker(config=self.config, broker=fifo_broker)
        scatter_future = shuffler.scatter(self.config, fifo_broker, partitioner, self._store)

        with bb:
            for k, v in kv_list:
                bb.put(item=(k, v))

        _wait_all_done(scatter_future, th)
        return RollPair(self._store, self.ctx)

    @_method_profile_logger
    def count(self) -> int:
        job_id = generate_job_id(self._session_id, tag=RollPair.COUNT)
        job = ErJob(id=job_id, name=RollPair.COUNT, inputs=[ErJobIO(self._store)])
        task_results = self._run_unary_unit_job(job, output_types=[CountResponse])
        total = 0
        for task_result in task_results:
            partition_count = task_result[0]
            total += partition_count.value
        return total

    @_method_profile_logger
    def destroy(self):
        if self.destroyed:
            L.exception(f"store:{self.get_store()} has been destroyed before")
            raise ValueError(f"store:{self.get_store()} has been destroyed before")
        self._run_unary_unit_job(
            ErJob(
                id=generate_job_id(self._session_id, RollPair.DESTROY),
                name=RollPair.DESTROY,
                inputs=[ErJobIO(self._store)],
                functors=[],
            )
        )
        self.ctx.get_session().cluster_manager_client.delete_store(self._store)
        L.debug(f"{RollPair.DESTROY}={self._store}")
        self.destroyed = True

    @_method_profile_logger
    def take(self, num: int, options: dict = None):
        if options is None:
            options = {}
        if num <= 0:
            num = 1

        keys_only = options.get("keys_only", False)
        ret = []
        count = 0
        for item in self.get_all(limit=num):
            if keys_only:
                if item:
                    ret.append(item[0])
                else:
                    ret.append(None)
            else:
                ret.append(item)
            count += 1
            if count == num:
                break
        return ret

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

    @_method_profile_logger
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
        if output_namespace is None:
            output_namespace = self.get_namespace()
        if output_name is None:
            output_name = str(uuid.uuid1())
        if output_store_type is None:
            output_store_type = self.get_store_type()
        if not shuffle:
            if input_key_serdes_type != output_key_serdes_type:
                raise ValueError("key serdes type changed without shuffle")
            if self.num_partitions != output_num_partitions:
                raise ValueError("partition num changed without shuffle")
            if input_partitioner_type != output_partitioner_type:
                raise ValueError("partitioner type changed without shuffle")

        map_functor = ErFunctor.from_func(name=RollPair.MAP_REDUCE_PARTITIONS_WITH_INDEX, func=map_partition_op)
        reduce_functor = ErFunctor.from_func(name=RollPair.MAP_REDUCE_PARTITIONS_WITH_INDEX, func=reduce_partition_op)
        shuffle = ErFunctor(
            name=f"{RollPair.MAP_REDUCE_PARTITIONS_WITH_INDEX}_{shuffle}",
            body=MapPartitionsWithIndexRequest(shuffle=shuffle).to_proto_string(),
        )
        job_id = generate_job_id(self._session_id, RollPair.MAP_REDUCE_PARTITIONS_WITH_INDEX)
        output_store = self.ctx.create_store(
            id=self._store.store_locator.id,
            name=output_name,
            namespace=output_namespace,
            total_partitions=output_num_partitions,
            store_type=output_store_type,
            key_serdes_type=output_key_serdes_type,
            value_serdes_type=output_value_serdes_type,
            partitioner_type=output_partitioner_type,
            options={},
        )
        output_store = self.ctx.get_session().get_or_create_store(output_store)
        job = ErJob(
            id=job_id,
            name=RollPair.MAP_REDUCE_PARTITIONS_WITH_INDEX,
            inputs=[
                ErJobIO(
                    self._store,
                    key_serdes=ErSerdes.from_func(input_key_serdes_type, input_key_serdes),
                    value_serdes=ErSerdes.from_func(input_value_serdes_type, input_value_serdes),
                    partitioner=ErPartitioner.from_func(input_partitioner_type, input_partitioner),
                )
            ],
            outputs=[
                ErJobIO(
                    output_store,
                    key_serdes=ErSerdes.from_func(output_key_serdes_type, output_key_serdes),
                    value_serdes=ErSerdes.from_func(output_value_serdes_type, output_value_serdes),
                    partitioner=ErPartitioner.from_func(output_partitioner_type, output_partitioner),
                )
            ],
            functors=[map_functor, shuffle, reduce_functor],
        )
        self._run_unary_unit_job(job=job)
        return RollPair(output_store, self.ctx)

    @_method_profile_logger
    def reduce(self, func):
        job_id = generate_job_id(self._session_id, tag=RollPair.REDUCE)
        reduce_op = ErFunctor.from_func(name=RollPair.REDUCE, func=func)
        job = ErJob(
            id=job_id,
            name=RollPair.REDUCE,
            inputs=[ErJobIO(self._store)],
            functors=[reduce_op],
        )
        results = self._run_unary_unit_job(job, output_types=[ReduceResponse])
        results = [r[0].value for r in results if r[0].value]
        if len(results) == 0:
            return None
        return functools.reduce(func, results)

    @_method_profile_logger
    def binary_sorted_map_partitions_with_index(
            self,
            other: "RollPair",
            binary_map_partitions_with_index_op: Callable[[int, Iterable, Iterable], Iterable],
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
        output_store = self._store.fork()
        output_store = self.ctx.get_session().get_or_create_store(output_store)
        functor = ErFunctor.from_func(
            name=RollPair.BINARY_SORTED_MAP_PARTITIONS_WITH_INDEX, func=binary_map_partitions_with_index_op
        )
        job = ErJob(
            id=generate_job_id(self._session_id, RollPair.BINARY_SORTED_MAP_PARTITIONS_WITH_INDEX),
            name=RollPair.BINARY_SORTED_MAP_PARTITIONS_WITH_INDEX,
            inputs=[ErJobIO(self._store), ErJobIO(other._store)],
            outputs=[ErJobIO(output_store)],
            functors=[functor],
        )
        self._run_unary_unit_job(job=job)
        return RollPair(output_store, self.ctx)

    @_method_profile_logger
    def with_stores(self, func, others: List["RollPair"] = None, options: dict = None, description: str = None):
        if options is None:
            options = {}
        tag = f"{RollPair.WITH_STORES}-{description}" if description else RollPair.WITH_STORES
        stores = [self._store]
        if others is None:
            others = []
        for other in others:
            stores.append(other._store)
        total_partitions = self.get_partitions()
        for other in others:
            if other.get_partitions() != total_partitions:
                raise ValueError(f"diff partitions: expected={total_partitions}, actual={other.get_partitions()}")
        job_id = generate_job_id(self._session_id, tag=tag)
        job = ErJob(
            id=job_id,
            name=RollPair.WITH_STORES,
            inputs=[ErJobIO(store) for store in stores],
            functors=[ErFunctor.from_func(name=RollPair.WITH_STORES, func=func)],
            options=options,
        )
        responses = self._run_unary_unit_job(job=job, output_types=[WithStoresResponse])
        results = []
        for response in responses:
            response = response[0]
            results.append((response.id, response.value))
        return results


class DaemonThreadWithExceptionPropagate:
    @classmethod
    def thread(cls, target, name, args=()):
        q = queue.Queue()
        th = Thread(target=_thread_target_wrapper(target), name=name, args=[q, *args], daemon=True)
        return cls(q, th)

    def __init__(self, q, thread):
        self.q = q
        self.thread = thread

    def start(self):
        self.thread.start()

    def done(self):
        return not self.thread.is_alive()

    def result(self):
        self.thread.join()
        e = self.q.get()
        if e:
            raise e


def _thread_target_wrapper(target):
    def wrapper(q: queue.Queue, *args):
        try:
            target(*args)
        except Exception as e:
            q.put(e)
        else:
            q.put(None)

    return wrapper


def _wait_all_done(*futures):
    has_call_result = [False] * len(futures)
    while not all(has_call_result):
        for i, f in enumerate(futures):
            if not has_call_result[i] and f.done():
                f.result()
                has_call_result[i] = True
        time.sleep(0.1)
