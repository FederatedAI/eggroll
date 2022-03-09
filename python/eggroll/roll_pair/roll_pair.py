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
import os
import time
import uuid
from concurrent.futures import wait, FIRST_EXCEPTION
from threading import Thread

import cloudpickle

from eggroll.core.aspects import _method_profile_logger
from eggroll.core.client import CommandClient
from eggroll.core.command.command_model import CommandURI
from eggroll.core.conf_keys import SessionConfKeys, RollPairConfKeys
from eggroll.core.constants import StoreTypes, SerdesTypes, PartitionerTypes, \
    SessionStatus
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor, \
    ErTask, ErPair, ErPartition
from eggroll.core.session import ErSession
from eggroll.core.utils import generate_job_id, generate_task_id, get_runtime_storage
from eggroll.core.utils import string_to_bytes, hash_code
from eggroll.roll_pair import create_serdes, create_adapter
from eggroll.roll_pair.transfer_pair import TransferPair, BatchBroker
from eggroll.roll_pair.utils.gc_utils import GcRecorder
from eggroll.roll_pair.utils.pair_utils import partitioner
from eggroll.utils.log_utils import get_logger

L = get_logger()


def runtime_init(session: ErSession):
    rpc = RollPairContext(session=session)
    return rpc


class RollPairContext(object):

    def __init__(self, session: ErSession):
        if session.get_session_meta()._status != SessionStatus.ACTIVE:
            raise Exception(
                f"session_id={session.get_session_id()} is not ACTIVE. current status={session.get_session_meta()._status}")
        self.__session = session
        self.session_id = session.get_session_id()
        default_store_type_str = RollPairConfKeys.EGGROLL_ROLLPAIR_DEFAULT_STORE_TYPE.get_with(
            session.get_all_options())
        self.default_store_type = getattr(StoreTypes, default_store_type_str, None)
        if not self.default_store_type:
            raise ValueError(f'store type "{default_store_type_str}" not found for roll pair')
        self.in_memory_output = RollPairConfKeys.EGGROLL_ROLLPAIR_IN_MEMORY_OUTPUT.get_with(session.get_all_options())
        if not self.default_store_type:
            raise ValueError(f'in_memory_output "{self.in_memory_output}" not found for roll pair')

        self.default_store_serdes = SerdesTypes.PICKLE
        self.__session_meta = session.get_session_meta()
        self.__session.add_exit_task(self.context_gc)
        self.rpc_gc_enable = True
        self.gc_recorder = GcRecorder(self)
        self.__command_client = CommandClient()

        self.session_default_rp = self.load(name=self.session_id,
                                            namespace=f'er_session_meta',
                                            options={'total_partitions': session.get_eggs_count(),
                                                     'store_type': StoreTypes.ROLLPAIR_CACHE,
                                                     'create_if_missing': True})
        eggs = session.get_eggs()

        def _broadcast_eggs(task: ErTask):
            from eggroll.core.utils import add_runtime_storage
            _input = task._inputs[0]
            add_runtime_storage("__eggs", eggs)
            L.debug(f"runtime_storage={get_runtime_storage('__eggs')}")

        self.session_default_rp.with_stores(func=_broadcast_eggs)

    def set_store_type(self, store_type: str):
        self.default_store_type = store_type

    def set_store_serdes(self, serdes_type: str):
        self.default_store_serdes = serdes_type

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
        options = dict()
        options['create_if_missing'] = True
        for k, v in (self.gc_recorder.gc_recorder.items()):
            namespace = k[0]
            name = k[1]
            rp = self.load(namespace=namespace, name=name, options=options)
            rp.destroy()

    def route_to_egg(self, partition: ErPartition):
        return self.__session.route_to_egg(partition)

    def populate_processor(self, store: ErStore):
        return self.__session.populate_processor(store)

    def load_store(self, name=None, namespace=None, options: dict = None):
        if options is None:
            options = {}
        if not namespace:
            namespace = options.get('namespace', self.get_session().get_session_id())
        if not name:
            raise ValueError(f"name is required, cannot be blank")
        store_type = options.get('store_type', self.default_store_type)
        total_partitions = options.get('total_partitions', 1)

        partitioner = options.get('partitioner', PartitionerTypes.BYTESTRING_HASH)
        store_serdes = options.get('serdes', self.default_store_serdes)
        create_if_missing = options.get('create_if_missing', False)
        # todo:1: add combine options to pass it through
        store_options = self.__session.get_all_options()
        store_options.update(options)
        final_options = store_options.copy()

        # TODO:0: add 'error_if_exist, persistent / default store type'
        store = ErStore(
            store_locator=ErStoreLocator(
                store_type=store_type,
                namespace=namespace,
                name=name,
                total_partitions=total_partitions,
                partitioner=partitioner,
                serdes=store_serdes),
            options=final_options)

        if create_if_missing:
            result = self.__session._cluster_manager_client.get_or_create_store(store)
        else:
            result = self.__session._cluster_manager_client.get_store(store)
            if len(result._partitions) == 0:
                L.info(f"store: namespace={namespace}, name={name} not exist, "
                       f"create_if_missing={create_if_missing}, create first")
                return None

        return self.populate_processor(result)

    def load(self, name=None, namespace=None, options: dict = None):
        store = self.load_store(name=name, namespace=namespace, options=options)
        return RollPair(store, self)

    # TODO:1: separates load parameters and put all parameters
    def parallelize(self, data, options: dict = None):
        if options is None:
            options = {}
        namespace = options.get("namespace", None)
        name = options.get("name", None)
        options['store_type'] = options.get("store_type", StoreTypes.ROLLPAIR_IN_MEMORY)
        options['include_key '] = options.get('include_key', False)
        options['create_if_missing'] = True

        if namespace is None:
            namespace = self.session_id
        if name is None:
            name = str(uuid.uuid1())
        rp = self.load(namespace=namespace, name=name, options=options)
        return rp.put_all(data, options=options)

    '''store name only supports full name and reg: *, *abc ,abc* and a*c'''

    def cleanup(self, name, namespace, options: dict = None):
        if not namespace:
            raise ValueError('namespace cannot be blank')
        L.debug(f'cleaning up namespace={namespace}, name={name}')
        if options is None:
            options = {}
        total_partitions = options.get('total_partitions', 1)
        partitioner = options.get('partitioner', PartitionerTypes.BYTESTRING_HASH)
        store_serdes = options.get('serdes', self.default_store_serdes)

        if name == '*':
            store_type = options.get('store_type', '*')
            L.debug(f'cleaning up whole store_type={store_type}, namespace={namespace}, name={name}')
            er_store = ErStore(store_locator=ErStoreLocator(namespace=namespace,
                                                            name=name,
                                                            store_type=store_type))
            job_id = generate_job_id(namespace, tag=RollPair.CLEANUP)
            job = ErJob(id=job_id,
                        name=RollPair.DESTROY,
                        inputs=[er_store],
                        options=options)

            args = list()
            cleanup_partitions = [ErPartition(id=-1, store_locator=er_store._store_locator)]

            for server_node, eggs in self.__session._eggs.items():
                egg = eggs[0]
                task = ErTask(id=generate_task_id(job_id, egg._command_endpoint._host),
                              name=job._name,
                              inputs=cleanup_partitions,
                              job=job)
                args.append(([task], egg._command_endpoint))

            futures = self.__command_client.async_call(
                args=args,
                output_types=[ErTask],
                command_uri=CommandURI(f'{RollPair.EGG_PAIR_URI_PREFIX}/{RollPair.RUN_TASK}'))

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
                    partitioner=partitioner,
                    serdes=store_serdes),
                options=final_options)
            task_results = self.__session._cluster_manager_client.get_store_from_namespace(store)
            L.trace('res={}'.format(task_results._stores))
            if task_results._stores is not None:
                L.trace("item count={}".format(len(task_results._stores)))
                for item in task_results._stores:
                    L.trace("item namespace={} name={}".format(item._store_locator._namespace,
                                                               item._store_locator._name))
                    rp = RollPair(er_store=item, rp_ctx=self)
                    rp.destroy()


def default_partitioner(k):
    return 0


def default_egg_router(k):
    return 0


class RollPair(object):
    ROLL_PAIR_URI_PREFIX = 'v1/roll-pair'
    EGG_PAIR_URI_PREFIX = 'v1/egg-pair'

    RUN_JOB = 'runJob'
    RUN_TASK = 'runTask'

    AGGREGATE = 'aggregate'
    COLLAPSE_PARTITIONS = 'collapsePartitions'
    CLEANUP = 'cleanup'
    COUNT = 'count'
    DELETE = "delete"
    DESTROY = "destroy"
    FILTER = 'filter'
    FLAT_MAP = 'flatMap'
    GET = "get"
    GET_ALL = "getAll"
    GLOM = 'glom'
    JOIN = 'join'
    MAP = 'map'
    MAP_PARTITIONS = 'mapPartitions'
    MAP_PARTITIONS_WITH_INDEX = 'mapPartitionsWithIndex'
    MAP_VALUES = 'mapValues'
    PUT = "put"
    PUT_ALL = "putAll"
    PUT_BATCH = "putBatch"
    REDUCE = 'reduce'
    SAMPLE = 'sample'
    SUBTRACT_BY_KEY = 'subtractByKey'
    UNION = 'union'

    RUN_TASK_URI = CommandURI(f'{EGG_PAIR_URI_PREFIX}/{RUN_TASK}')
    SERIALIZED_NONE = cloudpickle.dumps(None)

    def __setstate__(self, state):
        self.gc_enable = None
        pass

    def __getstate__(self):
        pass

    def __init__(self, er_store: ErStore, rp_ctx: RollPairContext):
        if not rp_ctx:
            raise ValueError('rp_ctx cannot be None')
        self.__store = er_store
        self.ctx = rp_ctx
        self.__command_serdes = SerdesTypes.PROTOBUF
        # self.__roll_pair_master = self.ctx.get_roll()
        self.__command_client = CommandClient()
        self.functor_serdes = create_serdes(SerdesTypes.CLOUD_PICKLE)
        self.value_serdes = self.get_store_serdes()
        self.key_serdes = self.get_store_serdes()
        # self.partitioner = partitioner(hash_code, self.__store._store_locator._total_partitions)
        import mmh3
        self.partitioner = partitioner(mmh3.hash, self.__store._store_locator._total_partitions)
        self.egg_router = default_egg_router
        self.__session_id = self.ctx.session_id
        self.gc_enable = rp_ctx.rpc_gc_enable
        self.gc_recorder = rp_ctx.gc_recorder
        self.gc_recorder.record(er_store)
        self.destroyed = False

    def __del__(self):
        if "EGGROLL_GC_DISABLE" in os.environ and os.environ["EGGROLL_GC_DISABLE"] == '1':
            L.trace("global RollPair gc is disable")
            return
        if not hasattr(self, 'gc_enable') \
                or not hasattr(self, 'ctx'):
            return
        if not self.gc_enable:
            L.debug('GC not enabled session={}'.format(self.__session_id))
            return

        if self.get_store_type() != StoreTypes.ROLLPAIR_IN_MEMORY:
            return

        if self.destroyed:
            return
        if self.ctx.get_session().is_stopped():
            L.trace('session={} has already been stopped'.format(self.__session_id))
            return

        self.ctx.gc_recorder.decrease_ref_count(self.__store)

    def __repr__(self):
        return f'<RollPair(_store={self.__store}) at {hex(id(self))}>'

    @staticmethod
    def __check_partition(input_partitions, output_partitions, shuffle=False):
        if not shuffle:
            return
        if input_partitions != output_partitions:
            raise ValueError(f"input partitions:{input_partitions}, output partitions:{output_partitions},"
                             f"must be the same!")

    def __repartition_with(self, other):
        self_partition = self.get_partitions()
        other_partition = other.get_partitions()

        should_shuffle = False
        if len(self.__store._partitions) != len(other.__store._partitions):
            should_shuffle = True
        else:
            for i in range(len(self.__store._partitions)):
                if self.__store._partitions[i]._processor._id != other.__store._partitions[i]._processor._id:
                    should_shuffle = True

        if other_partition != self_partition or should_shuffle:
            self_name = self.get_name()
            self_count = self.count()
            other_name = other.get_name()
            other_count = other.count()

            L.debug(f"repartition start: self rp={self_name} partitions={self_partition}, "
                    f"other={other_name}: partitions={other_partition}, repartitioning")

            if self_count < other_count:
                shuffle_rp = self
                shuffle_rp_count = self_count
                shuffle_rp_name = self_name
                shuffle_total_partitions = self_partition
                shuffle_rp_partitions = self.__store._partitions

                not_shuffle_rp = other
                not_shuffle_rp_count = other_count
                not_shuffle_rp_name = other_name
                not_shuffle_total_partitions = other_partition
                not_shuffle_rp_partitions = other.__store._partitions
            else:
                not_shuffle_rp = self
                not_shuffle_rp_count = self_count
                not_shuffle_rp_name = self_name
                not_shuffle_total_partitions = self_partition
                not_shuffle_rp_partitions = self.__store._partitions

                shuffle_rp = other
                shuffle_rp_count = other_count
                shuffle_rp_name = other_name
                shuffle_total_partitions = other_partition
                shuffle_rp_partitions = other.__store._partitions

            L.trace(f"repartition selection: rp={shuffle_rp_name} count={shuffle_rp_count}, "
                    f"rp={not_shuffle_rp_name} count={not_shuffle_rp_count}. "
                    f"repartitioning {shuffle_rp_name}")
            store = ErStore(store_locator=ErStoreLocator(store_type=shuffle_rp.get_store_type(),
                                                         namespace=shuffle_rp.get_namespace(),
                                                         name=str(uuid.uuid1()),
                                                         total_partitions=not_shuffle_total_partitions),
                            partitions=not_shuffle_rp_partitions)
            res_rp = shuffle_rp.map(lambda k, v: (k, v), output=store)
            res_rp.disable_gc()

            if L.isEnabledFor(logging.DEBUG):
                L.debug(f"repartition end: rp to shuffle={shuffle_rp_name}, "
                        f"count={shuffle_rp_count}, partitions={shuffle_total_partitions}; "
                        f"rp NOT shuffled={not_shuffle_rp_name}, "
                        f"count={not_shuffle_rp_count}, partitions={not_shuffle_total_partitions}' "
                        f"res rp={res_rp.get_name()}, "
                        f"count={res_rp.count()}, partitions={res_rp.get_partitions()}")
            store_shuffle = res_rp.get_store()
            return [store_shuffle, other.get_store()] if self_count < other_count \
                else [self.get_store(), store_shuffle]
        else:
            return [self.__store, other.__store]

    def enable_gc(self):
        self.gc_enable = True

    def disable_gc(self):
        self.gc_enable = False

    def get_store_serdes(self):
        return create_serdes(self.__store._store_locator._serdes)

    def get_partitions(self):
        return self.__store._store_locator._total_partitions

    def get_name(self):
        return self.__store._store_locator._name

    def get_namespace(self):
        return self.__store._store_locator._namespace

    def get_store(self):
        return self.__store

    def get_store_type(self):
        return self.__store._store_locator._store_type

    def _run_job(self,
                 job: ErJob,
                 output_types: list = None,
                 command_uri: CommandURI = RUN_TASK_URI,
                 create_output_if_missing: bool = True):
        from eggroll.core.utils import _map_and_listify
        start = time.time()
        L.debug(
            f"[RUNJOB] calling: job_id={job._id}, name={job._name}, inputs={_map_and_listify(lambda i: f'namespace={i._store_locator._namespace}, name={i._store_locator._name}, store_type={i._store_locator._store_type}, total_partitions={i._store_locator._total_partitions}', job._inputs)}")
        futures = self.ctx.get_session().submit_job(
            job=job,
            output_types=output_types,
            command_uri=command_uri,
            create_output_if_missing=create_output_if_missing)

        results = list()
        for future in futures:
            results.append(future.result())
        elapsed = time.time() - start
        L.debug(
            f"[RUNJOB] called (elapsed={elapsed}): job_id={job._id}, name={job._name}, inputs={_map_and_listify(lambda i: f'namespace={i._store_locator._namespace}, name={i._store_locator._name}, store_type={i._store_locator._store_type}, total_partitions={i._store_locator._total_partitions}', job._inputs)}")
        return results

    def __get_output_from_result(self, results):
        return results[0][0]._job._outputs[0]

    """
      storage api
    """

    @_method_profile_logger
    def get(self, k, options: dict = None):
        if options is None:
            options = {}
        k = create_serdes(self.__store._store_locator._serdes).serialize(k)
        er_pair = ErPair(key=k, value=None)
        partition_id = self.partitioner(k)
        egg = self.ctx.route_to_egg(self.__store._partitions[partition_id])
        inputs = [ErPartition(id=partition_id, store_locator=self.__store._store_locator)]
        outputs = [ErPartition(id=partition_id, store_locator=self.__store._store_locator)]

        job_id = generate_job_id(self.__session_id, RollPair.GET)
        job = ErJob(id=job_id,
                    name=RollPair.GET,
                    inputs=[self.__store],
                    outputs=[self.__store],
                    functors=[ErFunctor(name=RollPair.GET, body=cloudpickle.dumps(er_pair))])

        task = ErTask(id=generate_task_id(job_id, partition_id),
                      name=RollPair.GET,
                      inputs=inputs,
                      outputs=outputs,
                      job=job)
        job_resp = self.__command_client.simple_sync_send(
            input=task,
            output_type=ErPair,
            endpoint=egg._command_endpoint,
            command_uri=self.RUN_TASK_URI,
            serdes_type=self.__command_serdes
        )

        return self.value_serdes.deserialize(job_resp._value) if job_resp._value != b'' else None

    @_method_profile_logger
    def put(self, k, v, options: dict = None):
        if options is None:
            options = {}
        k, v = create_serdes(self.__store._store_locator._serdes).serialize(k), \
               create_serdes(self.__store._store_locator._serdes).serialize(v)
        er_pair = ErPair(key=k, value=v)
        outputs = []
        partition_id = self.partitioner(k)
        egg = self.ctx.route_to_egg(self.__store._partitions[partition_id])
        inputs = [ErPartition(id=partition_id, store_locator=self.__store._store_locator)]
        output = [ErPartition(id=0, store_locator=self.__store._store_locator)]

        job_id = generate_job_id(self.__session_id, RollPair.PUT)
        job = ErJob(id=job_id,
                    name=RollPair.PUT,
                    inputs=[self.__store],
                    outputs=outputs,
                    functors=[ErFunctor(name=RollPair.PUT, body=cloudpickle.dumps(er_pair))])

        task = ErTask(id=generate_task_id(job_id, partition_id),
                      name=RollPair.PUT,
                      inputs=inputs,
                      outputs=output,
                      job=job)
        job_resp = self.__command_client.simple_sync_send(
            input=task,
            output_type=ErPair,
            endpoint=egg._command_endpoint,
            command_uri=CommandURI(f'{RollPair.EGG_PAIR_URI_PREFIX}/{RollPair.RUN_TASK}'),
            serdes_type=self.__command_serdes
        )
        value = job_resp._value
        return value

    @_method_profile_logger
    def get_all(self, limit=None, options: dict = None):
        if options is None:
            options = {}

        if limit is not None and not isinstance(limit, int) and limit <= 0:
            raise ValueError(f"limit:{limit} must be positive int")

        job_id = generate_job_id(self.__session_id, RollPair.GET_ALL)
        er_pair = ErPair(key=create_serdes(self.__store._store_locator._serdes)
                         .serialize(limit) if limit is not None else None,
                         value=None)

        def send_command():
            job = ErJob(id=job_id,
                        name=RollPair.GET_ALL,
                        inputs=[self.__store],
                        outputs=[self.__store],
                        functors=[ErFunctor(name=RollPair.GET_ALL, body=cloudpickle.dumps(er_pair))])

            task_results = self._run_job(job=job)
            er_store = self.__get_output_from_result(task_results)

            return er_store

        send_command()

        populated_store = self.ctx.populate_processor(self.__store)
        transfer_pair = TransferPair(transfer_id=job_id)
        done_cnt = 0
        for k, v in transfer_pair.gather(populated_store):
            done_cnt += 1
            yield self.key_serdes.deserialize(k), self.value_serdes.deserialize(v)
        L.trace(f"get_all: namespace={self.get_namespace()} name={self.get_name()}, count={done_cnt}")

    @_method_profile_logger
    def put_all(self, items, output=None, options: dict = None):
        if options is None:
            options = {}
        include_key = options.get("include_key", True)
        job_id = generate_job_id(self.__session_id, RollPair.PUT_ALL)

        # TODO:1: consider multiprocessing scenario. parallel size should be sent to egg_pair to set write signal count
        def send_command():
            job = ErJob(id=job_id,
                        name=RollPair.PUT_ALL,
                        inputs=[self.__store],
                        outputs=[self.__store],
                        functors=[])

            task_results = self._run_job(job)

            return self.__get_output_from_result(task_results)

        th = Thread(target=send_command, name=f'roll_pair-send_command-{job_id}')
        th.start()
        populated_store = self.ctx.populate_processor(self.__store)
        shuffler = TransferPair(job_id)
        fifo_broker = FifoBroker()
        bb = BatchBroker(fifo_broker)
        scatter_future = shuffler.scatter(fifo_broker, self.partitioner, populated_store)

        key_serdes = self.key_serdes
        value_serdes = self.value_serdes
        try:
            if include_key:
                for k, v in items:
                    bb.put(item=(key_serdes.serialize(k), value_serdes.serialize(v)))
            else:
                k = 0
                for v in items:
                    bb.put(item=(key_serdes.serialize(k), value_serdes.serialize(v)))
                    k += 1
        finally:
            bb.signal_write_finish()

        scatter_results = scatter_future.result()
        th.join()
        return RollPair(populated_store, self.ctx)

    @_method_profile_logger
    def count(self):
        job_id = generate_job_id(self.__session_id, tag=RollPair.COUNT)

        job = ErJob(id=job_id,
                    name=RollPair.COUNT,
                    inputs=[self.__store])

        task_results = self._run_job(job=job, output_types=[ErPair], create_output_if_missing=False)

        result = 0
        for task_result in task_results:
            pair = task_result[0]
            result += self.functor_serdes.deserialize(pair._value)

        return result

    # todo:1: move to command channel to utilize batch command
    @_method_profile_logger
    def destroy(self, options: dict = None):
        if len(self.ctx.get_session()._cluster_manager_client.get_store(self.get_store())._partitions) == 0:
            L.exception(f"store:{self.get_store()} has been destroyed before")
            raise ValueError(f"store:{self.get_store()} has been destroyed before")

        if options is None:
            options = {}

        job = ErJob(id=generate_job_id(self.__session_id, RollPair.DESTROY),
                    name=RollPair.DESTROY,
                    inputs=[self.__store],
                    outputs=[self.__store],
                    functors=[],
                    options=options)

        task_results = self._run_job(job=job, create_output_if_missing=False)
        self.ctx.get_session()._cluster_manager_client.delete_store(self.__store)
        L.debug(f'{RollPair.DESTROY}={self.__store}')
        self.destroyed = True

    @_method_profile_logger
    def delete(self, k, options: dict = None):
        if options is None:
            options = {}
        key = create_serdes(self.__store._store_locator._serdes).serialize(k)
        er_pair = ErPair(key=key, value=None)
        value = None
        partition_id = self.partitioner(key)
        egg = self.ctx.route_to_egg(self.__store._partitions[partition_id])

        job_id = generate_job_id(self.__session_id, RollPair.DELETE)
        job = ErJob(id=job_id,
                    name=RollPair.DELETE,
                    inputs=[self.__store],
                    outputs=[],
                    functors=[ErFunctor(name=RollPair.DELETE, body=cloudpickle.dumps(er_pair))])

        task_results = self._run_job(job=job, create_output_if_missing=False)

    @_method_profile_logger
    def take(self, n: int, options: dict = None):
        if options is None:
            options = {}
        if n <= 0:
            n = 1

        keys_only = options.get("keys_only", False)
        ret = []
        count = 0
        for item in self.get_all(limit=n):
            if keys_only:
                if item:
                    ret.append(item[0])
                else:
                    ret.append(None)
            else:
                ret.append(item)
            count += 1
            if count == n:
                break
        return ret

    @_method_profile_logger
    def first(self, options: dict = None):
        if options is None:
            options = {}
        resp = self.take(1, options=options)
        if resp:
            return resp[0]
        else:
            return None

    @_method_profile_logger
    def save_as(self, name=None, namespace=None, partition=None, options: dict = None):
        if partition is not None and partition <= 0:
            raise ValueError('partition cannot <= 0')

        if not namespace:
            namespace = self.get_namespace()

        if not name:
            if self.get_namespace() == namespace:
                forked_store_locator = self.get_store()._store_locator.fork()
                name = forked_store_locator._name
            else:
                name = self.get_name()

        if not partition:
            partition = self.get_partitions()

        if options is None:
            options = {}

        store_type = options.get('store_type', self.ctx.default_store_type)
        refresh_nodes = options.get('refresh_nodes')

        saved_as_store = ErStore(store_locator=ErStoreLocator(
            store_type=store_type,
            namespace=namespace,
            name=name,
            total_partitions=partition))

        if partition == self.get_partitions() and not refresh_nodes:
            return self.map_values(lambda v: v, output=saved_as_store, options=options)
        else:
            return self.map(lambda k, v: (k, v), output=saved_as_store, options=options)

    """
        computing api
    """

    def _maybe_set_output(self, output):
        outputs = []
        if output:
            RollPair.__check_partition(self.get_partitions(), output._store_locator._total_partitions)
            outputs.append(output)
        elif self.get_store_type() != StoreTypes.ROLLPAIR_IN_MEMORY and self.ctx.in_memory_output:
            store = self.ctx.load_store(name=str(uuid.uuid1()),
                                        options={"store_type": StoreTypes.ROLLPAIR_IN_MEMORY,
                                                 "total_partitions": self.get_partitions(),
                                                 "create_if_missing": True})
            outputs.append(store)
        return outputs

    @_method_profile_logger
    def map_values(self, func, output=None, options: dict = None):
        if options is None:
            options = {}

        outputs = self._maybe_set_output(output)

        functor = ErFunctor(name=RollPair.MAP_VALUES, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
        # todo:1: options issues. refer to line 77
        final_options = {}
        final_options.update(self.__store._options)
        final_options.update(options)
        job = ErJob(id=generate_job_id(self.__session_id, RollPair.MAP_VALUES),
                    name=RollPair.MAP_VALUES,
                    inputs=[self.__store],
                    outputs=outputs,
                    functors=[functor],
                    options=final_options)

        task_results = self._run_job(job=job)
        er_store = self.__get_output_from_result(task_results)

        return RollPair(er_store, self.ctx)

    @_method_profile_logger
    def map(self, func, output=None, options: dict = None):
        if options is None:
            options = {}

        outputs = self._maybe_set_output(output)
        functor = ErFunctor(name=RollPair.MAP, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))

        job = ErJob(id=generate_job_id(self.__session_id, RollPair.MAP),
                    name=RollPair.MAP,
                    inputs=[self.__store],
                    outputs=outputs,
                    functors=[functor],
                    options=options)

        task_results = self._run_job(job=job)
        er_store = self.__get_output_from_result(task_results)

        return RollPair(er_store, self.ctx)

    @_method_profile_logger
    def map_partitions(self, func, reduce_op=None, output=None, options: dict = None):
        if options is None:
            options = {}

        outputs = self._maybe_set_output(output)

        shuffle = options.get('shuffle', True)
        if not shuffle and reduce_op:
            raise ValueError(f"shuffle cannot be False when reduce is needed!")
        functor = ErFunctor(name=RollPair.MAP_PARTITIONS, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
        reduce_functor = ErFunctor(name=RollPair.MAP_PARTITIONS, serdes=SerdesTypes.CLOUD_PICKLE,
                                   body=cloudpickle.dumps(reduce_op))
        need_shuffle = ErFunctor(name=RollPair.MAP_PARTITIONS, serdes=SerdesTypes.CLOUD_PICKLE,
                                 body=cloudpickle.dumps(shuffle))

        job = ErJob(id=generate_job_id(self.__session_id, RollPair.MAP_PARTITIONS),
                    name=RollPair.MAP_PARTITIONS,
                    inputs=[self.__store],
                    outputs=outputs,
                    functors=[functor, reduce_functor, need_shuffle])

        task_future = self._run_job(job=job)
        er_store = self.__get_output_from_result(task_future)
        return RollPair(er_store, self.ctx)

    @_method_profile_logger
    def collapse_partitions(self, func, output=None, options: dict = None):
        if options is None:
            options = {}

        outputs = self._maybe_set_output(output)

        functor = ErFunctor(name=RollPair.COLLAPSE_PARTITIONS, serdes=SerdesTypes.CLOUD_PICKLE,
                            body=cloudpickle.dumps(func))
        job = ErJob(id=generate_job_id(self.__session_id, RollPair.COLLAPSE_PARTITIONS),
                    name=RollPair.COLLAPSE_PARTITIONS,
                    inputs=[self.__store],
                    outputs=outputs,
                    functors=[functor])

        task_results = self._run_job(job=job)
        er_store = self.__get_output_from_result(task_results)

        return RollPair(er_store, self.ctx)

    @_method_profile_logger
    def map_partitions_with_index(self, func, output=None, options: dict = None):
        if options is None:
            options = {}

        outputs = self._maybe_set_output(output)

        shuffle = options.get('shuffle', True)

        functor = ErFunctor(name=RollPair.MAP_PARTITIONS_WITH_INDEX, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))

        need_shuffle = ErFunctor(name=RollPair.MAP_PARTITIONS_WITH_INDEX, serdes=SerdesTypes.CLOUD_PICKLE,
                                 body=cloudpickle.dumps(shuffle))

        job = ErJob(id=generate_job_id(self.__session_id, RollPair.MAP_PARTITIONS_WITH_INDEX),
                    name=RollPair.MAP_PARTITIONS_WITH_INDEX,
                    inputs=[self.__store],
                    outputs=outputs,
                    functors=[functor, need_shuffle])

        task_future = self._run_job(job=job)
        er_store = self.__get_output_from_result(task_future)
        return RollPair(er_store, self.ctx)

    @_method_profile_logger
    def flat_map(self, func, output=None, options: dict = None):
        if options is None:
            options = {}

        outputs = self._maybe_set_output(output)

        shuffle = options.get('shuffle', True)
        functor = ErFunctor(name=RollPair.FLAT_MAP, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
        need_shuffle = ErFunctor(name=RollPair.FLAT_MAP, serdes=SerdesTypes.CLOUD_PICKLE,
                                 body=cloudpickle.dumps(shuffle))

        job = ErJob(id=generate_job_id(self.__session_id, RollPair.FLAT_MAP),
                    name=RollPair.FLAT_MAP,
                    inputs=[self.__store],
                    outputs=outputs,
                    functors=[functor, need_shuffle])

        task_results = self._run_job(job=job)
        er_store = self.__get_output_from_result(task_results)

        return RollPair(er_store, self.ctx)

    @_method_profile_logger
    def reduce(self, func, output=None, options: dict = None):
        total_partitions = self.__store._store_locator._total_partitions
        job_id = generate_job_id(self.__session_id, tag=RollPair.REDUCE)

        serialized_func = ErFunctor(name=RollPair.REDUCE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
        job = ErJob(id=job_id,
                    name=RollPair.REDUCE,
                    inputs=[self.ctx.populate_processor(self.__store)],
                    functors=[serialized_func])
        args = list()
        for i in range(total_partitions):
            partition_input = job._inputs[0]._partitions[i]
            task = ErTask(id=generate_task_id(job_id, i),
                          name=job._name,
                          inputs=[partition_input],
                          job=job)
            args.append(([task], partition_input._processor._command_endpoint))

        futures = self.__command_client.async_call(
            args=args,
            output_types=[ErPair],
            command_uri=CommandURI(f'{RollPair.EGG_PAIR_URI_PREFIX}/{RollPair.RUN_TASK}'))

        done = wait(futures, return_when=FIRST_EXCEPTION).done

        result = None
        first = True
        for future in done:
            pair = future.result()[0]
            seq_op_result = self.functor_serdes.deserialize(pair._value)
            if seq_op_result is not None:
                if not first:
                    result = func(result, seq_op_result)
                else:
                    result = seq_op_result
                    first = False

        return result

    @_method_profile_logger
    def aggregate(self, zero_value, seq_op, comb_op, output=None, options: dict = None):
        total_partitions = self.__store._store_locator._total_partitions
        job_id = generate_job_id(self.__session_id, tag=RollPair.AGGREGATE)

        serialized_zero_value = ErFunctor(name=RollPair.AGGREGATE, serdes=SerdesTypes.CLOUD_PICKLE,
                                          body=cloudpickle.dumps(zero_value))
        serialized_seq_op = ErFunctor(name=RollPair.AGGREGATE, serdes=SerdesTypes.CLOUD_PICKLE,
                                      body=cloudpickle.dumps(seq_op))
        job = ErJob(id=job_id,
                    name=RollPair.AGGREGATE,
                    inputs=[self.ctx.populate_processor(self.__store)],
                    functors=[serialized_zero_value, serialized_seq_op])
        args = list()
        for i in range(total_partitions):
            partition_input = job._inputs[0]._partitions[i]
            task = ErTask(id=generate_task_id(job_id, i),
                          name=job._name,
                          inputs=[partition_input],
                          job=job)
            args.append(([task], partition_input._processor._command_endpoint))

        futures = self.__command_client.async_call(
            args=args,
            output_types=[ErPair],
            command_uri=CommandURI(f'{RollPair.EGG_PAIR_URI_PREFIX}/{RollPair.RUN_TASK}'))

        done = wait(futures, return_when=FIRST_EXCEPTION).done

        result = None
        first = True
        for future in done:
            pair = future.result()[0]
            seq_op_result = self.functor_serdes.deserialize(pair._value)
            if not first:
                result = comb_op(result, seq_op_result)
            else:
                result = seq_op_result
                first = False

        return result

    @_method_profile_logger
    def glom(self, output=None, options: dict = None):
        if options is None:
            options = {}

        outputs = self._maybe_set_output(output)
        functor = ErFunctor(name=RollPair.GLOM, serdes=SerdesTypes.CLOUD_PICKLE)

        job = ErJob(id=generate_job_id(self.__session_id, RollPair.GLOM),
                    name=RollPair.GLOM,
                    inputs=[self.__store],
                    outputs=outputs,
                    functors=[functor])

        task_results = self._run_job(job=job)
        er_store = self.__get_output_from_result(task_results)

        return RollPair(er_store, self.ctx)

    @_method_profile_logger
    def sample(self, fraction, seed=None, output=None, options: dict = None):
        if options is None:
            options = {}

        outputs = self._maybe_set_output(output)
        er_fraction = ErFunctor(name=RollPair.REDUCE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(fraction))
        er_seed = ErFunctor(name=RollPair.REDUCE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(seed))

        job = ErJob(id=generate_job_id(self.__session_id, RollPair.SAMPLE),
                    name=RollPair.SAMPLE,
                    inputs=[self.__store],
                    outputs=outputs,
                    functors=[er_fraction, er_seed])

        task_results = self._run_job(job=job)
        er_store = self.__get_output_from_result(task_results)

        return RollPair(er_store, self.ctx)

    @_method_profile_logger
    def filter(self, func, output=None, options: dict = None):
        if options is None:
            options = {}

        outputs = self._maybe_set_output(output)
        functor = ErFunctor(name=RollPair.FILTER, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))

        job = ErJob(id=generate_job_id(self.__session_id, RollPair.FILTER),
                    name=RollPair.FILTER,
                    inputs=[self.__store],
                    outputs=outputs,
                    functors=[functor])

        task_results = self._run_job(job=job)
        er_store = self.__get_output_from_result(task_results)

        return RollPair(er_store, self.ctx)

    @_method_profile_logger
    def subtract_by_key(self, other, output=None, options: dict = None):
        if options is None:
            options = {}

        inputs = self.__repartition_with(other)

        outputs = self._maybe_set_output(output)
        functor = ErFunctor(name=RollPair.SUBTRACT_BY_KEY, serdes=SerdesTypes.CLOUD_PICKLE)
        job = ErJob(id=generate_job_id(self.__session_id, RollPair.SUBTRACT_BY_KEY),
                    name=RollPair.SUBTRACT_BY_KEY,
                    inputs=inputs,
                    outputs=outputs,
                    functors=[functor])

        task_future = self._run_job(job=job)
        er_store = self.__get_output_from_result(task_future)
        return RollPair(er_store, self.ctx)

    @_method_profile_logger
    def union(self, other, func=lambda v1, v2: v1, output=None, options: dict = None):
        if options is None:
            options = {}

        inputs = self.__repartition_with(other)

        outputs = self._maybe_set_output(output)
        functor = ErFunctor(name=RollPair.UNION, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
        job = ErJob(id=generate_job_id(self.__session_id, RollPair.UNION),
                    name=RollPair.UNION,
                    inputs=inputs,
                    outputs=outputs,
                    functors=[functor])

        task_future = self._run_job(job=job)
        er_store = self.__get_output_from_result(task_future)
        return RollPair(er_store, self.ctx)

    @_method_profile_logger
    def join(self, other, func, output=None, options: dict = None):
        if options is None:
            options = {}

        inputs = self.__repartition_with(other)

        outputs = self._maybe_set_output(output)
        functor = ErFunctor(name=RollPair.JOIN, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))

        final_options = {}
        final_options.update(self.__store._options)
        final_options.update(options)
        job = ErJob(id=generate_job_id(self.__session_id, RollPair.JOIN),
                    name=RollPair.JOIN,
                    inputs=inputs,
                    outputs=outputs,
                    functors=[functor],
                    options=final_options)

        task_results = self._run_job(job=job)
        er_store = self.__get_output_from_result(task_results)

        return RollPair(er_store, self.ctx)

    @_method_profile_logger
    def with_stores(self, func, others=None, options: dict = None):
        if options is None:
            options = {}
        tag = "withStores"
        if others is None:
            others = []
        total_partitions = self.get_partitions()
        for other in others:
            if other.get_partitions() != total_partitions:
                raise ValueError(f"diff partitions: expected={total_partitions}, actual={other.get_partitions()}")
        job_id = generate_job_id(self.__session_id, tag=tag)
        job = ErJob(id=job_id,
                    name=tag,
                    inputs=[self.ctx.populate_processor(rp.get_store()) for rp in [self] + others],
                    functors=[ErFunctor(name=tag, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))],
                    options=options)
        args = list()
        for i in range(total_partitions):
            partition_self = job._inputs[0]._partitions[i]
            task = ErTask(id=generate_task_id(job_id, i),
                          name=job._name,
                          inputs=[store._partitions[i] for store in job._inputs],
                          job=job)
            args.append(([task], partition_self._processor._command_endpoint))

        futures = self.__command_client.async_call(
            args=args,
            output_types=[ErPair],
            command_uri=CommandURI(f'{RollPair.EGG_PAIR_URI_PREFIX}/{RollPair.RUN_TASK}'))

        result = list()
        for future in futures:
            ret_pair = future.result()[0]
            result.append((self.functor_serdes.deserialize(ret_pair._key),
                           self.functor_serdes.deserialize(ret_pair._value)))
        return result
