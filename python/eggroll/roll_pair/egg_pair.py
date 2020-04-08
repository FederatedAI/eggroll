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

import argparse
import configparser
import os
import signal
from collections.abc import Iterable
from concurrent import futures

import grpc
import numpy as np
from grpc._cython import cygrpc

from eggroll.core.client import ClusterManagerClient
from eggroll.core.command.command_router import CommandRouter
from eggroll.core.command.command_service import CommandServicer
from eggroll.core.conf_keys import SessionConfKeys, \
    ClusterManagerConfKeys
from eggroll.core.constants import ProcessorTypes, ProcessorStatus, SerdesTypes
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.meta_model import ErPair
from eggroll.core.meta_model import ErTask, ErProcessor, ErEndpoint
from eggroll.core.proto import command_pb2_grpc, transfer_pb2_grpc
from eggroll.core.transfer.transfer_service import GrpcTransferServicer, \
    TransferService
from eggroll.core.utils import _exception_logger
from eggroll.core.utils import hash_code
from eggroll.core.utils import set_static_er_conf
from eggroll.roll_pair import create_adapter, create_serdes, create_functor
from eggroll.roll_pair.transfer_pair import TransferPair
from eggroll.roll_pair.utils.pair_utils import generator, partitioner, \
    set_data_dir
from eggroll.utils.log_utils import get_logger

L = get_logger()


class EggPair(object):
    def __init__(self):
        self.functor_serdes = create_serdes(SerdesTypes.CLOUD_PICKLE)

    def __partitioner(self, hash_func, total_partitions):
        return lambda k: hash_func(k) % total_partitions

    def _run_unary(self, func, task, shuffle=False):
        key_serdes = create_serdes(task._inputs[0]._store_locator._serdes)
        value_serdes = create_serdes(task._inputs[0]._store_locator._serdes)
        with create_adapter(task._inputs[0]) as input_db:
            L.debug(f"create_store_adatper: {task._inputs[0]}")
            with input_db.iteritems() as rb:
                L.debug(f"create_store_adatper_iter: {task._inputs[0]}")
                from eggroll.roll_pair.transfer_pair import TransferPair, BatchBroker
                if shuffle:
                    total_partitions = task._inputs[0]._store_locator._total_partitions
                    output_store = task._job._outputs[0]
                    shuffle_broker = FifoBroker()
                    write_bb = BatchBroker(shuffle_broker)
                    try:
                        shuffler = TransferPair(transfer_id=task._job._id)
                        store_future = shuffler.store_broker(task._outputs[0], True, total_partitions)
                        scatter_future = shuffler.scatter(
                            shuffle_broker,
                            partitioner(hash_func=hash_code, total_partitions=total_partitions),
                            output_store)
                        func(rb, key_serdes, value_serdes, write_bb)
                    finally:
                        write_bb.signal_write_finish()
                    scatter_results = scatter_future.result()
                    store_result = store_future.result()
                    L.debug(f"scatter_result:{scatter_results}")
                    L.debug(f"gather_result:{store_result}")
                else:
                    # TODO: modification may be needed when store options finished
                    with create_adapter(task._outputs[0], options=task._job._options) as db, db.new_batch() as wb:
                        func(rb, key_serdes, value_serdes, wb)
                L.debug(f"close_store_adatper:{task._inputs[0]}")

    def _run_binary(self, func, task):
        left_key_serdes = create_serdes(task._inputs[0]._store_locator._serdes)
        left_value_serdes = create_serdes(task._inputs[0]._store_locator._serdes)

        right_key_serdes = create_serdes(task._inputs[1]._store_locator._serdes)
        right_value_serdes = create_serdes(task._inputs[1]._store_locator._serdes)

        with create_adapter(task._inputs[0]) as left_adapter, \
                create_adapter(task._inputs[1]) as right_adapter, \
                create_adapter(task._outputs[0]) as output_adapter, \
                left_adapter.iteritems() as left_iterator, \
                right_adapter.iteritems() as right_iterator, \
                output_adapter.new_batch() as output_writebatch:
            try:
                func(left_iterator, left_key_serdes, left_value_serdes,
                     right_iterator, right_key_serdes, right_value_serdes,
                     output_writebatch)
            except Exception as e:
                raise EnvironmentError("exec task:{} error".format(task), e)


    @_exception_logger
    def run_task(self, task: ErTask):
        L.info("start run task")
        L.debug("start run task")
        functors = task._job._functors
        result = task

        if task._name == 'get':
            L.info("egg_pair get call")
            # TODO:1: move to create_serdes
            f = create_functor(functors[0]._body)
            with create_adapter(task._inputs[0]) as input_adapter:
                print("get key:{} and path is:{}".format(self.functor_serdes.deserialize(f._key), input_adapter.path))
                value = input_adapter.get(f._key)
                result = ErPair(key=f._key, value=value)
        elif task._name == 'getAll':
            L.info("egg_pair getAll call")
            tag = f'{task._id}'
            def generate_broker():
                with create_adapter(task._inputs[0]) as db, db.iteritems() as rb:
                    yield from TransferPair.pair_to_bin_batch(rb)
                    # TODO:0 how to remove?
                    # TransferService.remove_broker(tag)
            TransferService.set_broker(tag, generate_broker())
        elif task._name == 'count':
            L.info('egg_pair count call')
            with create_adapter(task._inputs[0]) as input_adapter:
                result = ErPair(key=self.functor_serdes.serialize('result'),
                                value=self.functor_serdes.serialize(input_adapter.count()))

        # TODO:1: multiprocessor scenario
        elif task._name == 'putAll':
            L.info("egg_pair putAll call")
            output_partition = task._outputs[0]
            tag = f'{task._id}'
            L.info(f'egg_pair transfer service tag:{tag}')
            tf = TransferPair(tag)
            store_broker_result = tf.store_broker(output_partition, False).result()
            # TODO:2: should wait complete?, command timeout?
            L.debug(f"putAll result:{store_broker_result}")

        if task._name == 'put':
            L.info("egg_pair put call")
            f = create_functor(functors[0]._body)
            with create_adapter(task._inputs[0]) as input_adapter:
                value = input_adapter.put(f._key, f._value)
                #result = ErPair(key=f._key, value=bytes(value))

        if task._name == 'destroy':
            with create_adapter(task._inputs[0]) as input_adapter:
                input_adapter.destroy()
                L.info("finish destroy")

        if task._name == 'delete':
            f = create_functor(functors[0]._body)
            with create_adapter(task._inputs[0]) as input_adapter:
                L.info("delete k:{}, its value:{}".format(f._key, input_adapter.get(f._key)))
                if input_adapter.delete(f._key):
                    L.info("delete k success")

        if task._name == 'mapValues':
            f = create_functor(functors[0]._body)
            def map_values_wrapper(input_iterator, key_serdes, value_serdes, output_writebatch):
                for k_bytes, v_bytes in input_iterator:
                    v = value_serdes.deserialize(v_bytes)
                    output_writebatch.put(k_bytes, value_serdes.serialize(f(v)))
            self._run_unary(map_values_wrapper, task)
        elif task._name == 'map':
            f = create_functor(functors[0]._body)

            def map_wrapper(input_iterator, key_serdes, value_serdes, shuffle_broker):
                for k_bytes, v_bytes in input_iterator:
                    k1, v1 = f(key_serdes.deserialize(k_bytes), value_serdes.deserialize(v_bytes))
                    shuffle_broker.put((key_serdes.serialize(k1), value_serdes.serialize(v1)))
                L.info('finish calculating')
            self._run_unary(map_wrapper, task, shuffle=True)
            L.info('map finished')

        elif task._name == 'reduce':
            seq_op_result = self.aggregate_seq(task=task)
            L.info('reduce finished')
            result = ErPair(key=self.functor_serdes.serialize(task._inputs[0]._id),
                            value=self.functor_serdes.serialize(seq_op_result))

        elif task._name == 'aggregate':
            L.info('ready to aggregate')
            seq_op_result = self.aggregate_seq(task=task)
            L.info('aggregate finished')
            result = ErPair(key=self.functor_serdes.serialize(task._inputs[0]._id),
                            value=self.functor_serdes.serialize(seq_op_result))

        elif task._name == 'mapPartitions':
            def map_partitions_wrapper(input_iterator, key_serdes, value_serdes, shuffle_broker):
                f = create_functor(functors[0]._body)
                value = f(generator(key_serdes, value_serdes, input_iterator))
                if input_iterator.last():
                    L.info("value of mapPartitions:{}".format(value))
                    if isinstance(value, Iterable):
                        for k1, v1 in value:
                            shuffle_broker.put((key_serdes.serialize(k1), value_serdes.serialize(v1)))
                    else:
                        key = input_iterator.key()
                        shuffle_broker.put((key, value_serdes.serialize(value)))
            self._run_unary(map_partitions_wrapper, task, shuffle=True)

        elif task._name == 'collapsePartitions':
            def collapse_partitions_wrapper(input_iterator, key_serdes, value_serdes, output_writebatch):
                f = create_functor(functors[0]._body)
                value = f(generator(key_serdes, value_serdes, input_iterator))
                if input_iterator.last():
                    key = input_iterator.key()
                    output_writebatch.put(key, value_serdes.serialize(value))
            self._run_unary(collapse_partitions_wrapper, task)

        elif task._name == 'flatMap':
            def flat_map_wraaper(input_iterator, key_serdes, value_serdes, output_writebatch):
                f = create_functor(functors[0]._body)
                for k1, v1 in input_iterator:
                    for k2, v2 in f(key_serdes.deserialize(k1), value_serdes.deserialize(v1)):
                        output_writebatch.put(key_serdes.serialize(k2), value_serdes.serialize(v2))
            self._run_unary(flat_map_wraaper, task)

        elif task._name == 'glom':
            def glom_wrapper(input_iterator, key_serdes, value_serdes, output_writebatch):
                k_tmp = None
                v_list = []
                for k, v in input_iterator:
                    v_list.append((key_serdes.deserialize(k), value_serdes.deserialize(v)))
                    k_tmp = k
                if k_tmp is not None:
                    output_writebatch.put(k_tmp, value_serdes.serialize(v_list))
            self._run_unary(glom_wrapper, task)

        elif task._name == 'sample':
            def sample_wrapper(input_iterator, key_serdes, value_serdes, output_writebatch):
                fraction = create_functor(functors[0]._body)
                seed = create_functor(functors[1]._body)
                input_iterator.first()
                random_state = np.random.RandomState(seed)
                for k, v in input_iterator:
                    if random_state.rand() < fraction:
                        output_writebatch.put(k, v)
            self._run_unary(sample_wrapper, task)

        elif task._name == 'filter':
            def filter_wrapper(input_iterator, key_serdes, value_serdes, output_writebatch):
                f = create_functor(functors[0]._body)
                for k ,v in input_iterator:
                    if f(key_serdes.deserialize(k), value_serdes.deserialize(v)):
                        output_writebatch.put(k, v)
            self._run_unary(filter_wrapper, task)

        elif task._name == 'join':
            def merge_join_wrapper(left_iterator, left_key_serdes, left_value_serdes,
                    right_iterator, right_key_serdes, right_value_serdes,
                    output_writebatch):
                if not left_iterator.adapter.is_sorted() or not right_iterator.adapter.is_sorted():
                    raise RuntimeError(f"merge join cannot be applied: not both store types support sorting. "
                                       f"left type: {type(left_iterator.adapter)}, is_sorted: {left_iterator.adapter.is_sorted()}; "
                                       f"right type: {type(right_iterator.adapter)}, is_sorted: {right_iterator.adapter.is_sorted()}")
                f = create_functor(functors[0]._body)
                is_same_serdes = type(left_key_serdes) == type(right_key_serdes)

                l_iter = iter(left_iterator)
                r_iter = iter(right_iterator)

                try:
                    k_left, v_left_bytes = next(l_iter)
                    k_right_raw, v_right_bytes = next(r_iter)
                    if is_same_serdes:
                        k_right = k_right_raw
                    else:
                        k_right = left_key_serdes.serialize(right_key_serdes.deserialize(k_right_raw))

                    while True:
                        while k_right < k_left:
                            k_right_raw, v_right_bytes = next(r_iter)
                            if is_same_serdes:
                                k_right = k_right_raw
                            else:
                                k_right = left_key_serdes.serialize(right_key_serdes.deserialize(k_right_raw))

                        while k_left < k_right:
                            k_left, v_left_bytes = next(l_iter)

                        if k_left == k_right:
                            output_writebatch.put(k_left,
                                                  left_value_serdes.serialize(
                                                          f(left_value_serdes.deserialize(v_left_bytes),
                                                            right_value_serdes.deserialize(v_right_bytes))))
                            k_left, v_left_bytes = next(l_iter)
                            # skips next(r_iter) to avoid duplicate codes for the 3rd time
                except StopIteration as e:
                    return

            def hash_join_wrapper(left_iterator, left_key_serdes, left_value_serdes,
                    right_iterator, right_key_serdes, right_value_serdes,
                    output_writebatch):
                f = create_functor(functors[0]._body)
                is_diff_serdes = type(left_key_serdes) != type(right_key_serdes)
                for k_left, l_v_bytes in left_iterator:
                    if is_diff_serdes:
                        k_left = right_key_serdes.serialize(left_key_serdes.deserialize(k_left))
                    r_v_bytes = right_iterator.adapter.get(k_left)
                    if r_v_bytes:
                        #L.info("egg join:{}".format(right_value_serdes.deserialize(r_v_bytes)))
                        output_writebatch.put(k_left,
                                              left_value_serdes.serialize(
                                                      f(left_value_serdes.deserialize(l_v_bytes),
                                                        right_value_serdes.deserialize(r_v_bytes))))

            join_type = task._job._options.get('join_type', 'merge')

            if join_type == 'merge':
                self._run_binary(merge_join_wrapper, task)
            else:
                self._run_binary(hash_join_wrapper, task)

        elif task._name == 'subtractByKey':
            def subtract_by_key_wrapper(left_iterator, left_key_serdes, left_value_serdess,
                    right_iterator, right_key_serdes, right_value_serdess,
                    output_writebatch):
                L.info("sub wrapper")
                is_diff_serdes = type(left_key_serdes) != type(right_key_serdes)
                for k_left, v_left in left_iterator:
                    if is_diff_serdes:
                        k_left = right_key_serdes.serialize(left_key_serdes.deserialize(k_left))
                    v_right = right_iterator.adapter.get(k_left)
                    if v_right is None:
                        output_writebatch.put(k_left, v_left)
            self._run_binary(subtract_by_key_wrapper, task)

        elif task._name == 'union':
            def union_wrapper(left_iterator, left_key_serdes, left_value_serdess,
                    right_iterator, right_key_serdes, right_value_serdess,
                    output_writebatch):
                f = create_functor(functors[0]._body)

                is_diff_serdes = type(left_key_serdes) != type(right_key_serdes)
                for k_left, v_left in left_iterator:
                    if is_diff_serdes:
                        k_left = right_key_serdes.serialize(left_key_serdes.deserialize(k_left))
                    v_right = right_iterator.adapter.get(k_left)
                    if v_right is None:
                        output_writebatch.put(k_left, v_left)
                    else:
                        v_final = f(left_value_serdess.deserialize(v_left),
                                    right_value_serdess.deserialize(v_right))
                        output_writebatch.put(k_left, left_value_serdess.serialize(v_final))

                right_iterator.first()
                for k_right, v_right in right_iterator:
                    if is_diff_serdes:
                        final_v_bytes = output_writebatch.get(left_key_serdes.serialize(
                            right_key_serdes.deserialize(k_right)))
                    else:
                        final_v_bytes = output_writebatch.get(k_right)

                    if final_v_bytes is None:
                        output_writebatch.put(k_right, v_right)
            self._run_binary(union_wrapper, task)

        elif task._name == 'withStores':
            f = create_functor(functors[0]._body)
            result = ErPair(key=self.functor_serdes.serialize(task._inputs[0]._id),
                            value=self.functor_serdes.serialize(f(task._inputs)))
        return result

    def aggregate_seq(self, task: ErTask):
        functors = task._job._functors
        is_reduce = functors[0]._name == 'reduce'
        zero_value = None if is_reduce or functors[0] is None else create_functor(functors[0]._body)
        if is_reduce:
            seq_op = create_functor(functors[0]._body)
        else:
            seq_op = create_functor(functors[1]._body)

        first = True
        seq_op_result = zero_value
        input_partition = task._inputs[0]
        input_key_serdes = create_serdes(input_partition._store_locator._serdes)
        input_value_serdes = input_key_serdes

        with create_adapter(input_partition) as input_adapter, \
            input_adapter.iteritems() as input_iter:
            for k_bytes, v_bytes in input_iter:
                v = input_value_serdes.deserialize(v_bytes)
                if is_reduce and first:
                    seq_op_result = v
                    first = False
                else:
                    seq_op_result = seq_op(seq_op_result, v)

        return seq_op_result


def serve(args):
    prefix = 'v1/egg-pair'

    set_data_dir(args.data_dir)

    CommandRouter.get_instance().register(
            service_name=f"{prefix}/runTask",
            route_to_module_name="eggroll.roll_pair.egg_pair",
            route_to_class_name="EggPair",
            route_to_method_name="run_task")

    command_server = grpc.server(futures.ThreadPoolExecutor(max_workers=500, thread_name_prefix="grpc_server"),
                                 options=[
                                     ("grpc.max_metadata_size", 32 << 20),
                                     (cygrpc.ChannelArgKey.max_send_message_length, 2 << 30 - 1),
                                     (cygrpc.ChannelArgKey.max_receive_message_length, 2 << 30 - 1)])

    command_servicer = CommandServicer()
    command_pb2_grpc.add_CommandServiceServicer_to_server(command_servicer,
                                                          command_server)

    transfer_servicer = GrpcTransferServicer()

    port = args.port
    transfer_port = args.transfer_port

    port = command_server.add_insecure_port(f'[::]:{port}')

    if transfer_port == "-1":
        transfer_server = command_server
        transfer_port = port
        transfer_pb2_grpc.add_TransferServiceServicer_to_server(transfer_servicer,
                                                                transfer_server)
    else:
        transfer_server = grpc.server(futures.ThreadPoolExecutor(max_workers=500, thread_name_prefix="transfer_server"),
                                      options=[
                                          (cygrpc.ChannelArgKey.max_send_message_length, 2 << 30 - 1),
                                          (cygrpc.ChannelArgKey.max_receive_message_length, 2 << 30 - 1),
                                          ('grpc.max_metadata_size', 32 << 20)])
        transfer_port = transfer_server.add_insecure_port(f'[::]:{transfer_port}')
        transfer_pb2_grpc.add_TransferServiceServicer_to_server(transfer_servicer,
                                                                transfer_server)
        transfer_server.start()

    L.info(f"starting egg_pair service, port:{port}, transfer port: {transfer_port}")
    command_server.start()

    cluster_manager = args.cluster_manager
    myself = None
    cluster_manager_client = None
    if cluster_manager:
        session_id = args.session_id

        if not session_id:
            raise ValueError('session id is missing')
        options = {
            SessionConfKeys.CONFKEY_SESSION_ID: args.session_id
        }
        myself = ErProcessor(id=int(args.processor_id),
                             server_node_id=int(args.server_node_id),
                             processor_type=ProcessorTypes.EGG_PAIR,
                             command_endpoint=ErEndpoint(host='localhost', port=port),
                             transfer_endpoint=ErEndpoint(host='localhost', port=transfer_port),
                             pid=os.getpid(),
                             options=options,
                             status=ProcessorStatus.RUNNING)

        cluster_manager_host, cluster_manager_port = cluster_manager.strip().split(':')

        L.info(f'cluster_manager: {cluster_manager}')
        cluster_manager_client = ClusterManagerClient(options={
            ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST: cluster_manager_host,
            ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT: cluster_manager_port
        })
        cluster_manager_client.heartbeat(myself)

    L.info(f'egg_pair started at port {port}, transfer_port {transfer_port}')

    run = True

    def exit_gracefully(signum, frame):
        nonlocal run
        run = False

    signal.signal(signal.SIGTERM, exit_gracefully)
    signal.signal(signal.SIGINT, exit_gracefully)

    import time

    while run:
        time.sleep(1)

    if cluster_manager:
        myself._status = ProcessorStatus.STOPPED
        cluster_manager_client.heartbeat(myself)

    L.info(f'egg_pair at port {port}, transfer_port {transfer_port} stopped gracefully')


if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('-d', '--data-dir')
    args_parser.add_argument('-cm', '--cluster-manager')
    args_parser.add_argument('-nm', '--node-manager')
    args_parser.add_argument('-s', '--session-id')
    args_parser.add_argument('-p', '--port', default='0')
    args_parser.add_argument('-t', '--transfer-port', default='0')
    args_parser.add_argument('-sn', '--server-node-id')
    args_parser.add_argument('-prid', '--processor-id', default='0')
    args_parser.add_argument('-c', '--config')

    args = args_parser.parse_args()

    EGGROLL_HOME = os.environ['EGGROLL_HOME']
    configs = configparser.ConfigParser()
    if args.config:
        conf_file = args.config
    else:
        conf_file = f'{EGGROLL_HOME}/conf/eggroll.properties'
        print(f'reading default config: {conf_file}')

    configs.read(conf_file)
    set_static_er_conf(configs['eggroll'])
    if configs:
        if not args.data_dir:
            args.data_dir = configs['eggroll']['eggroll.data.dir']

    L.info(args)
    serve(args)
