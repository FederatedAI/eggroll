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

from collections.abc import Iterable

import grpc
from concurrent import futures

import numpy as np

from eggroll.core.client import NodeManagerClient
from eggroll.core.command.command_router import CommandRouter
from eggroll.core.command.command_service import CommandServicer
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.io.kv_adapter import RocksdbSortedKvAdapter, LmdbSortedKvAdapter
from eggroll.core.io.io_utils import get_db_path
from eggroll.core.meta_model import ErTask, ErPartition, ErProcessor, ErEndpoint
from eggroll.core.meta_model import ErSessionMeta, ErPair
from eggroll.core.proto import command_pb2_grpc, transfer_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.core.serdes import eggroll_serdes
from eggroll.core.utils import hash_code
from eggroll.utils import log_utils
from eggroll.core.transfer.transfer_service import GrpcTransferServicer, \
  TransferClient
from eggroll.core.constants import ProcessorTypes, ProcessorStatus, SerdesTypes
from eggroll.roll_pair.shuffler import DefaultShuffler
from eggroll.core.conf_keys import NodeManagerConfKeys, SessionConfKeys
from grpc._cython import cygrpc
import argparse
import os

log_utils.setDirectory()
LOGGER = log_utils.getLogger()

def generator(serde, iterator):
  for k, v in iterator:
    print("yield ({}, {})".format(serde.deserialize(k), serde.deserialize(v)))
    yield serde.deserialize(k), serde.deserialize(v)


class EggPair(object):
  uri_prefix = 'v1/egg-pair'
  GET = 'get'
  PUT = 'put'
  GET_ALL = 'getAll'
  PUT_ALL = 'putAll'

  def __init__(self):
    self.serde = self._create_serdes(SerdesTypes.PICKLE)

  def get_unary_input_adapter(self, task_info: ErTask, create_if_missing=True):
    input_partition = task_info._inputs[0]
    print("input_adapter: ", input_partition, "path: ",
          get_db_path(input_partition))
    input_adapter = None

    options = dict()
    options ["create_if_missing"] = create_if_missing

    if task_info._inputs[0]._store_locator._store_type == "rollpair.lmdb":
      options["path"] = get_db_path(partition=input_partition)
      input_adapter = LmdbSortedKvAdapter(options=options)
    elif task_info._inputs[0]._store_locator._store_type == "rollpair.leveldb":
      options["path"] = get_db_path(partition=input_partition)
      input_adapter = RocksdbSortedKvAdapter(options=options)
    return input_adapter

  def get_binary_input_adapter(self, task_info: ErTask, create_if_missing=True):
    left_partition = task_info._inputs[0]
    right_partition = task_info._inputs[1]
    print("left partition: ", left_partition, "path: ",
          get_db_path(left_partition))
    print("right partition: ", right_partition, "path: ",
          get_db_path(right_partition))
    left_adapter = None
    right_adapter = None

    options = dict()
    options ["create_if_missing"] = create_if_missing

    if task_info._inputs[0]._store_locator._store_type == "rollpair.lmdb":
      options["path"] = get_db_path(partition=left_partition)
      left_adapter = LmdbSortedKvAdapter(options=options)

      options["path"] = get_db_path(partition=right_partition)
      right_adapter = LmdbSortedKvAdapter(options=options)
    elif task_info._inputs[0]._store_locator._store_type == "rollpair.leveldb":
      options["path"] = get_db_path(partition=left_partition)
      left_adapter = RocksdbSortedKvAdapter(options=options)

      options["path"] = get_db_path(partition=right_partition)
      right_adapter = RocksdbSortedKvAdapter(options=options)

    return left_adapter, right_adapter

  def get_unary_output_adapter(self, task_info: ErTask, create_if_missing=True):
    output_partition = task_info._inputs[0]
    print("output_partition: ", output_partition, "path: ",
          get_db_path(output_partition))
    output_adapter = None
    options = dict()
    options ["create_if_missing"] = create_if_missing
    options["path"] = get_db_path(partition=output_partition)
    if task_info._inputs[0]._store_locator._store_type == "rollpair.lmdb":
      output_adapter = LmdbSortedKvAdapter(options=options)
    elif task_info._inputs[0]._store_locator._store_type == "rollpair.leveldb":
      output_adapter = RocksdbSortedKvAdapter(options=options)
    return output_adapter

  def _create_serdes(self, serdes_name):
    return eggroll_serdes.get_serdes(serdes_name)

  def __partitioner(self, hash_func, total_partitions):
    def partitioner_wrapper(k):
      return hash_func(k) % total_partitions
    return partitioner_wrapper

  def run_task(self, task: ErTask):
    LOGGER.info("start run task")
    functors = task._job._functors
    result = task

    if task._name == 'get':
      LOGGER.info("egg_pair get call")
      f = cloudpickle.loads(functors[0]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      value = input_adapter.get(f._key)
      print("value:{}".format(value))
      result = ErPair(key=f._key, value=value)
      input_adapter.close()
    elif task._name == 'getAll':
      LOGGER.info("egg_pair getAll call")
      f = cloudpickle.loads(functors[0]._body)
      LOGGER.info("get roll revc port:{}".format(f._port))
      input_partition = task._inputs[0]
      #roll_client_transfer_endpoint = cloudpickle.loads(functors[0]._body).split(":")
      rct_host = f._host
      rct_port = f._port
      LOGGER.info("host:{}, port:{}".format(rct_host, rct_port))

      # todo: decide partitioner
      #p = lambda k : k[-1] % output_partition._store_locator._total_partitions
      total_partitions = input_partition._store_locator._total_partitions
      partitioner = self.__partitioner(hash_func=hash_code, total_partitions=total_partitions)
      partition_id = partitioner(get_db_path(input_partition)[:-1])

      input_adapter = self.get_unary_input_adapter(task_info=task)

      broker = FifoBroker()
      transfer = TransferClient()
      LOGGER.info("job id:{}".format(task._job._id))
      future = transfer.send_pair(broker, task._job._id,
                                  ErProcessor(command_endpoint=ErEndpoint(host=rct_host, port=rct_port)))
      i = 0
      for k_bytes, v_bytes in input_adapter.iteritems():
        broker.put(bytes(i), bytes(i))
        i = i + 1
      LOGGER.info('finish calculating')
      input_adapter.close()
      #while broker.is_read_ready():
        #LOGGER.info("read broker:{}".format(broker.get()))
      broker.signal_write_finish()

      future.result()
      LOGGER.info('getAll finished')
    elif task._name == 'putAll':
      print("egg_pair putAll call")
      output_partition = task._outputs[0]
      print(output_partition)

      # todo: decide partitioner
      p = lambda k : k[-1] % output_partition._store_locator._total_partitions
      output_store = task._job._outputs[0]

      shuffle_broker = FifoBroker()
      shuffler = DefaultShuffler(task._job._id, shuffle_broker, output_store, output_partition, p)
      shuffler.start()
      shuffle_finished = shuffler.wait_until_finished(600)

    if task._name == 'put':
      print("egg_pair put call")
      f = cloudpickle.loads(functors[0]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      value = input_adapter.put(f._key, f._value)
      #result = ErPair(key=f._key, value=bytes(value))
      input_adapter.close()

    if task._name == 'mapValues':
      f = cloudpickle.loads(functors[0]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      for k_bytes, v_bytes in input_iterator:
        v = self.serde.deserialize(v_bytes)
        output_writebatch.put(k_bytes, self.serde.serialize(f(v)))

      output_writebatch.close()
      input_adapter.close()
      output_adapter.close()
    elif task._name == 'map':
      f = cloudpickle.loads(functors[0]._body)

      input_partition = task._inputs[0]
      output_partition = task._outputs[0]
      print(output_partition)

      p = output_partition._store_locator._partitioner

      # todo: decide partitioner
      p = lambda k : k[-1] % output_partition._store_locator._total_partitions
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_store = task._job._outputs[0]

      shuffle_broker = FifoBroker()
      shuffler = DefaultShuffler(task._job._id, shuffle_broker, output_store, output_partition, p)

      for k_bytes, v_bytes in input_adapter.iteritems():
        k1, v1 = f(self.serde.deserialize(k_bytes), self.serde.deserialize(v_bytes))
        shuffle_broker.put(self.serde.serialize(k1), self.serde.serialize(v1))
      print('finish calculating')
      input_adapter.close()
      shuffle_broker.signal_write_finish()

      shuffler.start()

      shuffle_finished = shuffler.wait_until_finished(600)

      print('map finished')
    # todo: use aggregate to reduce (also reducing duplicate codes)
    elif task._name == 'reduce':
      f = cloudpickle.loads(functors[0]._body)

      input_adapter = self.get_unary_input_adapter(task_info=task)
      seq_op_result = None
      input_serdes = self._create_serdes(task._inputs[0]._store_locator._serdes)

      print('mw: ready to do reduce')
      for k_bytes, v_bytes in input_adapter.iteritems():
        if seq_op_result:
          #seq_op_result = f(seq_op_result, self.serde.deserialize(v_bytes))
          seq_op_result = f(seq_op_result, input_serdes.deserialize(v_bytes))
        else:
          seq_op_result = input_serdes.deserialize(v_bytes)

      print(f'mw: seq_op_result: {seq_op_result}')
      partition_id = task._inputs[0]._id
      transfer_tag = task._job._name

      if 0 == partition_id:
        queue = GrpcTransferServicer.get_or_create_broker(transfer_tag)
        partition_size = len(task._job._inputs[0]._partitions)

        comb_op_result = seq_op_result

        for i in range(1, partition_size):
          other_seq_op_result = queue.get(block=True, timeout=10)

          comb_op_result = f(comb_op_result, other_seq_op_result.data)

        print('reduce finished. result: ', comb_op_result)
        output_adapter = self.get_unary_output_adapter(task_info=task)

        output_writebatch = output_adapter.new_batch()

        output_writebatch.put(self.serde.deserialize('result'.encode()), self.serde.deserialize(comb_op_result))

        output_writebatch.close()
        output_adapter.close()
      else:
        transfer_client = TransferClient()
        transfer_client.send_single(data=seq_op_result, tag=transfer_tag,
                                    processor=task._outputs[0]._processor)

      input_adapter.close()
      print('reduce finished')

    elif task._name == 'mapPartitions':
      f = cloudpickle.loads(functors[0]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      value = f(generator(self.serde, input_iterator))
      if input_iterator.last():
        print("value of mapPartitions2:{}".format(value))
        if isinstance(value, Iterable):
          for k1, v1 in value:
            output_writebatch.put(self.serde.serialize(k1), self.serde.serialize(v1))
        else:
          key = input_iterator.key()
          output_writebatch.put(key, self.serde.serialize(value))

      output_writebatch.close()
      output_adapter.close()
      input_adapter.close()

    elif task._name == 'collapsePartitions':
      f = cloudpickle.loads(functors[0]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      value = f(generator(self.serde, input_iterator))
      if input_iterator.last():
        key = input_iterator.key()
        output_writebatch.put(key, self.serde.serialize(value))

      output_writebatch.close()
      output_adapter.close()
      input_adapter.close()

    elif task._name == 'flatMap':
      f = cloudpickle.loads(functors[0]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      for k1, v1 in input_iterator:
        for k2, v2 in f(self.serde.deserialize(k1), self.serde.deserialize(v1)):
          output_writebatch.put(self.serde.serialize(k2), self.serde.serialize(v2))

      output_writebatch.close()
      output_adapter.close()
      input_adapter.close()

    elif task._name == 'glom':
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      k_tmp = None
      v_list = []
      for k, v in input_iterator:
        v_list.append((self.serde.deserialize(k), self.serde.deserialize(v)))
        k_tmp = k
      if k_tmp is not None:
        output_writebatch.put(k_tmp, self.serde.serialize(v_list))

      output_writebatch.close()
      output_adapter.close()
      input_adapter.close()

    elif task._name == 'sample':
      fraction = cloudpickle.loads(functors[0]._body)
      seed = cloudpickle.loads(functors[1]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      input_iterator.first()
      random_state = np.random.RandomState(seed)
      for k, v in input_iterator:
        if random_state.rand() < fraction:
          output_writebatch.put(k, v)

      output_writebatch.close()
      output_adapter.close()
      input_adapter.close()

    elif task._name == 'filter':
      f = cloudpickle.loads(functors[0]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      for k ,v in input_iterator:
        if f(self.serde.deserialize(k), self.serde.deserialize(v)):
          output_writebatch.put(k, v)

      output_writebatch.close()
      output_adapter.close()
      input_adapter.close()

    elif task._name == 'aggregate':
      self.aggregate(task)
      print('aggregate finished')

    elif task._name == 'join':
      f = cloudpickle.loads(functors[0]._body)
      left_adapter, right_adapter = self.get_binary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      left_iterator = left_adapter.iteritems()
      right_iterator = right_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      for k_bytes, l_v_bytes in left_iterator:
        r_v_bytes = right_adapter.get(k_bytes)
        if r_v_bytes:
          output_writebatch.put(k_bytes,
                                self.serde.serialize(f(self.serde.deserialize(l_v_bytes),
                                                       self.serde.deserialize(r_v_bytes))))

      output_writebatch.close()
      left_adapter.close()
      right_adapter.close()
      output_adapter.close()

    elif task._name == 'subtractByKey':
      left_adapter, right_adapter = self.get_binary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      left_iterator = left_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      for k_left, v_left in left_iterator:
        v_right = right_adapter.get(k_left)
        if v_right is None:
          output_writebatch.put(k_left, v_left)

      output_writebatch.close()
      output_adapter.close()
      left_adapter.close()
      right_adapter.close()

    elif task._name == 'union':
      f = cloudpickle.loads(functors[0]._body)
      left_adapter, right_adapter = self.get_binary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      left_iterator = left_adapter.iteritems()
      right_iterator = right_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      #store the iterator that has been iterated before
      k_list_iterated = []

      for k_left, v_left in left_iterator:
        v_right = right_adapter.get(k_left)
        if v_right is None:
          output_writebatch.put(k_left, v_left)
        else:
          k_list_iterated.append(self.serde.deserialize(v_left))
          v_final = f(v_left, v_right)
          output_writebatch.put(k_left, v_final)

      for k_right, v_right in right_iterator:
        if self.serde.deserialize(k_right) not in k_list_iterated:
          output_writebatch.put(k_right, v_right)

      output_writebatch.close()
      output_adapter.close()
      left_adapter.close()
      right_adapter.close()

    return result

  def aggregate(self, task: ErTask):
    functors = task._job._functors
    zero_value = cloudpickle.loads(functors[0]._body)
    seq_op = cloudpickle.loads(functors[1]._body)
    comb_op = cloudpickle.loads(functors[2]._body)

    input_partition = task._inputs[0]
    input_adapter = self.get_unary_input_adapter(task_info=task)
    input_serdes = self._create_serdes(task._inputs[0]._store_locator._serdes)
    output_serdes = self._create_serdes(task._outputs[0]._store_locator._serdes)

    seq_op_result = zero_value if zero_value is not None else None

    for k_bytes, v_bytes in input_adapter.iteritems():
      if seq_op_result:
        seq_op_result = seq_op(seq_op_result, input_serdes.deserialize(v_bytes))
      else:
        seq_op_result = input_serdes.deserialize(v_bytes)

    partition_id = input_partition._id
    transfer_tag = task._job._name

    if 0 == partition_id:
      queue = GrpcTransferServicer.get_or_create_broker(transfer_tag)
      partition_size = len(task._job._inputs[0]._partitions)

      comb_op_result = seq_op_result

      for i in range(1, partition_size):
        other_seq_op_result = queue.get(block=True, timeout=10)

        comb_op_result = comb_op(comb_op_result, output_serdes.deserialize(other_seq_op_result.data))

      print('aggregate finished. result: ', comb_op_result)
      output_partition = task._outputs[0]
      output_adapter = self.get_unary_output_adapter(task_info=task)

      output_writebatch = output_adapter.new_batch()
      output_writebatch.put(output_serdes.serialize('result'.encode()), output_serdes.serialize(comb_op_result))

      output_writebatch.close()
      output_adapter.close()
    else:
      ser_seq_op_result = output_serdes.serialize(seq_op_result)
      transfer_client = TransferClient()
      transfer_client.send_single(data=ser_seq_op_result, tag=transfer_tag,
                                  processor=task._outputs[0]._processor)

    input_adapter.close()
    print('reduce finished')


def serve(args):
  prefix = 'v1/egg-pair'

  #storage api
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/get",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/put",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/getAll",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/putAll",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")

  #computing api
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/mapValues",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
      service_name=f"{prefix}/map",
      route_to_module_name="eggroll.roll_pair.egg_pair",
      route_to_class_name="EggPair",
      route_to_method_name="run_task")
  CommandRouter.get_instance().register(
      service_name=f"{prefix}/reduce",
      route_to_module_name="eggroll.roll_pair.egg_pair",
      route_to_class_name="EggPair",
      route_to_method_name="run_task")
  CommandRouter.get_instance().register(
      service_name=f"{prefix}/join",
      route_to_module_name="eggroll.roll_pair.egg_pair",
      route_to_class_name="EggPair",
      route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/mapPartitions",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/collapsePartitions",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/flatMap",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/glom",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/sample",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/filter",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/subtractByKey",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/union",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
      service_name=f"{prefix}/runTask",
      route_to_module_name="eggroll.roll_pair.egg_pair",
      route_to_class_name="EggPair",
      route_to_method_name="run_task")

  server = grpc.server(futures.ThreadPoolExecutor(max_workers=1),
                       options=[
                         (cygrpc.ChannelArgKey.max_send_message_length, -1),
                         (cygrpc.ChannelArgKey.max_receive_message_length, -1)])

  command_servicer = CommandServicer()
  # todo: register egg_pair methods
  command_pb2_grpc.add_CommandServiceServicer_to_server(command_servicer,
                                                        server)

  transfer_servicer = GrpcTransferServicer()
  transfer_pb2_grpc.add_TransferServiceServicer_to_server(transfer_servicer,
                                                          server)
  port = args.port
  port = server.add_insecure_port(f'[::]:{port}')
  LOGGER.info("starting egg_pair service, port:{}".format(port))
  server.start()


  node_manager = args.node_manager
  if node_manager:
    session_id = args.session_id

    if not session_id:
      raise ValueError('session id is missing')
    options = {
      SessionConfKeys.CONFKEY_SESSION_ID: args.session_id
    }
    myself = ErProcessor(processor_type=ProcessorTypes.EGG_PAIR,
                         command_endpoint=ErEndpoint(host='localhost', port=port),
                         data_endpoint=ErEndpoint(host='localhost', port=port),
                         options=options,
                         status=ProcessorStatus.RUNNING)

    if node_manager.find(':') == -1:
      node_manager_host = 'localhost'
      node_manager_port = node_manager.strip()
    else:
      node_manager_host, node_manager_port = node_manager.strip().split(':', 1)

    print(f'node_manager: {node_manager_host}:{node_manager_port}')
    node_manager_client = NodeManagerClient(options = {
      NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HOST: node_manager_host,
      NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT: node_manager_port
    })
    node_manager_client.heartbeat(myself)

  print(f'egg_pair started at port {port}')

  import time
  time.sleep(100000)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-d', '--data-dir', default=os.path.dirname(os.path.realpath(__file__)))
  parser.add_argument('-n', '--node-manager')
  parser.add_argument('-s', '--session-id')
  parser.add_argument('-p', '--port', default='0')

  args = parser.parse_args()
  serve(args)
