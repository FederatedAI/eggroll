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

from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.datastructure.concurrent import CountDownLatch
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErTask, ErPartition
from eggroll.core.io.kv_adapter import RocksdbSortedKvAdapter
from eggroll.core.io.io_utils import get_db_path
from eggroll.core.transfer.transfer_service import GrpcTransferServicer, \
  TransferClient
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_EXCEPTION
from multiprocessing import cpu_count
from eggroll.core.io.format import BinBatchReader
import asyncio


class Shuffler(object):
  def start(self):
    raise NotImplementedError()

  def is_finished(self):
    raise NotImplementedError()

  def wait_until_finished(self, timeout):
    raise NotImplementedError()

  def get_total_partitioned_count(self):
    raise NotImplementedError()

  def hasError(self):
    raise NotImplementedError()

  def getError(self):
    raise NotImplementedError()

class DefaultShuffler(Shuffler):
  def __init__(self,
      shuffle_id,
      broker,
      output_store: ErStore,
      output_partition: ErPartition,
      partition_function,
      parallel_size = 1):
    # params from __init__ params
    self.__shuffle_id = shuffle_id
    self.__map_results_broker = broker
    self.__output_store = output_store
    self.__output_partition = output_partition
    self.__partition_function = partition_function
    self.__parallel_size = parallel_size

    self.__output_partitions = output_store._partitions
    self.__output_partitions_count = len(self.__output_partitions)

    self.__partitioned_brokers = [FifoBroker() for i in range(self.__output_partitions_count)]

    self.__is_partition_finished = False
    self.__is_send_finished = False
    self.__is_recv_finished = False

    self.__total_partitioned_elements_count = 0
    self.__shuffle_finish_latch = CountDownLatch(3)

  # todo: move map calculation to subprocesses
  def start(self):
    GrpcTransferServicer\
      .get_or_create_broker(key = f'{self.__shuffle_id}-{self.__output_partition._id}',
                            write_signals = self.__output_partitions_count)

    partition_id = self.__output_partition._id
    # todo: move partitioning to processes when needed
    with ThreadPoolExecutor(max_workers = (self.__parallel_size << 1) + 2) as executor:
      partitioner_futures = list()
      for i in range(self.__parallel_size):
        future = executor.submit(partitioner,
                                 self.__map_results_broker,
                                 self.__partitioned_brokers,
                                 self.__partition_function)
        partitioner_futures.append(future)

      sender_future = executor.submit(grpc_shuffle_sender,
                                      self.__shuffle_id,
                                      self.__partitioned_brokers,
                                      self.__output_partitions)


      receiver_future = executor.submit(grpc_shuffle_receiver,
                                        self.__shuffle_id,
                                        self.__output_partition,
                                        self.__output_partitions_count)

      print('checking partition status')
      partition_finished, partition_not_finished = wait(partitioner_futures, return_when=FIRST_EXCEPTION)

      if len(partition_finished) == len(partitioner_futures):
        for broker in self.__partitioned_brokers:
          broker.signal_write_finish()
        self.__is_partition_finished = True
        self.__shuffle_finish_latch.count_down()
      else:
        raise ValueError(f'length of partition_finished {len(partition_finished)} != length of partitioner_futures {len(partitioner_futures)}')

      print(f'{partition_id} checking send status')
      send_finished = sender_future.result()
      for future in send_finished:
        ex = future.exception()
        if ex:
          print("exception:", ex)
          raise ex

        self.__is_send_finished = True
        self.__shuffle_finish_latch.count_down()

      print(f'{partition_id} checking recv status')
      receiver_future.result()
      self.__is_recv_finished = True
      self.__shuffle_finish_latch.count_down()


  def is_finished(self):
    return self.__shuffle_finish_latch.count() == 0

  def wait_until_finished(self, timeout):
    return self.__shuffle_finish_latch.await_timeout(timeout=timeout)

  def get_total_partitioned_count(self):
    return self.__total_partitioned_elements_count


def partitioner(input: FifoBroker, partitioned_brokers, partition_func):
  partitioned_elements_count = 0

  while not input.is_closable():
    e = input.get(block=True, timeout = 1)

    if e:
      partitioned_brokers[partition_func(e[0])].put(e)
      partitioned_elements_count += 1

  return partitioned_elements_count


def grpc_shuffle_sender(shuffle_id: str, brokers, target_partitions, chunk_size = 100):
  futures = list()

  for idx in range(len(brokers)):
    client = TransferClient()
    tag = f'{shuffle_id}-{idx}'
    future = client.send_pair(broker=brokers[idx], tag=tag, processor=target_partitions[idx]._processor)
    futures.append(future)
  return futures


def grpc_shuffle_receiver(shuffle_id, output_partition, total_parititions_count):
  total_written = 0
  broker_id = f'{shuffle_id}-{output_partition._id}'

  path = get_db_path(output_partition)
  output_adapter = RocksdbSortedKvAdapter({'path': path})
  output_write_batch = output_adapter.new_batch()
  broker = GrpcTransferServicer.get_or_create_broker(broker_id, write_signals=total_parititions_count)

  while not broker.is_closable():
    proto_transfer_batch = broker.get(block=True, timeout=1)
    if proto_transfer_batch:
      bin_data = proto_transfer_batch.data
      reader = BinBatchReader(pair_batch=bin_data)
      try:
        while reader.has_remaining():
          key_size = reader.read_int()
          key = reader.read_bytes(size=key_size)
          value_size = reader.read_int()
          value = reader.read_bytes(size=value_size)
          output_write_batch.put(key, value)
          total_written += 1
      except IndexError as e:
        print('finish processing an output')
        output_write_batch.write()

  output_write_batch.close()
  output_adapter.close()

  GrpcTransferServicer.remove_broker(broker_id)

  return total_written
