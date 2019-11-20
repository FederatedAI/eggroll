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
from eggroll.core.meta_model import ErFunctor, ErPair, ErPairBatch
from eggroll.core.io.kv_adapter import RocksdbSortedKvAdapter
from eggroll.core.io.io_utils import get_db_path
from eggroll.core.transfer.transfer_service import GrpcTransferServicer, \
  TransferClient
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count


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
      parallel_size = 5):
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
    #self.__partition_finish_latch = CountDownLatch(self.__parallel_size)
    self.__partition_finish_latch = CountDownLatch(1)
    self.__shuffle_finish_latch = CountDownLatch(count = 3)

    self.__not_finished_partition_thread_count = CountDownLatch(self.__parallel_size)
    self.__is_send_finished = False
    self.__is_recv_finished = False

    self.__total_partitioned_elements_count = 0

  # todo: move map calculation to subprocesses
  def start(self):
    GrpcTransferServicer\
      .get_or_create_broker(key = f'{self.__shuffle_id}-{self.__output_partition._id}',
                            write_signals = self.__output_partitions_count)

    print('start shuffle')
    partitioner(self.__map_results_broker, self.__partitioned_brokers, self.__partition_function)
    for b in self.__partitioned_brokers:
      b.signal_write_finish()
    self.__partition_finish_latch.count_down()
    self.__shuffle_finish_latch.count_down()

    print('start send')
    shuffle_sender(self.__shuffle_id, self.__partitioned_brokers, self.__output_partitions)
    self.__shuffle_finish_latch.count_down()

    print('start receive')
    shuffle_receiver(self.__shuffle_id, self.__output_partition, self.__output_partitions_count)
    self.__shuffle_finish_latch.count_down()

    print('shuffle finished')

    """
    with ProcessPoolExecutor(max_workers = self.__parallel_size + 2) as executor:
      for i in range(self.__parallel_size):
        executor.submit(partitioner,
                        self.__map_results_broker,
                        self.__partitioned_brokers,
                        self.__partition_function)\
          .add_done_callback(
            lambda future:
              partitioner_callback(future,
                                 self.__not_finished_partition_thread_count,
                                 self.__partitioned_brokers,
                                 self.__shuffle_finish_latch,
                                 self.__total_partitioned_elements_count))

        executor.submit(shuffle_sender,
                        self.__shuffle_id,
                        self.__partitioned_brokers,
                        self.__output_partitions)\
          .add_done_callback(
            lambda future:
              shuffle_sender_callback(future,
                                      self.__is_send_finished,
                                      self.__shuffle_finish_latch))

        executor.submit(shuffle_receiver,
                        self.__shuffle_id,
                        self.__output_partition,
                        self.__output_partitions_count)\
          .add_done_callback(
            lambda future:
            shuffle_receiver_callback(future,
                                      self.__is_recv_finished,
                                      self.__shuffle_finish_latch))
    """

  def is_finished(self):
    return self.__shuffle_finish_latch.get_count() == 0

  def wait_until_finished(self, timeout):
    return self.__shuffle_finish_latch.await_timeout(timeout=timeout)

  def get_total_partitioned_count(self):
    return self.__total_partitioned_elements_count



def partitioner(input: FifoBroker, output, partition_func):
  partitioned_elements_count = 0
  chunk = list()
  input.drain_to(chunk)

  for e in chunk:
    output[partition_func(e[0])].put(e)
    partitioned_elements_count += 1

  return partitioned_elements_count

def partitioner_callback(future,
    not_finished_partition_thread_count,
    partitioned_brokers,
    shuffle_finish_latch,
    total_partitioned_elements_count):
  print('partitioner_callback')
  not_finished_partition_thread_count.count_down()
  if not_finished_partition_thread_count.get_count() <= 0:
    for b in partitioned_brokers:
      b.signal_write_finish()
    shuffle_finish_latch.count_down()
  #total_partitioned_elements_count += future.result()

def shuffle_sender(shuffle_id: str, brokers, targetPartitions, chunk_size = 100):
  not_finished_broker_index = set(range(len(brokers)))
  transfer_client = TransferClient()
  newly_finished_idx = list()
  total_sent = 0

  while not_finished_broker_index:
    for idx in not_finished_broker_index:
      send_buffer = list()
      broker = brokers[idx]
      is_broker_closable = False

      if broker.is_write_finished() or broker.size() >= chunk_size:
        broker.drain_to(send_buffer, chunk_size)
        is_broker_closable = broker.is_closable()

      if send_buffer:
        pairs = list()

        for t in send_buffer:
          pairs.append(ErPair(t[0], t[1]))

        pair_batch = ErPairBatch(pairs = pairs)

        transfer_status = ''
        if is_broker_closable:
          transfer_status = GrpcTransferServicer.TRANSFER_END
          newly_finished_idx.append(idx)

        tag = f'{shuffle_id}-{idx}'
        print(f'sending to {tag}')
        transfer_client.send(data=pair_batch.to_proto().SerializeToString(),
                             tag = tag,
                             processor= targetPartitions[idx]._node,
                             status = transfer_status)
        total_sent += len(pairs)

    for idx in newly_finished_idx:
      not_finished_broker_index.remove(idx)
    newly_finished_idx = set()

  print(f'total sent: {total_sent}')
  return total_sent

def shuffle_sender_callback(future, is_send_finished, shuffle_finish_latch):
  print('shuffle_sender_callback')
  is_send_finished = True
  shuffle_finish_latch.count_down()

def shuffle_receiver(shuffle_id, output_partition, total_paritions_count):
  total_write = 0
  broker_id = f'{shuffle_id}-{output_partition._id}'
  print(f'broker_id: {broker_id}')
  broker = GrpcTransferServicer.get_or_create_broker(broker_id)

  path = get_db_path(output_partition)
  output_adapter = RocksdbSortedKvAdapter({'path': path})
  output_writebatch = output_adapter.new_batch()

  while not broker.is_closable():
    bin_data = broker.get(timeout=1)
    if bin_data:
      pair_batch = ErPairBatch.from_proto_string(bin_data)
      for er_pair in pair_batch._pairs:
        output_writebatch.put(er_pair._key, er_pair._value)

      total_write += len(pair_batch._pairs)
  output_writebatch.close()
  output_adapter.close()

  return total_write

def shuffle_receiver_callback(future, is_recv_finished, shuffle_finish_latch):
  print('shuffle_receiver_callback')
  is_recv_finished = True
  shuffle_finish_latch.count_down()