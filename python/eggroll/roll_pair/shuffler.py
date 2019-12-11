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

import queue
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION

from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.datastructure.concurrent import CountDownLatch
from eggroll.core.io.io_utils import get_db_path
from eggroll.core.io.kv_adapter import LmdbSortedKvAdapter
from eggroll.core.meta_model import ErStore, ErPartition
from eggroll.core.pair_store.format import PairBinReader, PairBinWriter, \
    ArrayByteBuffer
from eggroll.core.transfer.transfer_service import TransferClient, \
    TransferService
from eggroll.core.utils import _exception_logger


class Shuffler(object):
    def start(self):
        raise NotImplementedError()

    def is_finished(self):
        raise NotImplementedError()

    def wait_until_finished(self, timeout):
        raise NotImplementedError()

    def get_total_partitioned_count(self):
        raise NotImplementedError()

    def has_error(self):
        raise NotImplementedError()

    def get_error(self):
        raise NotImplementedError()

class DefaultShuffler(Shuffler):
    def __init__(self,
            shuffle_id,
            input_broker,
            output_store: ErStore,
            output_partition: ErPartition,
            partition_function,
            parallel_size = 1):
        # params from __init__ params
        self.__shuffle_id = shuffle_id
        self.__input_broker = input_broker
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

        self.__partition_thread_pool = ThreadPoolExecutor(max_workers=self.__parallel_size)
        self.__send_thread_pool = ThreadPoolExecutor(max_workers=self.__output_partitions_count)
        self.__recv_thread_pool = ThreadPoolExecutor(max_workers=1)

        self.__partition_futures = list()
        self.__send_futures = list()
        self.__recv_futures = list()


    @_exception_logger
    def start(self):
        TransferService \
            .get_or_create_broker(key=f'{self.__shuffle_id}-{self.__output_partition._id}',
                                  write_signals=self.__output_partitions_count)

        partition_id = self.__output_partition._id
        # todo: move partitioning to processes when needed

        for i in range(self.__parallel_size):
            future = self.__partition_thread_pool\
                .submit(partitioner,
                        self.__input_broker,
                        self.__partitioned_brokers,
                        self.__partition_function)
            self.__partition_futures.append(future)

        for i in range(self.__output_partitions_count):
            future = self.__send_thread_pool.submit(
                    grpc_shuffle_sender,
                    tag=f'{self.__shuffle_id}-{i}',
                    input_broker=self.__partitioned_brokers[i],
                    target_partition=self.__output_partitions[i])
            self.__send_futures.append(future)

        receiver_future = self.__recv_thread_pool\
            .submit(grpc_shuffle_receiver,
                    f'{self.__shuffle_id}-{self.__output_partition._id}',
                    self.__output_partition,
                    self.__output_partitions_count)
        self.__recv_futures.append(receiver_future)

    def join(self):
        print('joining')
        partition_finished, partition_not_finished = wait(self.__partition_futures, return_when=FIRST_EXCEPTION)
        for broker in self.__partitioned_brokers:
            broker.signal_write_finish()
        if len(partition_finished) == len(self.__partition_futures):
            print('finishing partition normally')
            self.__is_partition_finished = True
            self.__shuffle_finish_latch.count_down()
        else:
            print('finishing partition abnormally')
            for e in partition_finished:
                raise e.exception(timeout=5)

        for future in self.__send_futures:
            try:
                print('finishing send normally')
                result = future.result()
                print('send actually finished')
            except Exception as e:
                print('finishing send with exception: ', e)
                raise e
            self.__shuffle_finish_latch.count_down()

        try:
            print('finishing recv normally')
            self.__recv_futures[0].result()
        except Exception as e:
            print('finishing recv with exception', e)
            raise e
        self.__shuffle_finish_latch.count_down()

        TransferService.remove_broker(key=f'{self.__shuffle_id}-{self.__output_partition._id}')

        self.__partition_thread_pool.shutdown(wait=False)
        self.__send_thread_pool.shutdown(wait=False)
        self.__recv_thread_pool.shutdown(wait=False)

    def is_finished(self):
        return self.__shuffle_finish_latch.get_count() == 0

    def wait_until_finished(self, timeout):
        return self.__shuffle_finish_latch.await_timeout(timeout=timeout)

    def get_total_partitioned_count(self):
        return self.__total_partitioned_elements_count


@_exception_logger
def partitioner(input: FifoBroker, partitioned_brokers, partition_func):
    print('partitioner started')
    partitioned_elements_count = 0

    while not input.is_closable():
        try:
            pair = input.get(block=True, timeout=1)

            if pair:
                partitioned_brokers[partition_func(pair[0])].put(pair)
                partitioned_elements_count += 1
        except queue.Empty as pair:
            print('partition broker is empty')

    return partitioned_elements_count


@_exception_logger
def grpc_shuffle_sender(tag: str, input_broker, target_partition, buffer_size=32 << 20):
    print('sender started')
    client = TransferClient()
    send_broker = FifoBroker()
    future = client.send(broker=send_broker, endpoint=target_partition._processor._data_endpoint, tag=tag)

    ba = None
    buffer = None
    writer = None

    def commit():
        nonlocal ba
        nonlocal buffer
        nonlocal writer
        if ba:
            bin_batch = bytes(ba[0:buffer.get_offset()])
            send_broker.put(bin_batch)
        ba = bytearray(buffer_size)
        buffer = ArrayByteBuffer(ba)
        writer = PairBinWriter(pair_buffer=buffer)

    commit()
    while not input_broker.is_closable():
        try:
            pair = input_broker.get(block=True, timeout=1)
            writer.write(pair[0], pair[1])
        except IndexError as e:
            commit()
            writer.write(pair[0], pair[1])
        except queue.Empty:
            pass

    commit()
    send_broker.signal_write_finish()

    return future.result()


@_exception_logger
def grpc_shuffle_receiver(tag, output_partition, total_parititions_count):
    print('receiver started')
    total_written = 0

    path = get_db_path(output_partition)
    output_adapter = LmdbSortedKvAdapter({'path': path})
    output_write_batch = output_adapter.new_batch()
    broker = TransferService.get_or_create_broker(tag, write_signals=total_parititions_count)

    while not broker.is_closable():
        try:
            transfer_batch = broker.get(block=True, timeout=1)
            if transfer_batch:
                bin_data = ArrayByteBuffer(transfer_batch.data)
                reader = PairBinReader(pair_buffer=bin_data)
                for k_bytes, v_bytes in reader.read_all():
                    output_write_batch.put(k_bytes, v_bytes)
                    total_written += 1
        except IndexError as e:
            output_write_batch.write()
        except queue.Empty as e:
            pass

    output_write_batch.write()
    output_write_batch.close()
    output_adapter.close()

    TransferService.remove_broker(tag)

    return total_written 
