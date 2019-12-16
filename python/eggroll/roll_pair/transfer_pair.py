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
from eggroll.core.meta_model import ErStore
from eggroll.core.pair_store.format import PairBinReader, PairBinWriter, \
    ArrayByteBuffer
from eggroll.core.transfer.transfer_service import TransferClient, \
    TransferService
from eggroll.core.utils import _exception_logger
from eggroll.roll_pair import create_adapter


class TransferPair(object):
    def __init__(self,
            transfer_id: str,
            output_store: ErStore):
        # params from __init__ params
        self.__transfer_id = transfer_id
        self.__output_store = output_store

        self.__output_adapter = None
        self.__output_tag = None
        self.__total_partitions = self.__output_store._store_locator._total_partitions

        self.__partitioned_brokers = list()
        self.__total_partitioned_elements_count = 0

        self.__push_executor_pool = None
        self.__pull_executor_pool = None
        self.__recv_executor_pool = None

        self.__partition_futures = list()
        self.__rpc_call_futures = list()
        self.__push_futures = list()
        self.__pull_future = None
        self.__recv_future = None

    def __generate_tag(self, partition_id):
        return f'{self.__transfer_id}-{partition_id}'

    @_exception_logger
    def start_push(self, input_broker, partition_function):
        self.__push_executor_pool = ThreadPoolExecutor(max_workers=self.__total_partitions + 1)
        self.__partitioned_brokers = [FifoBroker() for i in range(self.__total_partitions)]
        output_partitions = self.__output_store._partitions

        partition_future = self.__push_executor_pool\
            .submit(TransferPair.partitioner,
                    input_broker,
                    self.__partitioned_brokers,
                    partition_function)
        self.__partition_futures.append(partition_future)

        for i in range(self.__total_partitions):
            tag = self.__generate_tag(i)
            transfer_broker = FifoBroker()
            rpc_call_future = TransferPair.transfer_rpc_call(
                    tag=tag,
                    command_name='send',
                    target_partition=output_partitions[i],
                    output_broker=transfer_broker)
            self.__rpc_call_futures.append(rpc_call_future)

            push_future = self.__push_executor_pool.submit(
                    TransferPair.send,
                    input_broker=self.__partitioned_brokers[i],
                    output_broker=transfer_broker)
            self.__push_futures.append(push_future)

    @_exception_logger
    def start_pull(self, input_adapter):
        def start_pull_partition(partition_id):
            tag = self.__generate_tag(partition_id)

            input_broker = TransferPair.transfer_rpc_call(
                    tag=tag,
                    command_name='recv',
                    target_partition=self.__output_store._partitions[partition_id])

            TransferPair.recv(input_adapter, input_broker)

        def start_sequential_pull():
            for i in range(self.__total_partitions):
                start_pull_partition(i)

        self.__pull_executor_pool = ThreadPoolExecutor(max_workers=1)
        self.__pull_future = self.__pull_executor_pool.submit(start_sequential_pull)

    @_exception_logger
    def start_recv(self, output_partition_id):
        self.__output_tag = self.__generate_tag(output_partition_id)
        output_broker = TransferService.get_or_create_broker(self.__output_tag, write_signals=self.__total_partitions)
        self.__recv_executor_pool = ThreadPoolExecutor(max_workers=1)

        from eggroll.core.pair_store.lmdb import LmdbAdapter
        from eggroll.core.io.io_utils import get_db_path

        self.__output_adapter = create_adapter(self.__output_store._partitions[output_partition_id])
        self.__recv_future = self.__push_executor_pool \
            .submit(TransferPair.recv,
                    output_adapter=self.__output_adapter,
                    output_broker=output_broker)

    def _join_push(self):
        print('joining')
        for broker in self.__partitioned_brokers:
            broker.signal_write_finish()
        try:
            partition_finished, partition_not_finished = wait(self.__partition_futures, return_when=FIRST_EXCEPTION)
            if len(partition_finished) == len(self.__partition_futures):
                print('finishing partition normally')
                self.__is_partition_finished = True
            else:
                print('finishing partition abnormally')
                for e in partition_finished:
                    raise e.exception(timeout=1)

            push_finished, push_not_finished = wait(self.__push_futures, return_when=FIRST_EXCEPTION)
            if len(push_finished) == len(self.__push_futures):
                print('finishing send broker normally')
                self.__is_partition_finished = True
            else:
                print('finishing send broker abnormally')
                for e in partition_finished:
                    raise e.exception(timeout=1)

            for future in self.__rpc_call_futures:
                try:
                    print('finishing send rpc call normally')
                    result = future.result()
                    print('send rpc call actually finished')
                except Exception as e:
                    print('finishing send with exception: ', e)
                    raise e
        finally:
            if self.__push_executor_pool:
                self.__push_executor_pool.shutdown(wait=False)

    def _join_recv(self):
        try:
            if self.__recv_future:
                print('finishing recv normally')
                self.__recv_future.result()
        except Exception as e:
            print('finishing recv with exception', e)
            raise e
        finally:
            if self.__output_adapter:
                self.__output_adapter.close()
            if self.__output_tag:
                TransferService.remove_broker(self.__output_tag)
            if self.__recv_executor_pool:
                self.__recv_executor_pool.shutdown(wait=False)

    def _join_pull(self):
        try:
            if self.__pull_future:
                print('finishing pull normally')
                self.__pull_future.result()
                print('pull finished normally')
        except Exception as e:
            print('finishing pull with exception', e)
        finally:
            if self.__pull_executor_pool:
                self.__pull_executor_pool.shutdown(wait=False)

    def join(self):
        self._join_push()
        self._join_recv()

    def get_total_partitioned_count(self):
        return self.__total_partitioned_elements_count

    @staticmethod
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
            except queue.Empty as e:
                print("partitioner queue empty")

        return partitioned_elements_count

    @staticmethod
    @_exception_logger
    def transfer_rpc_call(tag: str, command_name: str, target_partition, output_broker=None):
        print(f'starting send command from partition {tag} to partition {target_partition._id}')
        client = TransferClient()
        target_endpoint = target_partition._processor._transfer_endpoint
        result = None
        if command_name == 'send':
            result = client.send(broker=output_broker,
                                 endpoint=target_endpoint,
                                 tag=tag)
        elif command_name == 'recv':
            result = client.recv(endpoint=target_endpoint, tag=tag)
        else:
            raise ValueError('operation not implemented')

        return result


    @staticmethod
    @_exception_logger
    def send(input_broker, output_broker, buffer_size=32 << 20):
        print('sender started')

        total_sent = 0
        ba = None
        buffer = None
        writer = None

        def commit(bs=buffer_size):
            nonlocal ba
            nonlocal buffer
            nonlocal writer
            if ba:
                bin_batch = bytes(ba[0:buffer.get_offset()])
                output_broker.put(bin_batch)
            ba = bytearray(bs)
            buffer = ArrayByteBuffer(ba)
            writer = PairBinWriter(pair_buffer=buffer)

        commit()
        pair = None
        while not input_broker.is_closable():
            total_sent += 1
            try:
                pair = input_broker.get(block=True, timeout=1)
                writer.write(pair[0], pair[1])
            except IndexError as e:
                commit(max(buffer_size, len(pair[0] + pair[1])))
                writer.write(pair[0], pair[1])
            except queue.Empty:
                print("transfer send queue empty")

        commit()
        print("finish static send")
        output_broker.signal_write_finish()

        return total_sent

    @staticmethod
    @_exception_logger
    def recv(output_adapter, output_broker):
        print('receiver started')
        total_written = 0

        output_write_batch = output_adapter.new_batch()
        while not output_broker.is_closable():
            try:
                transfer_batch = output_broker.get(block=True, timeout=1)
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

        return total_written
