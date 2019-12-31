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
import traceback
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION, ALL_COMPLETED
from threading import Thread

from eggroll.core.datastructure.broker import FifoBroker, BrokerClosed
from eggroll.core.meta_model import ErStore
from eggroll.core.pair_store.format import PairBinReader, PairBinWriter, \
    ArrayByteBuffer
from eggroll.core.transfer.transfer_service import TransferClient, \
    TransferService
from eggroll.core.utils import _exception_logger
from eggroll.core.utils import generate_task_id
from eggroll.roll_pair import create_adapter
from eggroll.utils.log_utils import get_logger
LG = get_logger()

class CompositeFuture(object):
    def __init__(self, futures):
        self._futures = futures
    def get_futures(self):
        return self._futures

    def cancel(self):
        ret = True
        for f in self._futures:
            if not f.cancel():
                ret = False
        return ret

    def result(self):
        return list(f.result() for f in self._futures)

class BatchBroker(object):
    def __init__(self, broker, batch_size=100):
        self.broker = broker
        self.batch = []
        self.batch_size = batch_size

    def _commit(self, block=True, timeout=None):
        if len(self.batch) == 0:
            return
        self.broker.put(self.batch, block, timeout)
        self.batch = []

    def put(self, item, block=True, timeout=0):
        if len(self.batch) >= self.batch_size:
            self._commit(block, timeout)
        self.batch.append(item)

    def signal_write_finish(self):
        self._commit()
        self.broker.signal_write_finish()

    def is_closable(self):
        return len(self.batch) == 0 and self.broker.is_closable()

    def get(self, block=True, timeout=None):
        if len(self.batch) == 0:
            self.batch = self.broker.get(block, timeout)
        if len(self.batch) == 0:
            raise queue.Empty("empty")
        return self.batch.pop(0)

    def __iter__(self):
        return self

    def __next__(self):
        while not self.is_closable():
            try:
                return self.get(block=True, timeout=0.1)
            except queue.Empty as e:
                # retry
                pass
            except BrokerClosed as e:
                raise StopIteration
        raise StopIteration

class IteratorBroker(object):
    def __init__(self, it):
        self._it = it
        self._end = False
        self._cur = None
        self._first_time = True

    def _fetch(self):
        try:
            self._cur = next(self._it)
        except StopIteration:
            self._end = True
            self._cur = None
        return self._cur

    def _init(self):
        if self._first_time:
            self._fetch()
            self._first_time = False
    def get(self, block=True, timeout=None):
        self._init()
        ret = self._cur
        # fetch next
        self._fetch()
        return ret

    def is_closable(self):
        self._init()
        return self._end

class TransferPair(object):
    def __init__(self, transfer_id: str):
        # params from __init__ params
        self.__transfer_id = transfer_id

        self.__partitioned_brokers = list()
        self.__total_partitioned_elements_count = 0

        self.__partition_executor_pool = None
        self.__pull_executor_pool = None
        self.__recv_executor_pool = None

        self.__partition_futures = list()
        self.__scatter_futures = list()
        self.__pull_future = None
        self.__recv_future = None
        self._executor_pool = ThreadPoolExecutor(max_workers=100)

    def __generate_tag(self, partition_id):
        return generate_task_id(job_id=self.__transfer_id, partition_id=partition_id)

    @_exception_logger
    def start_scatter(self, input_broker, partition_function, output_store):
        output_partitions = output_store._partitions
        total_partitions = len(output_partitions)
        partitioned_brokers = [FifoBroker() for i in range(total_partitions)]
        partitioned_bb = [BatchBroker(v) for v in partitioned_brokers]
        futures = []
        @_exception_logger
        def do_partition():
            LG.debug('do_partition start')
            done_count = 0
            for k, v in BatchBroker(input_broker):
                partitioned_bb[partition_function(k)].put((k,v))
                done_count +=1
            LG.debug(f"do_partition end:{done_count}")
            for broker in partitioned_bb:
                broker.signal_write_finish()
        futures.append(self._executor_pool.submit(do_partition))
        client = TransferClient()
        for i, part in enumerate(output_partitions):
            tag = self.__generate_tag(i)
            LG.debug(f"start_scatter_partition_tag:{tag}")
            # TODO:1: change client.send to support iterator
            futures.append(
                client.send(TransferPair.bin_batch_generator(BatchBroker(partitioned_brokers[i])),
                        part._processor._transfer_endpoint, tag))
        return CompositeFuture(futures)

    @staticmethod
    @_exception_logger
    def bin_batch_generator(input_broker, buffer_size=32 * 1024 * 1024):
        import os
        buffer_size = os.environ.get("EGGROLL_ROLLPAIR_BIN_BATCH_SIZE", buffer_size)
        # TODO:1: buffer_size auto adjust?
        LG.debug('generate_bin_batch start')
        done_cont = 0
        ba = None
        buffer = None
        writer = None

        def commit(bs=buffer_size):
            print('generate_bin_batch commit', done_cont)
            nonlocal ba
            nonlocal buffer
            nonlocal writer
            if ba:
                bin_batch = bytes(ba[0:buffer.get_offset()])
                return bin_batch
            ba = bytearray(bs)
            buffer = ArrayByteBuffer(ba)
            writer = PairBinWriter(pair_buffer=buffer)
        # init var
        commit()
        try:
            for k, v in  input_broker:
                try:
                    writer.write(k, v)
                    done_cont += 1
                except IndexError as e:
                    yield commit(max(buffer_size, len(k) + len(v) + 1024))
                    writer.write(k, v)
            LG.debug(f'generate_bin_batch last one: {done_cont}')
            yield commit()
        except Exception as e:
            # TODO:1: LOG
            print("bin_batch_generator error", e)
            traceback.print_stack()
        LG.debug(f'generate_bin_batch end: {done_cont}')

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
    def start_recv(self, output_partition_id, output_store):
        self.__output_tag = self.__generate_tag(output_partition_id)
        output_broker = TransferService.get_or_create_broker(self.__output_tag, write_signals=len(output_store._partitions))
        self.__recv_executor_pool = ThreadPoolExecutor(max_workers=1)

        self.__output_adapter = create_adapter(output_store._partitions[output_partition_id])
        self.__recv_future = self._executor_pool \
            .submit(TransferPair.recv,
                    output_adapter=self.__output_adapter,
                    output_broker=output_broker)


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
                pair = input_broker.get(block=True, timeout=0.1)
                writer.write(pair[0], pair[1])
            except IndexError as e:
                commit(max(buffer_size, len(pair[0] + pair[1])))
                writer.write(pair[0], pair[1])
            except queue.Empty:
                #print("transfer send queue empty")
                pass
            except BrokerClosed:
                break

        commit()
        print("finish static send:", total_sent)
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
                transfer_batch = output_broker.get(block=True, timeout=0.1)
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
            except BrokerClosed as e:
                break

        output_write_batch.write()
        output_write_batch.close()

        return total_written
