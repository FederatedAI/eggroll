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
from concurrent.futures import ThreadPoolExecutor

from eggroll.core.datastructure.broker import FifoBroker, BrokerClosed
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


class TransferPair(object):
    def __init__(self, transfer_id: str):
        # params from __init__ params
        self.__transfer_id = transfer_id
        self._executor_pool = ThreadPoolExecutor(max_workers=100)

    def __generate_tag(self, partition_id):
        return generate_task_id(job_id=self.__transfer_id, partition_id=partition_id)

    @_exception_logger
    def scatter(self, input_broker, partition_function, output_store):
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
            return done_count
        futures.append(self._executor_pool.submit(do_partition))
        client = TransferClient()
        for i, part in enumerate(output_partitions):
            tag = self.__generate_tag(i)
            LG.debug(f"start_scatter_partition_tag:{tag}")
            # TODO:1: change client.send to support iterator
            futures.append(
                client.send(TransferPair.pair_to_bin_batch(BatchBroker(partitioned_brokers[i])),
                            part._processor._transfer_endpoint, tag))
        return CompositeFuture(futures)

    @staticmethod
    def pair_to_bin_batch(input_iter, buffer_size=32 * 1024 * 1024):
        import os
        buffer_size = int(os.environ.get("EGGROLL_ROLLPAIR_BIN_BATCH_SIZE", buffer_size))
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
            bin_batch = None
            if ba:
                bin_batch = bytes(ba[0:buffer.get_offset()])
            ba = bytearray(bs)
            buffer = ArrayByteBuffer(ba)
            writer = PairBinWriter(pair_buffer=buffer)
            return bin_batch
        # init var
        commit()
        try:
            for k, v in  input_iter:
                try:
                    writer.write(k, v)
                    done_cont += 1
                except IndexError as e:
                    yield commit(max(buffer_size, len(k) + len(v) + 1024))
                    writer.write(k, v)
            LG.debug(f'generate_bin_batch last one: {done_cont}')
            yield commit()
        except Exception as e:
            LG.exception(f"bin_batch_generator error:{e}")
        LG.debug(f'generate_bin_batch end: {done_cont}')

    @staticmethod
    def bin_batch_to_pair(input_iter):
        LG.debug(f"bin_batch_to_pair start")
        total_written = 0
        for batch in input_iter:
            try:
                bin_data = ArrayByteBuffer(batch)
                reader = PairBinReader(pair_buffer=bin_data)
                for k_bytes, v_bytes in reader.read_all():
                    yield k_bytes, v_bytes
                    total_written += 1
            except IndexError as e:
                LG.exception(f"error bin bath format:{e}")
        LG.debug(f"bin_batch_to_pair total_written:{total_written}")

    def store_broker(self, store_partition, is_shuffle, total_partitions=1):
        """
        is_shuffle=True: all partition in one broker
        is_shuffle=False: just save broker to store, for put_all
        """
        @_exception_logger
        def do_store():
            tag = self.__generate_tag(store_partition._id) if is_shuffle else self.__transfer_id
            broker = TransferService.get_or_create_broker(tag, write_signals=total_partitions)
            LG.debug(f"do_store_start:{tag}")
            done_cnt = 0
            batches = TransferPair.bin_batch_to_pair(b.data for b in broker)
            with create_adapter(store_partition) as db:
                LG.debug(f"do_store_create_db:{tag}")
                with db.new_batch() as wb:
                    for k, v in batches:
                        wb.put(k, v)
                        done_cnt+=1
            TransferService.remove_broker(tag)
            return done_cnt
        return self._executor_pool.submit(do_store)

    def gather(self, store):
        client = TransferClient()
        for partition in store._partitions:
            tag = self.__generate_tag(partition._id)
            target_endpoint = partition._processor._transfer_endpoint
            batchs = (b.data for b in client.recv(endpoint=target_endpoint, tag=tag, broker=None))
            yield from TransferPair.bin_batch_to_pair(batchs)
