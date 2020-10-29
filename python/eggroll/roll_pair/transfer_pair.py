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
import queue
import threading

from eggroll.core.conf_keys import CoreConfKeys, RollPairConfKeys
from eggroll.core.datastructure import create_executor_pool
from eggroll.core.datastructure.broker import FifoBroker, BrokerClosed
from eggroll.core.pair_store.format import PairBinReader, PairBinWriter, ArrayByteBuffer
from eggroll.core.transfer.transfer_service import TransferClient, \
    TransferService
from eggroll.core.utils import _exception_logger
from eggroll.core.utils import generate_task_id
from eggroll.roll_pair import create_adapter, create_serdes
from eggroll.utils.log_utils import get_logger

L = get_logger()


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
    def __init__(self, broker, batch_size=RollPairConfKeys.EGGROLL_ROLLPAIR_TRANSFERPAIR_BATCHBROKER_DEFAULT_SIZE.default_value):
        self.broker = broker
        self.batch = []
        self.batch_size = batch_size

    def _commit(self, block=True, timeout=None):
        if len(self.batch) == 0:
            return
        self.broker.put(self.batch, block, timeout)
        self.batch = []

    def put(self, item, block=True, timeout=None):
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
    _executor_pool = None
    _executor_pool_lock = threading.Lock()

    def __init__(self, transfer_id: str):
        # params from __init__ params
        self.__transfer_id = transfer_id
        if TransferPair._executor_pool is None:
            with TransferPair._executor_pool_lock:
                if TransferPair._executor_pool is None:
                    _max_workers = int(RollPairConfKeys.EGGROLL_ROLLPAIR_TRANSFERPAIR_EXECUTOR_POOL_MAX_SIZE.get())
                    _thread_pool_type = CoreConfKeys.EGGROLL_CORE_DEFAULT_EXECUTOR_POOL.get()
                    TransferPair._executor_pool = create_executor_pool(
                            canonical_name=_thread_pool_type,
                            max_workers=_max_workers,
                            thread_name_prefix="transferpair_pool")
                    L.info(f'transfer pair _executor_pool max_workers={_max_workers}')

    def __generate_tag(self, partition_id):
        return generate_task_id(job_id=self.__transfer_id, partition_id=partition_id)

    @_exception_logger
    def scatter(self, input_broker, partition_function, output_store):
        output_partitions = output_store._partitions
        total_partitions = len(output_partitions)
        L.trace(f'scatter starts for transfer_id={self.__transfer_id}, total_partitions={total_partitions}, output_store={output_store}')
        partitioned_brokers = [FifoBroker() for i in range(total_partitions)]
        partitioned_bb = [BatchBroker(v) for v in partitioned_brokers]
        futures = []

        @_exception_logger
        def do_partition():
            L.trace(f'do_partition start for transfer_id={self.__transfer_id}')
            done_count = 0
            for k, v in BatchBroker(input_broker):
                partitioned_bb[partition_function(k)].put((k, v))
                done_count += 1
            for pb in partitioned_bb:
                pb.signal_write_finish()
            L.trace(f"do_partition end for transfer id={self.__transfer_id}, "
                    f"total partitions={total_partitions}, "
                    f"cur done partition count={done_count}")
            return done_count
        futures.append(self._executor_pool.submit(do_partition))
        client = TransferClient()

        def do_send_all():
            send_all_futs = []
            for i, part in enumerate(output_partitions):
                tag = self.__generate_tag(i)
                L.trace(f"do_send_all for tag={tag}, "
                        f"active thread count={threading.active_count()}")
                fut = client.send(
                        TransferPair.pair_to_bin_batch(
                            BatchBroker(partitioned_brokers[i])),
                                part._processor._transfer_endpoint, tag)
                send_all_futs.append(fut)
            return CompositeFuture(send_all_futs).result()

        futures.append(self._executor_pool.submit(do_send_all))
        return CompositeFuture(futures)

    @staticmethod
    @_exception_logger
    def pair_to_bin_batch(input_iter, limit=None, sendbuf_size=-1):
        import os
        if sendbuf_size <= 0:
            sendbuf_size = int(RollPairConfKeys.EGGROLL_ROLLPAIR_TRANSFERPAIR_SENDBUF_SIZE.get())

        L.trace(f'pair_to_bin_batch start')
        pair_count = 0
        ba = None
        buffer = None
        writer = None

        def commit(bs=sendbuf_size):
            nonlocal ba
            nonlocal buffer
            nonlocal writer
            bin_batch = None
            if ba:
                bin_batch = bytes(ba[0:writer.get_offset()])
            # if ba:
            #     bin_batch = bytes(ba[0:buffer.get_offset()])
            ba = bytearray(bs)
            buffer = ArrayByteBuffer(ba)
            writer = PairBinWriter(pair_buffer=buffer, data=ba)
            return bin_batch
        # init var
        commit()
        try:
            for k, v in input_iter:
                try:
                    writer.write(k, v)
                    pair_count += 1
                    if limit is not None and pair_count >= limit:
                        break
                except IndexError as e:
                    # TODO:0: replace 1024 with constant
                    yield commit(max(sendbuf_size, len(k) + len(v) + 1024))
                    writer.write(k, v)
            L.trace(f'pair_to_bin_batch final pair count={pair_count}')
            yield commit()
        except Exception as e:
            L.exception(f"bin_batch_generator error:{e}")
        L.trace(f'generate_bin_batch end. pair count={pair_count}')

    @staticmethod
    def bin_batch_to_pair(input_iter):
        L.trace(f"bin_batch_to_pair start")
        write_count = 0
        for batch in input_iter:
            L.trace(f"bin_batch_to_pair: cur batch size={len(batch)}")
            try:
                bin_data = ArrayByteBuffer(batch)
                reader = PairBinReader(pair_buffer=bin_data, data=batch)
                for k_bytes, v_bytes in reader.read_all():
                    yield k_bytes, v_bytes
                    write_count += 1
            except IndexError as e:
                L.exception(f"error bin bath format: {e}")
            L.trace(f"bin_batch_to_pair batch ends. total write count={write_count}")
        L.trace(f"bin_batch_to_pair total_written count={write_count}")

    def store_broker(self, store_partition, is_shuffle, total_writers=1, reduce_op=None):
        """
        is_shuffle=True: all partition in one broker
        is_shuffle=False: just save broker to store, for put_all
        """
        def do_merge(old_value, update_value, merge_func, serdes):
            new_value = merge_func(serdes.deserialize(old_value), serdes.deserialize(update_value))
            return serdes.serialize(new_value)

        @_exception_logger
        def do_store(store_partition_inner, is_shuffle_inner, total_writers_inner, reduce_op_inner):
            done_cnt = 0
            tag = self.__generate_tag(store_partition_inner._id) if is_shuffle_inner else self.__transfer_id
            try:
                broker = TransferService.get_or_create_broker(tag, write_signals=total_writers_inner)
                L.trace(f"do_store start for tag={tag}")
                batches = TransferPair.bin_batch_to_pair(b.data for b in broker)

                serdes = create_serdes(store_partition_inner._store_locator._serdes)
                if reduce_op_inner is not None:
                    merger = functools.partial(do_merge,
                                               merge_func=reduce_op_inner,
                                               serdes=serdes)
                else:
                    merger = None

                with create_adapter(store_partition_inner) as db:
                    L.trace(f"do_store create_db for tag={tag} for partition={store_partition_inner}")
                    with db.new_batch() as wb:
                        for k, v in batches:
                            if reduce_op_inner is None:
                                wb.put(k, v)
                            else:
                                wb.merge(merger, k, v)
                            done_cnt += 1
                    L.trace(f"do_store done for tag={tag} for partition={store_partition_inner}")
            except Exception as e:
                L.exception(f'Error in do_store for tag={tag}')
                raise e
            finally:
                TransferService.remove_broker(tag)
            return done_cnt
        return self._executor_pool.submit(do_store, store_partition, is_shuffle, total_writers, reduce_op)

    def gather(self, store):
        L.trace(f'gather start for transfer_id={self.__transfer_id}, store={store}')
        client = TransferClient()
        for partition in store._partitions:
            tag = self.__generate_tag(partition._id)
            L.trace(f'gather for tag={tag}, partition={partition}')
            target_endpoint = partition._processor._transfer_endpoint
            batches = (b.data for b in client.recv(endpoint=target_endpoint, tag=tag, broker=None))
            yield from TransferPair.bin_batch_to_pair(batches)
