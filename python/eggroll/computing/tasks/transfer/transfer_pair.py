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

import contextlib
import logging
import queue
import threading

from eggroll.computing.tasks import job_util, store
from eggroll.config import Config
from eggroll.core.datastructure import create_executor_pool
from eggroll.core.datastructure.broker import FifoBroker, BrokerClosed
from eggroll.core.meta_model import ErPartition, ErStore
from eggroll.trace import exception_catch
from .transfer_service import TransferClient, TransferService

L = logging.getLogger(__name__)


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

    def done(self):
        return all(f.done() for f in self._futures)

    def result(self, timeout=None):
        # TODO: sp3: use wait instead of busy loop
        return [f.result(timeout) for f in self._futures]
        # results = [None] * len(self._futures)
        # uncompleted_futures = set(self._futures)
        # start_time = time.time()
        #
        # while uncompleted_futures:
        #     for f in list(uncompleted_futures):
        #         if f.done():
        #             index = self._futures.index(f)
        #             if f.exception():
        #                 raise f.exception()
        #             results[index] = f.result()
        #             uncompleted_futures.remove(f)
        #
        #     # Check for timeout
        #     if timeout is not None and time.time() - start_time > timeout:
        #         raise TimeoutError("Operation timed out after {} seconds".format(timeout))
        #
        #     time.sleep(0.001)
        #
        # return results


class BatchBroker(object):
    def __init__(self, config: Config, broker, batch_size=None):
        self.broker = broker
        self.batch = []
        if batch_size is None:
            batch_size = config.eggroll.rollpair.transferpair.batchbroker.default.size
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.signal_write_finish()


class TransferPair(object):
    _executor_pool = None
    _executor_pool_lock = threading.Lock()

    def __init__(self, config: Config, transfer_id: str):
        # params from __init__ params
        self.__transfer_id = transfer_id
        if TransferPair._executor_pool is None:
            with TransferPair._executor_pool_lock:
                if TransferPair._executor_pool is None:
                    _max_workers = (
                        config.eggroll.rollpair.transferpair.executor.pool.max.size
                    )
                    _thread_pool_type = config.eggroll.core.default.executor.pool
                    TransferPair._executor_pool = create_executor_pool(
                        canonical_name=_thread_pool_type,
                        max_workers=_max_workers,
                        thread_name_prefix="transferpair_pool",
                    )
                    L.debug(f"transfer pair _executor_pool max_workers={_max_workers}")

    def __generate_tag(self, partition_id):
        return job_util.generate_task_id(
            job_id=self.__transfer_id, partition_id=partition_id
        )

    @exception_catch
    def scatter(self, config: Config, input_broker, partitioner, output_store):
        """
        scatter input_broker to output_store

        we use fifo broker to store the input_broker,
        and then scatter the fifo broker to correct partition of output_store with transfer client.
        `do_shuffle_write` saves the input_broker to fifo brokers for each partition.
        `do_shuffle_send` sends the fifo brokers to correct partition of output_store.
        """
        output_partitions = output_store.partitions
        total_partitions = len(output_partitions)
        L.debug(
            f"scatter starts for transfer_id={self.__transfer_id}, total_partitions={total_partitions}, output_store={output_store}"
        )
        shuffle_write_targets = {i: FifoBroker(config) for i in range(total_partitions)}
        futures = []

        @exception_catch
        def do_shuffle_write():
            L.debug(f"do_shuffle_write start for transfer_id={self.__transfer_id}")
            done_count = 0
            with contextlib.ExitStack() as stack:
                shuffle_write_target_batch_brokers = {
                    k: stack.enter_context(BatchBroker(config=config, broker=v))
                    for k, v in shuffle_write_targets.items()
                }
                for k, v in BatchBroker(config=config, broker=input_broker):
                    partition_id = partitioner(k, total_partitions)
                    shuffle_write_target_batch_brokers[partition_id].put((k, v))
                    done_count += 1
                L.debug(
                    f"do_shuffle_write end for transfer id={self.__transfer_id}, "
                    f"total partitions={total_partitions}, "
                    f"cur done partition count={done_count}"
                )
            return done_count

        futures.append(self._executor_pool.submit(do_shuffle_write))
        client = TransferClient()

        def do_shuffle_send():
            do_shuffle_send_futures = []
            for i, partition in enumerate(output_partitions):
                tag = self.__generate_tag(i)
                L.debug(
                    f"do_shuffle_send for tag={tag}, "
                    f"active thread count={threading.active_count()}"
                )
                future = client.send(
                    config=config,
                    broker=TransferPair.pair_to_bin_batch(
                        config=config,
                        input_iter=BatchBroker(
                            config=config, broker=shuffle_write_targets[i]
                        ),
                    ),
                    endpoint=partition.transfer_endpoint,
                    tag=tag,
                )
                do_shuffle_send_futures.append(future)
            return CompositeFuture(do_shuffle_send_futures).result()

        futures.append(self._executor_pool.submit(do_shuffle_send))
        return CompositeFuture(futures)

    @staticmethod
    @exception_catch
    def pair_to_bin_batch(config: Config, input_iter, limit=None, sendbuf_size=-1):
        if sendbuf_size <= 0:
            sendbuf_size = config.eggroll.rollpair.transferpair.sendbuf.size

        L.debug(f"pair_to_bin_batch start")
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
                bin_batch = bytes(ba[0 : writer.get_offset()])
            # if ba:
            #     bin_batch = bytes(ba[0:buffer.get_offset()])
            ba = bytearray(bs)
            buffer = store.ArrayByteBuffer(ba)
            writer = store.PairBinWriter(pair_buffer=buffer, data=ba)
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
            L.debug(f"pair_to_bin_batch final pair count={pair_count}")
            yield commit()
        except Exception as e:
            L.exception(f"bin_batch_generator error:{e}")
        L.debug(f"generate_bin_batch end. pair count={pair_count}")

    @staticmethod
    def bin_batch_to_pair(input_iter):
        L.debug(f"bin_batch_to_pair start")
        write_count = 0
        for batch in input_iter:
            L.debug(f"bin_batch_to_pair: cur batch size={len(batch)}")
            try:
                bin_data = store.ArrayByteBuffer(batch)
                reader = store.PairBinReader(pair_buffer=bin_data, data=batch)
                for k_bytes, v_bytes in reader.read_all():
                    yield k_bytes, v_bytes
                    write_count += 1
            except IndexError as e:
                L.exception(f"error bin bath format: {e}")
            L.debug(f"bin_batch_to_pair batch ends. total write count={write_count}")
        L.debug(f"bin_batch_to_pair total_written count={write_count}")

    def store_broker(
        self,
        config: Config,
        data_dir,
        store_partition,
        is_shuffle,
        total_writers=1,
        reduce_op=None,
    ):
        """
        is_shuffle=True: all partition in one broker
        is_shuffle=False: just save broker to store, for put_all
        """

        # def do_merge(old_value, update_value, merge_func):
        #     return merge_func(old_value, update_value)

        @exception_catch
        def do_store(
            _config: Config,
            store_partition_inner: ErPartition,
            is_shuffle_inner,
            total_writers_inner,
            reduce_op_inner,
        ):
            done_cnt = 0
            tag = (
                self.__generate_tag(store_partition_inner._id)
                if is_shuffle_inner
                else self.__transfer_id
            )
            try:
                broker = TransferService.get_or_create_broker(
                    config=_config, key=tag, write_signals=total_writers_inner
                )
                L.debug(f"do_store start for tag={tag}")
                batches = TransferPair.bin_batch_to_pair(b.data for b in broker)

                with store.get_adapter(store_partition_inner, data_dir) as db:
                    L.debug(
                        f"do_store create_db for tag={tag} for partition={store_partition_inner}"
                    )
                    with db.new_batch() as wb:
                        if reduce_op_inner is None:
                            for k, v in batches:
                                wb.put(k, v)
                                done_cnt += 1
                        else:
                            # merger = functools.partial(do_merge, merge_func=reduce_op_inner)
                            for k, v in batches:
                                # TODO: why error raise here block the whole process?
                                wb.merge(reduce_op_inner, k, v)
                                done_cnt += 1
                        L.debug(
                            f"do_store done for tag={tag} for partition={store_partition_inner}"
                        )
            except Exception as e:
                L.exception(f"Error in do_store for tag={tag}")
                raise e
            finally:
                TransferService.remove_broker(tag)
            return done_cnt

        return self._executor_pool.submit(
            do_store, config, store_partition, is_shuffle, total_writers, reduce_op
        )

    def gather(self, config: Config, store: ErStore):
        L.debug(f"gather start for transfer_id={self.__transfer_id}, store={store}")
        client = TransferClient()
        for i in range(store.num_partitions):
            partition = store.get_partition(i)
            tag = self.__generate_tag(partition.id)
            L.debug(f"gather for tag={tag}, partition={partition}")
            target_endpoint = partition.processor.transfer_endpoint
            batches = (
                b.data
                for b in client.recv(
                    config=config, endpoint=target_endpoint, tag=tag, broker=None
                )
            )
            yield from TransferPair.bin_batch_to_pair(batches)
