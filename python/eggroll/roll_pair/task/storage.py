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

import logging
import queue
import threading
from collections import defaultdict, namedtuple

from eggroll.core.conf_keys import CoreConfKeys
from eggroll.core.datastructure.broker import BrokerClosed
from eggroll.core.pair_store.format import ArrayByteBuffer, PairBinReader
from eggroll.core.transfer.transfer_service import TransferService
from eggroll.core.transfer_model import ErRollSiteHeader
from eggroll.roll_pair import create_adapter
from eggroll.utils.log_utils import get_logger

L = get_logger()
FINISH_STATUS = "finish_partition"

BSS = namedtuple('BSS', ['tag', 'is_finished', 'total_batches', 'batch_seq_to_pair_counter', 'total_streams', 'stream_seq_to_pair_counter', 'stream_seq_to_batch_seq', 'total_pairs', 'data_type'])


class _BatchStreamStatus:
    _recorder = {}

    _recorder_lock = threading.Lock()

    # Initialisation of this class MUST BE wrapped in lock
    def __init__(self, tag):
        self._tag = tag
        self._stage = "doing"
        self._total_batches = -1
        self._total_streams = -1
        self._data_type = None
        self._batch_seq_to_pair_counter = defaultdict(int)
        self._stream_seq_to_pair_counter = defaultdict(int)
        self._stream_seq_to_batch_seq = defaultdict(int)
        self._stream_finish_event = threading.Event()
        self._header_arrive_event = threading.Event()
        self._rs_header = None
        self._rs_key = None
        # removes lock. otherwise it deadlocks
        self._recorder[self._tag] = self
        self._is_in_order = True
        #self._last_updated_at = None

    def _debug_string(self):
        return f"BatchStreams end normally, tag={self._tag} " \
               f"total_batches={self._total_batches}:total_elems={sum(self._batch_seq_to_pair_counter.values())}"

    def set_done(self, rs_header):
        L.trace(f'set done. rs_key={rs_header.get_rs_key()}, rs_header={rs_header}')
        self._total_batches = rs_header._total_batches
        self._total_streams = rs_header._total_streams
        self._stage = "done"
        if self._total_batches != len(self._batch_seq_to_pair_counter):
            self._is_in_order = False
            L.debug(f"MarkEnd BatchStream ahead of all BatchStreams received, {self._debug_string()}, rs_key={rs_header.get_rs_key()}, rs_header={rs_header}")

    def count_batch(self, rs_header: ErRollSiteHeader, batch_pairs):
        L.trace(f'count batch. rs_key={rs_header.get_rs_key()}, rs_header={rs_header}, batch_pairs={batch_pairs}')
        batch_seq_id = rs_header._batch_seq
        stream_seq_id = rs_header._stream_seq
        if self._rs_header is None:
            self._rs_header = rs_header
            self._rs_key = rs_header.get_rs_key()
            L.debug(f"header arrived. rs_key={rs_header.get_rs_key()}, rs_header={rs_header}")
            self._header_arrive_event.set()
        self._batch_seq_to_pair_counter[batch_seq_id] = batch_pairs
        self._stream_seq_to_pair_counter[stream_seq_id] += batch_pairs
        self._stream_seq_to_batch_seq[stream_seq_id] = batch_seq_id

    def check_finish(self):
        if L.isEnabledFor(logging.TRACE):
            L.trace(f'checking finish. rs_key={self._rs_key}, rs_header={self._rs_header}, stage={self._stage}, total_batches={self._total_batches}, len={len(self._batch_seq_to_pair_counter)}')
        if self._stage == "done" and self._total_batches == len(self._batch_seq_to_pair_counter):
            L.debug(f"All BatchStreams finished, {self._debug_string()}. is_in_order={self._is_in_order}, rs_key={self._rs_key}, rs_header={self._rs_header}")
            self._stream_finish_event.set()
            return True
        else:
            return False

    @classmethod
    def get_or_create(cls, tag):
        with cls._recorder_lock:
            if tag not in cls._recorder:
                bss = _BatchStreamStatus(tag)
            else:
                bss = cls._recorder[tag]
        return bss

    @classmethod
    def wait_finish(cls, tag, timeout):
        bss = cls.get_or_create(tag)
        finished = bss._stream_finish_event.wait(timeout)
        if finished:
            TransferService.remove_broker(tag)
            #del cls._recorder[tag]

        return BSS(
                tag=bss._tag,
                is_finished=finished,
                total_batches=bss._total_batches,
                batch_seq_to_pair_counter=bss._batch_seq_to_pair_counter,
                total_streams=bss._total_streams,
                stream_seq_to_pair_counter=bss._stream_seq_to_pair_counter,
                stream_seq_to_batch_seq=bss._stream_seq_to_batch_seq,
                total_pairs=sum(bss._batch_seq_to_pair_counter.values()),
                data_type=bss._data_type)
        #return finished, bss._total_batches, bss._counter, sum(bss._counter.values()), bss._data_type

    @classmethod
    def clear_status(cls, tag):
        with cls._recorder_lock:
            if tag in cls._recorder:
                bss = cls._recorder[tag]
                del cls._recorder[tag]
                return bss._to_tuple()

    @classmethod
    def wait_header(cls, tag, timeout):
        bss = cls.get_or_create(tag)
        bss._header_arrive_event.wait(timeout)
        return bss._rs_header

    def __repr__(self):
        return f'<_BatchStreamStatus(tag={self._tag}, ' \
               f'stage={self._stage}, ' \
               f'total_batches={self._total_batches}, ' \
               f'data_type={self._data_type}, ' \
               f'batch_seq_to_pair_counter={self._batch_seq_to_pair_counter}, ' \
               f'stream_seq_to_pair_counter={self._stream_seq_to_pair_counter}, ' \
               f'stream_seq_to_batch_seq={self._stream_seq_to_batch_seq}, ' \
               f'stream_finish_event={self._stream_finish_event.is_set()}, ' \
               f'header_arrive_event={self._header_arrive_event.is_set()}, ' \
               f'rs_header={self._rs_header}) at {hex(id(self))}>'

    def _to_tuple(self):
        return BSS(
                tag=self._tag,
                is_finished=self._stream_finish_event.is_set(),
                total_batches=self._total_batches,
                batch_seq_to_pair_counter=self._batch_seq_to_pair_counter,
                total_streams=self._total_streams,
                stream_seq_to_pair_counter=self._stream_seq_to_pair_counter,
                stream_seq_to_batch_seq=self._stream_seq_to_batch_seq,
                total_pairs=sum(self._batch_seq_to_pair_counter.values()),
                data_type=self._data_type)

class PutBatchTask:

    """
    transfer a total roll_pair by several batch streams
    """
    # tag -> seq -> count

    _class_lock = threading.Lock()
    _partition_lock = defaultdict(threading.Lock)

    def __init__(self, tag, partition=None):
        self.partition = partition
        self.tag = tag

    def run(self):
        # batch stream must be executed serially, and reinit.
        # TODO:0:  remove lock to bss
        rs_header = None
        with PutBatchTask._partition_lock[self.tag]:   # tag includes partition info in tag generation
            L.trace(f"do_store start for tag={self.tag}, partition_id={self.partition._id}")
            bss = _BatchStreamStatus.get_or_create(self.tag)
            try:
                broker = TransferService.get_or_create_broker(self.tag, write_signals=1)

                iter_wait = 0
                iter_timeout = int(CoreConfKeys.EGGROLL_CORE_FIFOBROKER_ITER_TIMEOUT_SEC.get())

                batch = None
                batch_get_interval = 0.1
                with create_adapter(self.partition) as db, db.new_batch() as wb:
                    #for batch in broker:
                    while not broker.is_closable():
                        try:
                            batch = broker.get(block=True, timeout=batch_get_interval)
                        except queue.Empty as e:
                            iter_wait += batch_get_interval
                            if iter_wait > iter_timeout:
                                raise TimeoutError(f'timeout in PutBatchTask.run. tag={self.tag}, iter_timeout={iter_timeout}, iter_wait={iter_wait}')
                            else:
                                continue
                        except BrokerClosed as e:
                            continue

                        iter_wait = 0
                        rs_header = ErRollSiteHeader.from_proto_string(batch.header.ext)
                        batch_pairs = 0
                        if batch.data:
                            bin_data = ArrayByteBuffer(batch.data)
                            reader = PairBinReader(pair_buffer=bin_data, data=batch.data)
                            for k_bytes, v_bytes in reader.read_all():
                                wb.put(k_bytes, v_bytes)
                                batch_pairs += 1
                        bss.count_batch(rs_header, batch_pairs)
                        # TODO:0
                        bss._data_type = rs_header._data_type
                        if rs_header._stage == FINISH_STATUS:
                            bss.set_done(rs_header)  # starting from 0

                bss.check_finish()
                # TransferService.remove_broker(tag) will be called in get_status phrase finished or exception got
            except Exception as e:
                L.exception(f'_run_put_batch error, tag={self.tag}, '
                        f'rs_key={rs_header.get_rs_key() if rs_header is not None else None}, rs_header={rs_header}')
                raise e
            finally:
                TransferService.remove_broker(self.tag)

    def get_status(self, timeout):
        return _BatchStreamStatus.wait_finish(self.tag, timeout)

    def get_header(self, timeout):
        return _BatchStreamStatus.wait_header(self.tag, timeout)

    def clear_status(self):
        return _BatchStreamStatus.clear_status(self.tag)