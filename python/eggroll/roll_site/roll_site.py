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
#
#
import itertools
import logging
import os
import functools
import threading
import time
import pickle

from eggroll.core.conf_keys import SessionConfKeys, RollSiteConfKeys, \
    CoreConfKeys
from eggroll.core.constants import DeployModes
from eggroll.core.constants import StoreTypes
from eggroll.core.datastructure import create_executor_pool
from eggroll.core.error import GrpcCallError
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErStoreLocator, ErStore, ErEndpoint
from eggroll.core.pair_store.format import ArrayByteBuffer, PairBinWriter
from eggroll.core.proto import proxy_pb2, proxy_pb2_grpc
from eggroll.core.proto.transfer_pb2 import TransferBatch, TransferHeader
from eggroll.core.serdes import eggroll_serdes
from eggroll.core.transfer_model import ErRollSiteHeader
from eggroll.core.utils import _stringify, static_er_conf
from eggroll.core.utils import to_one_line_string
from eggroll.roll_pair import create_adapter
from eggroll.roll_pair.roll_pair import RollPair, RollPairContext
from eggroll.roll_pair.task.storage import PutBatchTask
from eggroll.roll_pair.transfer_pair import TransferPair
from eggroll.roll_site.utils.roll_site_utils import create_store_name
from eggroll.utils import log_utils

L = log_utils.get_logger()
P = log_utils.get_logger('profile')
_serdes = eggroll_serdes.PickleSerdes
RS_KEY_DELIM = "#"
STATUS_TABLE_NAME = "__rs_status"

ERROR_STATES = [proxy_pb2.STOP, proxy_pb2.KILL]
RS_KEY_PREFIX = "__rsk"
CONF_KEY_TARGET = "rollsite"
CONF_KEY_LOCAL = "local"
CONF_KEY_SERVER = "servers"


class CountDownLatch(object):
    def __init__(self, count):
        self.count = count
        self.lock = threading.Condition()

    def count_up(self):
        with self.lock:
            self.count += 1

    def count_down(self):
        with self.lock:
            self.count -= 1
            if self.count <= 0:
                self.lock.notifyAll()

    def await_latch(self, timeout=None, attempt=1, after_attempt=None):
        try_count = 0
        with self.lock:
            while self.count > 0 and try_count < attempt:
                try_count += 1
                self.lock.wait(timeout)
                if after_attempt:
                    after_attempt(try_count)
        return self.count


class RollSiteContext:
    grpc_channel_factory = GrpcChannelFactory()

    def __init__(self, roll_site_session_id, rp_ctx: RollPairContext, options: dict = None):
        if options is None:
            options = {}
        self.roll_site_session_id = roll_site_session_id
        self.rp_ctx = rp_ctx

        self.role = options["self_role"]
        self.party_id = str(options["self_party_id"])
        endpoint = options["proxy_endpoint"]
        if isinstance(endpoint, str):
            splitted = endpoint.split(':')
            self.proxy_endpoint = ErEndpoint(host=splitted[0].strip(), port=int(splitted[1].strip()))
        elif isinstance(endpoint, ErEndpoint):
            self.proxy_endpoint = endpoint
        else:
            raise ValueError("endpoint only support str and ErEndpoint type")

        self.is_standalone = RollSiteConfKeys.EGGROLL_ROLLSITE_DEPLOY_MODE.get_with(options) == "standalone"
        if self.is_standalone:
            self.stub = None
        else:
            channel = self.grpc_channel_factory.create_channel(self.proxy_endpoint)
            self.stub = proxy_pb2_grpc.DataTransferServiceStub(channel)

        self.pushing_latch = CountDownLatch(0)
        self.rp_ctx.get_session().add_exit_task(self._wait_push_complete)
        self._wait_push_exit_timeout = options["wait_push_exit_timeout"] if "wait_push_exit_timeout" in options else 10 * 60
        L.info(f"inited RollSiteContext: {self.__dict__}")

    def _wait_push_complete(self):
        session_id = self.rp_ctx.get_session().get_session_id()
        L.info(f"running roll site exit func for er session={session_id},"
               f" roll site session id={self.roll_site_session_id}")
        residual_count = self.pushing_latch.await_latch(self._wait_push_exit_timeout)
        if residual_count != 0:
            L.error(f"exit session when not finish push: "
                    f"residual_count={residual_count}, timeout={self._wait_push_exit_timeout}")

    def load(self, name: str, tag: str, options: dict = None):
        if options is None:
            options = {}
        return RollSite(name, tag, self, options=options)


class RollSiteBase:
    _receive_executor_pool = None

    def __init__(self, name: str, tag: str, rs_ctx: RollSiteContext, options: dict = None):
        if options is None:
            options = {}
        self.ctx = rs_ctx
        self.party_id = self.ctx.party_id
        self.dst_host = self.ctx.proxy_endpoint._host
        self.dst_port = self.ctx.proxy_endpoint._port
        self.roll_site_session_id = self.ctx.roll_site_session_id
        self.local_role = self.ctx.role
        self.name = name
        self.tag = tag
        self.stub = self.ctx.stub
        if RollSiteBase._receive_executor_pool is None:
            receive_executor_pool_size = int(RollSiteConfKeys.EGGROLL_ROLLSITE_RECEIVE_EXECUTOR_POOL_MAX_SIZE.get_with(options))
            receive_executor_pool_type = CoreConfKeys.EGGROLL_CORE_DEFAULT_EXECUTOR_POOL.get_with(options)
            self._receive_executor_pool = create_executor_pool(
                canonical_name=receive_executor_pool_type,
                max_workers=receive_executor_pool_size,
                thread_name_prefix="rollsite-pull-waiting")
        self._push_start_time = None
        self._pull_start_time = None
        self._is_standalone = self.ctx.is_standalone
        L.debug(f'inited RollSite. my party id={self.ctx.party_id}. proxy endpoint={self.dst_host}:{self.dst_port}')

    def _run_thread(self, fn, *args, **kwargs):
        return self._receive_executor_pool.submit(fn, *args, **kwargs)


class RollSiteLocalMock(RollSiteBase):
    pass


class RollSite(RollSiteBase):
    def __init__(self, name: str, tag: str, rs_ctx: RollSiteContext, options: dict = None):
        if options is None:
            options = dict()
        super().__init__(name, tag, rs_ctx)
        self.batch_body_bytes = int(RollSiteConfKeys.EGGROLL_ROLLSITE_ADAPTER_SENDBUF_SIZE.get_with(options))
        # TODO:0: configurable
        self.stream_chunk_count = 10
        self.polling_header_timeout = 10 * 60
        self.polling_timeout = 10 * 60
        self.polling_max_retry = 3

    def _push_bytes(self, obj, rs_header: ErRollSiteHeader):
        start_time = time.time()
        data = pickle.dumps(obj)
        rs_key = self._get_rs_key(rs_header)
        int_size = 4
        #
        # def _generate_batch_streams(chunk_count, body_bytes):
        #     total_batches = len(data) // body_bytes + 1
        #     total_streams = total_batches // chunk_count + 1
        #     seq_id = -1
        #
        #     def chunk_batch_stream():
        #         nonlocal seq_id
        #         for i in range(chunk_count):
        #             seq_id += 1
        #             rs_header._seq = seq_id
        #
        #             # TODO:0: consider this protocol
        #             rs_header._batch_streams = total_streams
        #             rs_header._total_partitions = 1
        #             rs_header._partition_id = 0
        #             rs_header._data_type = 'object'
        #             rs_header._stage = "finish_partition"   # storage.py: FINISH_STATUS
        #
        #             batch_bytes = data[seq_id * body_bytes: (seq_id + 1) * body_bytes]
        #             if batch_bytes:
        #                 value = list(TransferPair.pair_to_bin_batch([(seq_id.to_bytes(int_size, "big"), batch_bytes)]))[0]
        #                 yield proxy_pb2.Packet(
        #                         header=proxy_pb2.Metadata(
        #                                 src=proxy_pb2.Topic(partyId=rs_header._src_party_id, role=rs_header._src_role),
        #                                 dst=proxy_pb2.Topic(partyId=rs_header._dst_party_id, role=rs_header._dst_role),
        #                                 ext=rs_header.to_proto_string(),
        #                                 version="2.2.0"),
        #                         body=proxy_pb2.Data(value=value))
        #             else:
        #                 # out of len
        #                 break
        #
        #     for s in range(total_streams):
        #         yield chunk_batch_stream()

        #batch_streams = _generate_batch_streams(self.stream_chunk_count, self.batch_body_bytes - int_size - 8)
        # for batch_stream in batch_streams:
        #     batches = TransferPair.pair_to_bin_batch(batch_stream)
        #     def _generate_transfer_batches(bytes_iter):
        #         for b in bytes_iter:
        #             yield TransferBatch(TransferHeader())

        # bin format: key_len=4, key=4, val_len=4, val=?,  we count val only
        # batch_streams = TransferPair.pair_to_bin_batch(
        #     _generate_batch_streams(self.stream_chunk_count, self.batch_body_bytes - int_size - 8),
        #     sendbuf_size=self.batch_body_bytes)

        def _generate_obj_bytes(py_obj, body_bytes):
            key_id = 0
            obj_bytes = pickle.dumps(py_obj)
            obj_bytes_len = len(obj_bytes)
            cur_pos = 0

            while cur_pos <= obj_bytes_len:
                yield key_id.to_bytes(int_size, "big"), obj_bytes[cur_pos:cur_pos + body_bytes]
                key_id += 1
                cur_pos += body_bytes

        bin_batch_streams = self._generate_batch_streams(
                pair_iter=_generate_obj_bytes(obj, self.batch_body_bytes),
                chunk_size=self.stream_chunk_count,
                body_bytes=self.batch_body_bytes)

        rs_header._total_partitions = 1
        rs_header._partition_id = 0
        rs_header._data_type = 'object'

        # if use stub.push.future here, retry mechanism is a problem to solve
        for batch_stream in bin_batch_streams:
            self.stub.push(self.generate_packet(batch_stream, rs_header))

        L.debug(f"pushed object: rs_key={rs_key}, is_none={obj is None}, time_cost={time.time() - start_time}")
        self.ctx.pushing_latch.count_down()

    @staticmethod
    def generate_packet(bin_batch_iter, rs_header: ErRollSiteHeader):
        def encode_packet():
            rs_header._seq = seq
            return proxy_pb2.Packet(
                    header=proxy_pb2.Metadata(
                            src=proxy_pb2.Topic(partyId=rs_header._src_party_id, role=rs_header._src_role),
                            dst=proxy_pb2.Topic(partyId=rs_header._dst_party_id, role=rs_header._dst_role),
                            seq=seq,
                            ext=rs_header.to_proto_string(),
                            version="2.2.0"),
                    body=proxy_pb2.Data(value=bin_batch))

        prev_batch = None
        seq = 0
        for bin_batch in bin_batch_iter:
            if prev_batch:
                yield encode_packet()
                seq += 1
            prev_batch = bin_batch

        rs_header._stage = "finish_partition"
        yield encode_packet()

    @staticmethod
    def _generate_batch_streams(pair_iter, chunk_size, body_bytes):
        batches = TransferPair.pair_to_bin_batch(pair_iter, sendbuf_size=body_bytes)

        def chunk_batch_stream():
            try:
                for i in range(chunk_size - 1):
                    yield next(batches)
            except StopIteration as e:
                L.info("stop iteration in chunk_batch_stream")

        for first in batches:
             yield itertools.chain([first], chunk_batch_stream())

    def _push_rollpair(self, rp, rs_header):
        rs_key = self._get_rs_key(rs_header)
        if L.isEnabledFor(logging.DEBUG):
            L.debug(f"pushing object: rs_key={rs_key}, count={rp.count()}")
        start_time = time.time()

        def _push_partiton(inputs):
            partition = inputs[0]
            written_batched = 0
            written_pairs = 0

            with create_adapter(partition) as db, db.iteritems() as rb:
                for batch_stream in self._generate_batch_streams(rb, self.stream_chunk_count, self.batch_body_bytes):
                    self.stub.push(batch_stream)

        rp.with_stores(_push_partiton)
        if L.isEnabledFor(logging.DEBUG):
            L.debug(f"pushed object: rs_key={rs_key}, count={rp.count()}, time_cost={time.time() - start_time}")
        self.ctx.pushing_latch.count_down()

    def _pull_one(self, rs_header: ErRollSiteHeader):
        start_time = time.time()
        rs_key = rp_name = self._get_rs_key(rs_header)
        rp_namespace = self.roll_site_session_id
        transfer_tag_prefix = "putBatch-" + self._get_rs_key(rs_header) + "-"
        last_total_batches = None
        polling_attempts = 0
        data_type = None
        try:
            # make sure rollpair already created
            header = self.ctx.rp_ctx.load(name=STATUS_TABLE_NAME, namespace=rp_namespace,
                                          options={'create_if_missing': False, 'total_partitions': 1}).with_stores(
                lambda x: PutBatchTask(transfer_tag_prefix + "0").get_header(self.polling_header_timeout))
            if header is None:
                raise IOError(f"roll site pull_status failed: rs_key={rs_key}, timeout={self.polling_header_timeout}")
            else:
                # TODO:0:  push bytes has only one partition, that means it has finished, need not get_status
                data_type = header.data_type
                L.debug(f"roll site pull_status ok: rs_key={rs_key}, header={header}")
            for i in range(self.polling_max_retry):
                polling_attempts = i
                total_batches = 0
                all_finished = True
                all_status = self.ctx.rp_ctx.load(name=rp_name, namespace=rp_namespace,
                                                  options={'create_if_missing': False}).with_stores(
                    lambda x: PutBatchTask(transfer_tag_prefix + str(x[0]._id), None).get_status(self.polling_timeout))
                for part_id, part_status in all_status:
                    part_finished, part_batches, part_counter, _ = part_status
                    if not part_finished:
                        all_finished = False
                    total_batches += part_batches
                if not all_finished and last_total_batches == total_batches:
                    raise IOError(f"roll site pull_waiting failed because there is no updated progress: rs_key={rs_key}"
                                  f"detail={all_status}")
                last_total_batches = total_batches

                if all_finished:
                    rp = self.ctx.rp_ctx.load(name=rp_name, namespace=rp_namespace)
                    if data_type == "object":
                        result = pickle.loads("".join(sorted(rp.get_all(), key=lambda x: int.from_bytes(x[0], "big"))))
                        L.debug(f"roll site pulled object: rs_key={rs_key}, is_none={result is None}, "
                                f"time_cost={time.time() - start_time}")
                    else:
                        result = rp
                        if L.isEnabledFor(logging.DEBUG):
                            L.debug(f"roll site pulled roll_pair: rs_key={rs_key}, count={rp.count()}, "
                                    f"time_cost={time.time() - start_time}")
                    return result
                else:
                    L.debug(f"roll site pulling attempt: rs_key={rs_key}, attempt={polling_attempts}"
                            f"time_cost={time.time() - start_time}")
            raise IOError(f"roll site polling failed because exceed max try: {self.polling_max_retry}, rs_key={rs_key}"
                          f"detail={all_status}")
        except Exception as ex:
            L.exception(f"fatal error: when pulling rs_key={rs_key}, attempt_count={polling_attempts}")
            raise ex

    def _get_rs_key(self, header: ErRollSiteHeader):
        return RS_KEY_DELIM.join([RS_KEY_PREFIX,
                                  header._roll_site_session_id, header._name, header._tag,
                                  header._src_role, header._src_party_id, header._dst_role, header._dst_party_id])

    def push(self, obj, parties: list = None):
        futures = []
        for role_party_id in parties:
            self.ctx.pushing_latch.count_up()
            dst_role = role_party_id[0]
            dst_party_id = str(role_party_id[1])
            data_type = 'rollpair' if isinstance(obj, RollPair) else 'object'
            rs_header = ErRollSiteHeader(
                roll_site_session_id=self.roll_site_session_id, name=self.name, tag=self.tag,
                src_role=self.local_role, src_party_id=self.party_id, dst_role=dst_role, dst_party_id=dst_party_id,
                data_type=data_type)
            if isinstance(obj, RollPair):
                future = self._run_thread(self._push_bytes, obj, rs_header)
            else:
                future = self._run_thread(self._push_rollpair, obj, rs_header)
            futures.append(future)
        return futures

    def pull(self, parties: list = None):
        futures = []
        for src_role, src_party_id in parties:
            src_party_id = str(src_party_id)
            rs_header = ErRollSiteHeader(
                roll_site_session_id=self.roll_site_session_id, name=self.name, tag=self.tag,
                src_role=src_role, src_party_id=src_party_id, dst_role=self.local_role, dst_party_id=self.party_id)
            futures.append(self._receive_executor_pool.submit(self._pull_one, rs_header))
        return futures
