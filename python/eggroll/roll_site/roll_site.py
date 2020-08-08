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
from eggroll.core.serdes import eggroll_serdes
from eggroll.core.transfer_model import ErRollSiteHeader
from eggroll.core.utils import _stringify, static_er_conf
from eggroll.core.utils import to_one_line_string
from eggroll.roll_pair import create_adapter
from eggroll.roll_pair.roll_pair import RollPair, RollPairContext
from eggroll.roll_pair.task.storage import PutBatchTask
from eggroll.roll_pair.transfer_pair import TransferPair
from eggroll.roll_site.utils.roll_site_utils import create_store_name, DELIM
from eggroll.utils import log_utils

L = log_utils.get_logger()
P = log_utils.get_logger('profile')
_serdes = eggroll_serdes.PickleSerdes

STATUS_TABLE_NAME = "__roll_site_standalone_status__"


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

    def await(self, timeout=None, attempt=1, after_attempt=None):
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
        residual_count = self.pushing_latch.await(self._wait_push_exit_timeout)
        if residual_count != 0:
            L.error(f"exit session when not finish push: "
                    f"residual_count={residual_count}, timeout={self._wait_push_exit_timeout}")

    def load(self, name: str, tag: str, options: dict = None):
        if options is None:
            options = {}
        return RollSite(name, tag, self, options=options)


ERROR_STATES = [proxy_pb2.STOP, proxy_pb2.KILL]
OBJECT_STORAGE_NAME = "__federation__"
CONF_KEY_TARGET = "rollsite"
CONF_KEY_LOCAL = "local"
CONF_KEY_SERVER = "servers"


class RollSiteBase:
    _executor_pool = None

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
        if self._receive_executor_pool is None:
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

    def is_object(self, object_type):
        # bad smell
        return object_type == b'object' or object_type == 'object'

    # def _fetch_result(self, content_type):
    #     is_object = content_type == b'object' or content_type == 'object'
    #     if self.is_object(obj_type):
    #         result = rp.get(table_name)
    #         L.debug(f"roll site pulled object: table_name={table_name}, is_none={result is None}")
    #     else:
    #     result = rp
    #     if L.isEnabledFor(logging.DEBUG):
    #         L.debug(f"roll site pulled roll_pair: table_name={table_name}, count={rp.count()}")

    def _push_callback(self, fn, tmp_rp):
        self.ctx.pushing_latch.count_down()
        L.debug(f"push {self.tag} callback: total_time={time.time() - self._push_start_time}")


class RollSiteLocalMock(RollSiteBase):

    def _pull_waiting(self, packet, namespace, roll_site_header: ErRollSiteHeader):
        max_retry_cnt = int(RollSiteConfKeys.EGGROLL_ROLLSITE_PULL_CLIENT_MAX_RETRY.get())
        status_rp = self.ctx.rp_ctx.load(namespace, STATUS_TABLE_NAME + DELIM + self.ctx.roll_site_session_id,
                                         options={'create_if_missing': True})
        table_name = create_store_name(roll_site_header)
        retry_cnt = 0
        while True:
            msg = f"retry pull: retry_cnt: {retry_cnt}," + \
                  f" tagged_key: '{table_name}', packet: {to_one_line_string(packet)}, namespace: {namespace}"
            if retry_cnt % 10 == 0:
                L.debug(msg)
            else:
                L.trace(msg)
            retry_cnt += 1
            ret_list = status_rp.get(table_name)
            if ret_list:
                table_namespace = ret_list[2]
                table_name = ret_list[1]
                obj_type = ret_list[0]
                break
            time.sleep(min(0.1 * retry_cnt, 30))
            if retry_cnt > max_retry_cnt:
                raise IOError("receive timeout")
        rp = self.ctx.rp_ctx.load(namespace=table_namespace, name=table_name, options={'create_if_missing': True})
        if self.is_object(obj_type):
            result = rp.get(table_name)
            L.debug(f"roll site pulled object: table_name={table_name}, is_none={result is None}")
        else:
            result = rp
            if L.isEnabledFor(logging.DEBUG):
                L.debug(f"roll site pulled roll_pair: table_name={table_name}, count={rp.count()}")
        return result


class RollSite(RollSiteBase):
    def __init__(self, name: str, tag: str, rs_ctx: RollSiteContext):
        super().__init__(name, tag, rs_ctx)
        self.batch_body_bytes = int(static_er_conf.get(RollSiteConfKeys.EGGROLL_ROLLSITE_ADAPTER_SENDBUF_SIZE.key,
                               RollSiteConfKeys.EGGROLL_ROLLSITE_ADAPTER_SENDBUF_SIZE.default_value))
        self.stream_chunk_count = 10

    def _push_bytes(self, data):
        def _generate_batch_streams(chunk_count, body_bytes):
            total_batches = len(data) // self.batch_body_bytes + 1
            total_streams = total_batches // chunk_count + 1
            seq_id = -1

            def chunk_batch_stream():
                nonlocal seq_id
                for i in range(chunk_count):
                    seq_id += 1
                    batch_bytes = data[seq_id * body_bytes: (seq_id + 1) * body_bytes]
                    if batch_bytes:
                        yield batch_bytes
                    else:
                        # out of len
                        break
            for s in range(total_streams):
                yield chunk_batch_stream()
        for batch_stream in _generate_batch_streams(self.stream_chunk_count, self.batch_body_bytes):
            self.stub.push(batch_stream)

    def _push_rollpair(self):
        def _generate_batch_streams(rb, chunk_size):
            batches = TransferPair.pair_to_bin_batch(rb, sendbuf_size=self.batch_body_bytes)
            def chunk_batch_stream():
                for i in range(chunk_size - 1):
                    yield next(batches)
            for first in batches:
                yield itertools.chain([first], chunk_batch_stream())

        def _push_partiton(inputs):
            partition = inputs[0]
            written_batched = 0
            written_pairs = 0

            with create_adapter(partition) as db, db.iteritems() as rb:
                for batch_stream in _generate_batch_streams(rb):
                    self.stub.push(batch_stream)

        table_name = create_store_name(roll_site_header)
        table_namespace = self.roll_site_session_id
        self.ctx.rp_ctx.load(table_name, table_namespace).with_stores(_push_partiton)

    def _pull_one(self, packet, namespace, roll_site_header: ErRollSiteHeader):
        table_name = create_store_name(roll_site_header)
        table_namespace = self.roll_site_session_id
        tag = table_name
        last_total_batches = None
        max_retry_cnt = int(RollSiteConfKeys.EGGROLL_ROLLSITE_PULL_CLIENT_MAX_RETRY.get())
        attempt_count = 0
        content_type = None
        try:
            for i in range(max_retry_cnt):
                attempt_count = i
                total_batches = 0
                all_finished = True
                all_status = self.ctx.rp_ctx.load(table_name, table_namespace).with_stores(
                    lambda x: PutBatchTask(tag, None).get_status(5 * 60))
                for part_id, part_status in all_status:
                    part_finished, part_batches, part_counter, content_type = part_status
                    if not part_finished:
                        all_finished = False
                    total_batches += part_batches
                if not all_finished and last_total_batches == total_batches:
                    raise IOError(f"roll site pull_waiting failed because there is no updated progress: "
                                  f"detail={all_status}")
                last_total_batches = total_batches

                if all_finished:
                    rp = self.ctx.rp_ctx.load(namespace=table_namespace, name=table_name,
                                              options={'create_if_missing': False})
                    if self.is_object(content_type):
                        result = rp.get(table_name)
                        L.debug(f"roll site pulled object: table_name={table_name}, is_none={result is None}")
                    else:
                        result = rp
                    if L.isEnabledFor(logging.DEBUG):
                        L.debug(f"roll site pulled roll_pair: table_name={table_name}, count={rp.count()}")
                    return result

            raise IOError(f"roll site pull_waiting failed because exceed max retry: retry_cnt={max_retry_cnt}"
                          f"detail={all_status}")
        except Exception as ex:
            L.exception(f"fatal error: when pulling tag={tag}, attempt_count={attempt_count}")
            raise ex

    def push(self, obj, parties: list = None):
        L.debug(f"pushing: self={self.__dict__}, obj_type={type(obj)}, parties={parties}")
        self._push_start_wall_time = time.time()
        self._push_start_cpu_time = time.perf_counter()
        futures = []
        for role_party_id in parties:
            self.ctx.pushing_latch.count_up()
            _role = role_party_id[0]
            _party_id = str(role_party_id[1])

            _options = {}
            obj_type = 'rollpair' if isinstance(obj, RollPair) else 'object'
            roll_site_header = ErRollSiteHeader(
                roll_site_session_id=self.roll_site_session_id,
                name=self.name,
                tag=self.tag,
                src_role=self.local_role,
                src_party_id=self.party_id,
                dst_role=_role,
                dst_party_id=_party_id,
                data_type=obj_type,
                options=_options)
            _tagged_key = create_store_name(roll_site_header)
            L.debug(f"pushing start party={type(obj)}, key={_tagged_key}")
            namespace = self.roll_site_session_id

            self._push_bytes()
            self._push_rollpair()

            future = RollSite._receive_executor_pool.submit(map_values, _tagged_key, self._is_standalone, roll_site_header)
            if not self._is_standalone and (obj_type == 'object' or obj_type == b'object'):
                tmp_rp = rp
            else:
                tmp_rp = None

            future.add_done_callback(functools.partial(self._push_callback, tmp_rp=tmp_rp))
            futures.append(future)

        return futures

    def pull(self, parties: list = None):
        futures = []
        for src_role, src_party_id in parties:
            src_party_id = str(src_party_id)
            roll_site_header = ErRollSiteHeader(
                roll_site_session_id=self.roll_site_session_id,
                name=self.name,
                tag=self.tag,
                src_role=src_role,
                src_party_id=src_party_id,
                dst_role=self.local_role,
                dst_party_id=self.party_id)
            _tagged_key = create_store_name(roll_site_header)

            name = _tagged_key

            model = proxy_pb2.Model(name=_stringify(roll_site_header))
            task_info = proxy_pb2.Task(taskId=name, model=model)
            topic_src = proxy_pb2.Topic(name="get_status", partyId=src_party_id,
                                        role=src_role, callback=None)
            topic_dst = proxy_pb2.Topic(name="get_status", partyId=self.party_id,
                                        role=self.local_role, callback=None)
            get_status_command = proxy_pb2.Command(name="get_status")

            metadata = proxy_pb2.Metadata(task=task_info,
                                          src=topic_src,
                                          dst=topic_dst,
                                          command=get_status_command,
                                          operator="getStatus",
                                          seq=0,
                                          ack=0)

            packet = proxy_pb2.Packet(header=metadata)
            namespace = self.roll_site_session_id
            L.trace(f"pulling prepared tagged_key={_tagged_key}, packet={to_one_line_string(packet)}")
            futures.append(self._executor_pool.submit(self._pull_one, self, packet, namespace, roll_site_header))

        return futures
