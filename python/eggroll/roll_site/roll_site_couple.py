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
import logging
import pickle
import threading
import time

from eggroll import __version__ as eggroll_version
from eggroll.core.conf_keys import RollSiteConfKeys, \
    CoreConfKeys
from eggroll.core.datastructure import create_executor_pool
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErEndpoint
from eggroll.core.proto import proxy_pb2, proxy_pb2_grpc
from eggroll.core.serdes import eggroll_serdes
from eggroll.core.transfer_model import ErRollSiteHeader
from eggroll.roll_pair import create_adapter
from eggroll.roll_pair.roll_pair import RollPair, RollPairContext
from eggroll.roll_pair.task.storage import PutBatchTask, FINISH_STATUS
from eggroll.roll_pair.transfer_pair import TransferPair
from eggroll.utils import log_utils
from grpc import RpcError

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
        self._options = options
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
        self._wait_push_exit_timeout = int(RollSiteConfKeys.EGGROLL_ROLLSITE_PUSH_OVERALL_TIMEOUT_SEC.get_with(options))

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
        final_options = self._options.copy().update(options)
        return RollSite(name, tag, self, options=final_options)


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
        #self.stub = self.ctx.stub
        if RollSiteBase._receive_executor_pool is None:
            receive_executor_pool_size = int(RollSiteConfKeys.EGGROLL_ROLLSITE_RECEIVE_EXECUTOR_POOL_MAX_SIZE.get_with(options))
            receive_executor_pool_type = CoreConfKeys.EGGROLL_CORE_DEFAULT_EXECUTOR_POOL.get_with(options)
            self._receive_executor_pool = create_executor_pool(
                    canonical_name=receive_executor_pool_type,
                    max_workers=receive_executor_pool_size,
                    thread_name_prefix="rollsite-client")
        self._push_start_time = None
        self._pull_start_time = None
        self._is_standalone = self.ctx.is_standalone
        L.debug(f'inited RollSite. my party_id={self.ctx.party_id}. proxy endpoint={self.dst_host}:{self.dst_port}')

    def _run_thread(self, fn, *args, **kwargs):
        return self._receive_executor_pool.submit(fn, *args, **kwargs)


class RollSiteLocalMock(RollSiteBase):
    pass


class _BatchStreamHelper(object):
    def __init__(self, rs_header: ErRollSiteHeader):
        self._rs_header = rs_header
        self._rs_header._batch_seq = 0
        self._rs_header._stream_seq = 0
        self._finish_partition = False
        self._last_batch_seq = 0
        self._last_stream_seq = 0

    def generate_packet(self, bin_batch_iter, cur_retry):
        if cur_retry == 0:
            self._last_batch_seq = self._rs_header._batch_seq
            self._last_stream_seq = self._rs_header._stream_seq
        else:
            self._rs_header._batch_seq = self._last_batch_seq
            self._rs_header._stream_seq = self._last_stream_seq

        def encode_packet(rs_header_inner, batch_inner):
            header = proxy_pb2.Metadata(
                    src=proxy_pb2.Topic(partyId=rs_header_inner._src_party_id, role=rs_header_inner._src_role),
                    dst=proxy_pb2.Topic(partyId=rs_header_inner._dst_party_id, role=rs_header_inner._dst_role),
                    seq=rs_header_inner._batch_seq,
                    ext=rs_header_inner.to_proto_string(),
                    version=eggroll_version)

            if batch_inner:
                result = proxy_pb2.Packet(header=header, body=proxy_pb2.Data(value=batch_inner))
            else:
                result = proxy_pb2.Packet(header=header)

            return result

        prev_batch = None
        for bin_batch in bin_batch_iter:
            if prev_batch:
                self._rs_header._batch_seq += 1
                yield encode_packet(self._rs_header, prev_batch)
            prev_batch = bin_batch

        self._rs_header._batch_seq += 1
        if self._finish_partition:
            self._rs_header._stage = FINISH_STATUS
            self._rs_header._total_streams = self._rs_header._stream_seq
            self._rs_header._total_batches = self._rs_header._batch_seq
        yield encode_packet(self._rs_header, prev_batch)

    def _generate_batch_streams(self, pair_iter, batches_per_stream, body_bytes):
        batches = TransferPair.pair_to_bin_batch(pair_iter, sendbuf_size=body_bytes)
        try:
            peek = next(batches)
        except StopIteration as e:
            self._finish_partition = True

        def chunk_batch_stream():
            nonlocal self
            nonlocal peek
            cur_batch = peek
            try:
                for i in range(batches_per_stream - 1):
                    next_batch = next(batches)
                    yield cur_batch
                    cur_batch = next_batch
                peek = next(batches)
            except StopIteration as e:
                self._finish_partition = True
            finally:
                yield cur_batch

        try:
            while not self._finish_partition:
                self._rs_header._stream_seq += 1
                yield chunk_batch_stream()
        except Exception as e:
            L.exception(f'error in generating stream, rs_key={self._rs_header.get_rs_key()}, rs_header={self._rs_header}')


class RollSite(RollSiteBase):
    def __init__(self, name: str, tag: str, rs_ctx: RollSiteContext, options: dict = None):
        if options is None:
            options = dict()
        super().__init__(name, tag, rs_ctx)
        self.batch_body_bytes = int(RollSiteConfKeys.EGGROLL_ROLLSITE_ADAPTER_SENDBUF_SIZE.get_with(options))
        # TODO:0: configurable
        self.push_batches_per_stream = int(RollSiteConfKeys.EGGROLL_ROLLSITE_PUSH_BATCHES_PER_STREAM.get_with(options))
        self.push_per_stream_timeout = int(RollSiteConfKeys.EGGROLL_ROLLSITE_PUSH_PER_STREAM_TIMEOUT_SEC.get_with(options))
        self.push_max_retry = int(RollSiteConfKeys.EGGROLL_ROLLSITE_PUSH_MAX_RETRY.get_with(options))
        self.pull_header_interval = int(RollSiteConfKeys.EGGROLL_ROLLSITE_PULL_HEADER_INTERVAL_SEC.get())
        self.pull_header_timeout = int(RollSiteConfKeys.EGGROLL_ROLLSITE_PULL_HEADER_TIMEOUT_SEC.get_with(options))
        self.pull_interval = int(RollSiteConfKeys.EGGROLL_ROLLSITE_PULL_INTERVAL_SEC.get_with(options))
        self.pull_max_retry = int(RollSiteConfKeys.EGGROLL_ROLLSITE_PULL_MAX_RETRY.get_with(options))
        L.debug(f"RollSite __init__: push_batch_per_stream={self.push_batches_per_stream}, "
                f"push_per_stream_timeout={self.push_per_stream_timeout}, "
                f"push_max_retry={self.push_max_retry}, "
                f"pull_header_interval={self.pull_header_interval}, "
                f"pull_header_timeout={self.pull_header_timeout}, "
                f"pull_interval={self.pull_interval}, "
                f"pull_max_retry={self.pull_max_retry}")

    ################## push ##################
    def _push_bytes(self, obj, rs_header: ErRollSiteHeader, options: dict):
        start_time = time.time()

        rs_key = rs_header.get_rs_key()
        int_size = 4

        if L.isEnabledFor(logging.DEBUG):
            L.debug(f"pushing object: rs_key={rs_key}, rs_header={rs_header}")

        def _generate_obj_bytes(py_obj, body_bytes):
            key_id = 0
            obj_bytes = pickle.dumps(py_obj)
            obj_bytes_len = len(obj_bytes)
            cur_pos = 0

            while cur_pos <= obj_bytes_len:
                yield key_id.to_bytes(int_size, "big"), obj_bytes[cur_pos:cur_pos + body_bytes]
                key_id += 1
                cur_pos += body_bytes

        rs_header._partition_id = 0
        rs_header._total_partitions = 1

        serdes = options.get('serdes', None)
        if serdes is not None:
            rs_header._options['serdes'] = serdes

        wrapee_cls = options.get('wrapee_cls', None)
        if serdes is not None:
            rs_header._options['wrapee_cls'] = wrapee_cls
        # NOTICE: all modifications to rs_header are limited in bs_helper.
        # rs_header is shared by bs_helper and here. any modification in bs_helper affects this header.
        # Remind that python's object references are passed by value,
        # meaning the 'pointer' is copied, while the contents are modificable
        bs_helper = _BatchStreamHelper(rs_header=rs_header)
        bin_batch_streams = bs_helper._generate_batch_streams(
                pair_iter=_generate_obj_bytes(obj, self.batch_body_bytes),
                batches_per_stream=self.push_batches_per_stream,
                body_bytes=self.batch_body_bytes)

        grpc_channel_factory = GrpcChannelFactory()
        channel = grpc_channel_factory.create_channel(self.ctx.proxy_endpoint)
        stub = proxy_pb2_grpc.DataTransferServiceStub(channel)

        # if use stub.push.future here, retry mechanism is a problem to solve
        for batch_stream in bin_batch_streams:
            max_retry_cnt = self.push_max_retry
            cur_retry = 0
            per_stream_timeout = self.push_per_stream_timeout

            batch_stream_data = list(batch_stream)
            exception = None
            while cur_retry < max_retry_cnt:
                L.trace(f'pushing object stream. rs_key={rs_key}, rs_header={rs_header}, cur_retry={cur_retry}')
                try:
                    stub.push(bs_helper.generate_packet(batch_stream_data, cur_retry), timeout=per_stream_timeout)
                    exception = None
                    break
                except Exception as e:
                    L.warn(f"push object error. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}, cur_retry={cur_retry}", exc_info=e)
                    time.sleep(min(2 * cur_retry, 20))
                    if isinstance(e, RpcError) and e.code().name == 'UNAVAILABLE':
                        channel = grpc_channel_factory.create_channel(self.ctx.proxy_endpoint, refresh=True)
                        stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
                    exception = e
                finally:
                    cur_retry += 1
            if exception is not None:
                L.exception(f"push object failed. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}, cur_retry={cur_retry}", exc_info=e)
                raise exception
            L.trace(f'pushed object stream. rs_key={rs_key}, rs_header={rs_header}, cur_retry={cur_retry - 1}')

        L.debug(f"pushed object: rs_key={rs_key}, rs_header={rs_header}, is_none={obj is None}, elapsed={time.time() - start_time}")
        self.ctx.pushing_latch.count_down()

    def _push_rollpair(self, rp: RollPair, rs_header: ErRollSiteHeader, options: dict):
        rs_key = rs_header.get_rs_key()
        if L.isEnabledFor(logging.DEBUG):
            L.debug(f"pushing rollpair: rs_key={rs_key}, rs_header={rs_header}, rp.count={rp.count()}")
        start_time = time.time()

        rs_header._total_partitions = rp.get_partitions()
        serdes = options.get('serdes', None)
        if serdes is not None:
            rs_header._options['serdes'] = serdes

        wrapee_cls = options.get('wrapee_cls', None)
        if serdes is not None:
            rs_header._options['wrapee_cls'] = wrapee_cls

        batches_per_stream = self.push_batches_per_stream
        body_bytes = self.batch_body_bytes
        endpoint = self.ctx.proxy_endpoint
        max_retry_cnt = self.push_max_retry
        per_stream_timeout = self.push_per_stream_timeout

        def _push_partition(ertask):
            rs_header._partition_id = ertask._inputs[0]._id
            L.trace(f"pushing rollpair partition. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}")

            from eggroll.core.grpc.factory import GrpcChannelFactory
            from eggroll.core.proto import proxy_pb2_grpc
            grpc_channel_factory = GrpcChannelFactory()

            with create_adapter(ertask._inputs[0]) as db, db.iteritems() as rb:
                # NOTICE AGAIN: all modifications to rs_header are limited in bs_helper.
                # rs_header is shared by bs_helper and here. any modification in bs_helper affects this header.
                # Remind that python's object references are passed by value,
                # meaning the 'pointer' is copied, while the contents are modificable
                bs_helper = _BatchStreamHelper(rs_header)
                bin_batch_streams = bs_helper._generate_batch_streams(pair_iter=rb,
                                                                      batches_per_stream=batches_per_stream,
                                                                      body_bytes=body_bytes)

                channel = grpc_channel_factory.create_channel(endpoint)
                stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
                for batch_stream in bin_batch_streams:
                    batch_stream_data = list(batch_stream)
                    cur_retry = 0
                    exception = None
                    while cur_retry < max_retry_cnt:
                        L.trace(f'pushing rollpair partition stream. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}, cur_retry={cur_retry}')
                        try:
                            stub.push(bs_helper.generate_packet(batch_stream_data, cur_retry), timeout=per_stream_timeout)
                            exception = None
                            break
                        except Exception as e:
                            L.warn(f"push partition error. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}, cur_retry={cur_retry}", exc_info=e)
                            time.sleep(min(2 * cur_retry, 20))
                            if isinstance(e, RpcError) and e.code().name == 'UNAVAILABLE':
                                channel = grpc_channel_factory.create_channel(endpoint, refresh=True)
                                stub = proxy_pb2_grpc.DataTransferServiceStub(channel)

                            exception = e
                        finally:
                            cur_retry += 1
                    if exception is not None:
                        L.exception(f"push partition failed. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}, cur_retry={cur_retry}", exc_info=exception)
                        raise exception
                    L.trace(f'pushed rollpair partition stream. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}, retry count={cur_retry - 1}')

            L.trace(f"pushed rollpair partition. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}")

        rp.with_stores(_push_partition, options={"__op": "push_partition"})
        if L.isEnabledFor(logging.DEBUG):
            L.debug(f"pushed rollpair: rs_key={rs_key}, rs_header={rs_header}, count={rp.count()}, elapsed={time.time() - start_time}")
        self.ctx.pushing_latch.count_down()

    ################## pull ##################

    def _pull_one(self, rs_header: ErRollSiteHeader, options: dict):
        start_time = time.time()
        rs_key = rp_name = rs_header.get_rs_key()
        rp_namespace = self.roll_site_session_id
        transfer_tag_prefix = "putBatch-" + rs_header.get_rs_key() + "-"
        last_total_batches = None
        last_cur_pairs = -1
        pull_attempts = 0
        data_type = None
        L.debug(f'pulling rs_key={rs_key}')
        try:
            # make sure rollpair already created
            pull_header_interval = self.pull_header_interval
            pull_header_timeout = self.pull_header_timeout        # skips pickling self
            pull_interval = self.pull_interval      # skips pickling self

            def get_partition_status(task):
                put_batch_task = PutBatchTask(transfer_tag_prefix + str(task._inputs[0]._id), None)
                return put_batch_task.get_status(pull_interval)

            def get_status(roll_site):
                pull_status = {}
                total_pairs = 0
                total_batches = 0
                all_finished = True

                final_options = options.copy()
                final_options['create_if_missing'] = False
                store = roll_site.ctx.rp_ctx.load(name=rp_name,
                                                  namespace=rp_namespace,
                                                  options=final_options)

                if store is None:
                    raise ValueError(f'illegal state for rp_name={rp_name}, rp_namespace={rp_namespace}')

                all_status = store.with_stores(get_partition_status, options={"__op": "get_partition_status"})

                for part_id, part_status in all_status:
                    if not part_status.is_finished:
                        all_finished = False
                    pull_status[part_id] = part_status
                    total_batches += part_status.total_batches
                    total_pairs += part_status.total_pairs

                return pull_status, all_finished, total_batches, total_pairs

            def clear_status(task):
                return PutBatchTask(transfer_tag_prefix + str(task._inputs[0]._id)).clear_status()

            wait_time = 0
            header_response = None
            while wait_time < pull_header_timeout and \
                    (header_response is None or not isinstance(header_response[0][1], ErRollSiteHeader)):
                final_options = options.copy()
                final_options['create_if_missing'] = True
                final_options['total_partitions'] = 1
                header_response = self.ctx.rp_ctx.load(name=STATUS_TABLE_NAME,
                                                       namespace=rp_namespace,
                                                       options=final_options) \
                    .with_stores(lambda x: PutBatchTask(transfer_tag_prefix + "0").get_header(pull_header_interval), options={"__op": "pull_header"})
                wait_time += pull_header_interval

                #pull_status, all_finished, total_batches, total_pairs = stat_all_status(self)
                L.debug(f"roll site get header_response: rs_key={rs_key}, rs_header={rs_header}, wait_time={wait_time}")

            if header_response is None or not isinstance(header_response[0][1], ErRollSiteHeader):
                raise IOError(f"roll site pull header failed: rs_key={rs_key}, rs_header={rs_header}, timeout={self.pull_header_timeout}")
            else:
                header: ErRollSiteHeader = header_response[0][1]
                # TODO:0:  push bytes has only one partition, that means it has finished, need not get_status
                data_type = header._data_type
                L.debug(f"roll site pull header successful: rs_key={rs_key}, rs_header={header}")

            pull_status = {}
            for cur_retry in range(self.pull_max_retry):
                pull_attempts = cur_retry
                pull_status, all_finished, total_batches, cur_pairs = get_status(self)

                if not all_finished:
                    L.debug(f'getting status NOT finished for rs_key={rs_key}, '
                            f'rs_header={rs_header}, '
                            f'cur_status={pull_status}, '
                            f'attempts={pull_attempts}, '
                            f'cur_pairs={cur_pairs}, '
                            f'last_cur_pairs={last_cur_pairs}, '
                            f'total_batches={total_batches}, '
                            f'last_total_batches={last_total_batches}, '
                            f'elapsed={time.time() - start_time}')
                    if last_cur_pairs == cur_pairs and cur_pairs > 0:
                        raise IOError(f"roll site pull waiting failed because there is no updated progress: rs_key={rs_key}, "
                                      f"rs_header={rs_header}, pull_status={pull_status}, last_cur_pairs={last_cur_pairs}, cur_pairs={cur_pairs}")
                else:
                    L.debug(f"getting status DO finished for rs_key={rs_key}, rs_header={rs_header}, pull_status={pull_status}, cur_pairs={cur_pairs}, total_batches={total_batches}")
                    rp = self.ctx.rp_ctx.load(name=rp_name, namespace=rp_namespace)

                    clear_future = self._receive_executor_pool.submit(rp.with_stores, clear_status, options={"__op": "clear_status"})
                    if data_type == "object":
                        result = pickle.loads(b''.join(map(lambda t: t[1], sorted(rp.get_all(), key=lambda x: int.from_bytes(x[0], "big")))))
                        rp.destroy()
                        L.debug(f"pulled object: rs_key={rs_key}, rs_header={rs_header}, is_none={result is None}, "
                                f"elapsed={time.time() - start_time}")
                    else:
                        result = rp

                        if L.isEnabledFor(logging.DEBUG):
                            L.debug(f"pulled roll_pair: rs_key={rs_key}, rs_header={rs_header}, rp.count={rp.count()}, "
                                    f"elapsed={time.time() - start_time}")

                    clear_future.result()
                    return result
                last_total_batches = total_batches
                last_cur_pairs = cur_pairs
            raise IOError(f"roll site pull failed. max try exceeded: {self.pull_max_retry}, rs_key={rs_key}, "
                          f"rs_header={rs_header}, pull_status={pull_status}")
        except Exception as e:
            L.exception(f"fatal error: when pulling rs_key={rs_key}, rs_header={rs_header}, attempts={pull_attempts}")
            raise e

    def push(self, obj, parties: list = None, options: dict = None):
        if options is None:
            options = {}
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
                future = self._run_thread(self._push_rollpair, obj, rs_header, options)
            else:
                future = self._run_thread(self._push_bytes, obj, rs_header, options)
            futures.append(future)
        return futures

    def pull(self, parties: list = None, options: dict = None):
        if options is None:
            options = {}
        futures = []
        for src_role, src_party_id in parties:
            src_party_id = str(src_party_id)
            rs_header = ErRollSiteHeader(
                    roll_site_session_id=self.roll_site_session_id, name=self.name, tag=self.tag,
                    src_role=src_role, src_party_id=src_party_id, dst_role=self.local_role, dst_party_id=self.party_id)
            futures.append(self._receive_executor_pool.submit(self._pull_one, rs_header, options))
        return futures
