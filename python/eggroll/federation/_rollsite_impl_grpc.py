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
import random
import time
import typing

from grpc import RpcError

from eggroll import __version__ as eggroll_version
from eggroll.computing import RollPair
from eggroll.computing.tasks import consts, store, transfer
from eggroll.computing.tasks.store import StoreTypes
from eggroll.config import Config
from eggroll.config import ConfigKey
from eggroll.config import ConfigUtils
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErTask, ErRollSiteHeader
from eggroll.core.proto import proxy_pb2, proxy_pb2_grpc
from ._rollsite_impl_base import RollSiteImplBase

if typing.TYPE_CHECKING:
    from ._rollsite_context import RollSiteContext
L = logging.getLogger(__name__)
STATUS_TABLE_NAME = "__rs_status"


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
                src=proxy_pb2.Topic(
                    partyId=rs_header_inner._src_party_id,
                    role=rs_header_inner._src_role,
                ),
                dst=proxy_pb2.Topic(
                    partyId=rs_header_inner._dst_party_id,
                    role=rs_header_inner._dst_role,
                ),
                seq=rs_header_inner._batch_seq,
                ext=rs_header_inner.to_proto_string(),
                version=eggroll_version,
            )

            if batch_inner:
                result = proxy_pb2.Packet(
                    header=header, body=proxy_pb2.Data(value=batch_inner)
                )
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
            self._rs_header._stage = consts.FINISH_STATUS
            self._rs_header._total_streams = self._rs_header._stream_seq
            self._rs_header._total_batches = self._rs_header._batch_seq
        yield encode_packet(self._rs_header, prev_batch)

    def _generate_batch_streams(
        self, config: Config, pair_iter, batches_per_stream, body_bytes
    ):
        batches = transfer.TransferPair.pair_to_bin_batch(
            config=config, input_iter=pair_iter, sendbuf_size=body_bytes
        )
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
            L.exception(
                f"error in generating stream, rs_key={self._rs_header.get_rs_key()}, rs_header={self._rs_header}"
            )


class RollSiteGrpc(RollSiteImplBase):
    def __init__(
        self, name: str, tag: str, rs_ctx: "RollSiteContext", options: dict = None
    ):
        if options is None:
            options = dict()
        super().__init__(name, tag, rs_ctx)
        config = rs_ctx.config
        self.batch_body_bytes = ConfigUtils.get_option(
            config, options, ConfigKey.eggroll.rollsite.adapter.sendbuf.size
        )
        # TODO:0: configurable
        self.push_batches_per_stream = ConfigUtils.get_option(
            config, options, ConfigKey.eggroll.rollsite.push.batches.per.stream
        )
        self.push_per_stream_timeout = ConfigUtils.get_option(
            config, options, ConfigKey.eggroll.rollsite.push.stream.timeout.sec
        )
        self.push_max_retry = ConfigUtils.get_option(
            config, options, ConfigKey.eggroll.rollsite.push.max.retry
        )
        self.push_long_retry = ConfigUtils.get_option(
            config, options, ConfigKey.eggroll.rollsite.push.long.retry
        )
        self.pull_header_interval = ConfigUtils.get_option(
            config, options, ConfigKey.eggroll.rollsite.pull.header.interval.sec
        )
        self.pull_header_timeout = ConfigUtils.get_option(
            config, options, ConfigKey.eggroll.rollsite.pull.header.timeout.sec
        )
        self.pull_interval = ConfigUtils.get_option(
            config, options, ConfigKey.eggroll.rollsite.pull.interval.sec
        )
        self.pull_max_retry = ConfigUtils.get_option(
            config, options, ConfigKey.eggroll.rollsite.pull.max.retry
        )
        L.debug(
            f"RollSite __init__: push_batch_per_stream={self.push_batches_per_stream}, "
            f"push_per_stream_timeout={self.push_per_stream_timeout}, "
            f"push_max_retry={self.push_max_retry}, "
            f"pull_header_interval={self.pull_header_interval}, "
            f"pull_header_timeout={self.pull_header_timeout}, "
            f"pull_interval={self.pull_interval}, "
            f"pull_max_retry={self.pull_max_retry}"
        )

    def _push_bytes(self, obj, rs_header: ErRollSiteHeader, options: dict = None):
        if options is None:
            options = {}
        start_time = time.time()

        rs_key = rs_header.get_rs_key()
        int_size = 4

        if L.isEnabledFor(logging.DEBUG):
            L.debug(f"pushing object: rs_key={rs_key}, rs_header={rs_header}")

        def _generate_obj_bytes(obj_bytes, body_bytes):
            key_id = 0
            obj_bytes_len = len(obj_bytes)
            cur_pos = 0

            while cur_pos <= obj_bytes_len:
                yield key_id.to_bytes(int_size, "big"), obj_bytes[
                    cur_pos : cur_pos + body_bytes
                ]
                key_id += 1
                cur_pos += body_bytes

        rs_header._partition_id = 0
        rs_header._total_partitions = 1

        # NOTICE: all modifications to rs_header are limited in bs_helper.
        # rs_header is shared by bs_helper and here. any modification in bs_helper affects this header.
        # Remind that python's object references are passed by value,
        # meaning the 'pointer' is copied, while the contents are modificable
        bs_helper = _BatchStreamHelper(rs_header=rs_header)
        bin_batch_streams = bs_helper._generate_batch_streams(
            config=self.ctx.config,
            pair_iter=_generate_obj_bytes(obj, self.batch_body_bytes),
            batches_per_stream=self.push_batches_per_stream,
            body_bytes=self.batch_body_bytes,
        )

        grpc_channel_factory = GrpcChannelFactory()
        channel = grpc_channel_factory.create_channel(
            config=self.ctx.config, endpoint=self.ctx.proxy_endpoint
        )
        stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
        max_retry_cnt = self.push_max_retry
        long_retry_cnt = self.push_long_retry
        per_stream_timeout = self.push_per_stream_timeout

        # if use stub.push.future here, retry mechanism is a problem to solve
        for batch_stream in bin_batch_streams:
            cur_retry = 0
            batch_stream_data = list(batch_stream)
            exception = None
            while cur_retry < max_retry_cnt:
                L.debug(
                    f"pushing object stream. rs_key={rs_key}, rs_header={rs_header}, cur_retry={cur_retry}"
                )
                try:
                    stub.push(
                        bs_helper.generate_packet(batch_stream_data, cur_retry),
                        timeout=per_stream_timeout,
                    )
                    exception = None
                    break
                except Exception as e:
                    if cur_retry <= max_retry_cnt - long_retry_cnt:
                        retry_interval = round(
                            min(2 * cur_retry, 20) + random.random() * 10, 3
                        )
                    else:
                        retry_interval = round(300 + random.random() * 10, 3)
                    L.warning(
                        f"push object error. rs_key={rs_key}, partition_id={rs_header.partition_id}, rs_header={rs_header}, max_retry_cnt={max_retry_cnt}, cur_retry={cur_retry}, retry_interval={retry_interval}",
                        exc_info=e,
                    )
                    time.sleep(retry_interval)
                    if isinstance(e, RpcError) and e.code().name == "UNAVAILABLE":
                        channel = grpc_channel_factory.create_channel(
                            config=self.ctx.config,
                            endpoint=self.ctx.proxy_endpoint,
                            refresh=True,
                        )
                        stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
                    exception = e
                finally:
                    cur_retry += 1
            if exception is not None:
                L.exception(
                    f"push object failed. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}, cur_retry={cur_retry}",
                    exc_info=exception,
                )
                raise exception
            L.debug(
                f"pushed object stream. rs_key={rs_key}, rs_header={rs_header}, cur_retry={cur_retry - 1}"
            )

        L.debug(
            f"pushed object: rs_key={rs_key}, rs_header={rs_header}, is_none={obj is None}, elapsed={time.time() - start_time}"
        )
        self.ctx.pushing_latch.count_down()

    def _push_rollpair(
        self, rp: RollPair, rs_header: ErRollSiteHeader, options: dict = None
    ):
        if options is None:
            options = {}
        rs_key = rs_header.get_rs_key()
        if L.isEnabledFor(logging.DEBUG):
            L.debug(
                f"pushing rollpair: rs_key={rs_key}, rs_header={rs_header}, rp.count={rp.count()}"
            )
        start_time = time.time()

        rs_header._total_partitions = rp.num_partitions
        serdes = options.get("serdes", None)
        if serdes is not None:
            rs_header._options["serdes"] = serdes

        wrapee_cls = options.get("wrapee_cls", None)
        if wrapee_cls is not None:
            rs_header._options["wrapee_cls"] = wrapee_cls

        batches_per_stream = self.push_batches_per_stream
        body_bytes = self.batch_body_bytes
        endpoint = self.ctx.proxy_endpoint
        max_retry_cnt = self.push_max_retry
        long_retry_cnt = self.push_long_retry
        per_stream_timeout = self.push_per_stream_timeout

        def _push_partition(_config: Config, data_dir, ertask: ErTask):
            rs_header._partition_id = ertask.first_input.id
            L.debug(
                f"pushing rollpair partition. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}"
            )

            from eggroll.core.grpc.factory import GrpcChannelFactory
            from eggroll.core.proto import proxy_pb2_grpc

            grpc_channel_factory = GrpcChannelFactory()
            with store.get_adapter(
                ertask.first_input, data_dir
            ) as db, db.iteritems() as rb:
                # NOTICE AGAIN: all modifications to rs_header are limited in bs_helper.
                # rs_header is shared by bs_helper and here. any modification in bs_helper affects this header.
                # Remind that python's object references are passed by value,
                # meaning the 'pointer' is copied, while the contents are modificable
                bs_helper = _BatchStreamHelper(rs_header)
                bin_batch_streams = bs_helper._generate_batch_streams(
                    config=_config,
                    pair_iter=rb,
                    batches_per_stream=batches_per_stream,
                    body_bytes=body_bytes,
                )

                channel = grpc_channel_factory.create_channel(
                    config=_config, endpoint=endpoint
                )
                stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
                for batch_stream in bin_batch_streams:
                    batch_stream_data = list(batch_stream)
                    cur_retry = 0
                    exception = None
                    while cur_retry < max_retry_cnt:
                        L.debug(
                            f"pushing rollpair partition stream. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}, cur_retry={cur_retry}"
                        )
                        try:
                            stub.push(
                                bs_helper.generate_packet(batch_stream_data, cur_retry),
                                timeout=per_stream_timeout,
                            )
                            exception = None
                            break
                        except Exception as e:
                            if cur_retry < max_retry_cnt - long_retry_cnt:
                                retry_interval = round(
                                    min(2 * cur_retry, 20) + random.random() * 10, 3
                                )
                            else:
                                retry_interval = round(300 + random.random() * 10, 3)
                            L.warning(
                                f"push rp partition error. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}, max_retry_cnt={max_retry_cnt}, cur_retry={cur_retry}, retry_interval={retry_interval}",
                                exc_info=e,
                            )
                            time.sleep(retry_interval)
                            if (
                                isinstance(e, RpcError)
                                and e.code().name == "UNAVAILABLE"
                            ):
                                channel = grpc_channel_factory.create_channel(
                                    config=_config,
                                    endpoint=endpoint,
                                    refresh=True,
                                )
                                stub = proxy_pb2_grpc.DataTransferServiceStub(channel)

                            exception = e
                        finally:
                            cur_retry += 1
                    if exception is not None:
                        L.exception(
                            f"push partition failed. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}, cur_retry={cur_retry}",
                            exc_info=exception,
                        )
                        raise exception
                    L.debug(
                        f"pushed rollpair partition stream. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}, retry count={cur_retry - 1}"
                    )

            L.debug(
                f"pushed rollpair partition. rs_key={rs_key}, partition_id={rs_header._partition_id}, rs_header={rs_header}"
            )

        if self.ctx.push_session_enabled:
            rp_to_push = self.ctx._push_rp_ctx.load_rp(
                name=rp.get_name(),
                namespace=rp.get_namespace(),
                store_type=StoreTypes.ROLLPAIR_IN_MEMORY,
            )
            rp_to_push.with_stores(_push_partition, options={"__op": "push_partition"})
        else:
            rp.with_stores(_push_partition, options={"__op": "push_partition"})
        if L.isEnabledFor(logging.DEBUG):
            L.debug(
                f"pushed rollpair: rs_key={rs_key}, rs_header={rs_header}, count={rp.count()}, elapsed={time.time() - start_time}"
            )
        self.ctx.pushing_latch.count_down()

    def _pull_one(self, rs_header: ErRollSiteHeader):
        start_time = time.time()
        rs_key = rp_name = rs_header.get_rs_key()
        rp_namespace = self.roll_site_session_id
        transfer_tag_prefix = "putBatch-" + rs_header.get_rs_key() + "-"
        last_total_batches = None
        last_cur_pairs = -1
        pull_attempts = 0
        data_type = None
        L.debug(f"pulling rs_key={rs_key}")
        try:
            wait_time = 0
            header_response = None
            while wait_time < self.pull_header_timeout and (
                header_response is None
                or not isinstance(header_response[0], ErRollSiteHeader)
            ):
                rp = self.ctx.rp_ctx.create_rp(
                    id=-1,
                    name=STATUS_TABLE_NAME,
                    namespace=rp_namespace,
                    total_partitions=1,
                    store_type=StoreTypes.ROLLPAIR_LMDB,
                    key_serdes_type=0,
                    value_serdes_type=0,
                    partitioner_type=0,
                    options={},
                    gc_enabled=False,
                )
                header_response = rp.pull_get_header(
                    tag=transfer_tag_prefix + "0", timeout=self.pull_header_timeout
                )

                wait_time += self.pull_header_interval

                L.debug(
                    f"roll site get header_response: rs_key={rs_key}, rs_header={rs_header}, wait_time={wait_time}"
                )

            if header_response is None or not isinstance(
                header_response[0], ErRollSiteHeader
            ):
                raise IOError(
                    f"roll site pull header failed: rs_key={rs_key}, rs_header={rs_header}, timeout={self.pull_header_timeout}, header_response={header_response}"
                )
            else:
                header: ErRollSiteHeader = header_response[0]
                # TODO:0:  push bytes has only one partition, that means it has finished, need not get_status
                data_type = header._data_type
                L.debug(
                    f"roll site pull header successful: rs_key={rs_key}, rs_header={header}"
                )

            pull_status = {}
            for cur_retry in range(self.pull_max_retry):
                pull_attempts = cur_retry
                rp = self.ctx.rp_ctx.load_rp(
                    name=rp_name,
                    namespace=rp_namespace,
                    store_type=StoreTypes.ROLLPAIR_IN_MEMORY,
                    gc_enabled=False,
                )
                (
                    pull_status,
                    all_finished,
                    total_batches,
                    cur_pairs,
                ) = rp.pull_get_partition_status(
                    tag=transfer_tag_prefix, timeout=self.pull_interval
                )

                if not all_finished:
                    L.debug(
                        f"getting status NOT finished for rs_key={rs_key}, "
                        f"rs_header={rs_header}, "
                        f"cur_status={pull_status}, "
                        f"attempts={pull_attempts}, "
                        f"cur_pairs={cur_pairs}, "
                        f"last_cur_pairs={last_cur_pairs}, "
                        f"total_batches={total_batches}, "
                        f"last_total_batches={last_total_batches}, "
                        f"elapsed={time.time() - start_time}"
                    )
                    if last_cur_pairs == cur_pairs and cur_pairs > 0:
                        raise IOError(
                            f"roll site pull waiting failed because there is no updated progress: rs_key={rs_key}, "
                            f"rs_header={rs_header}, pull_status={pull_status}, last_cur_pairs={last_cur_pairs}, cur_pairs={cur_pairs}"
                        )
                else:
                    L.debug(
                        f"getting status DO finished for rs_key={rs_key}, rs_header={rs_header}, pull_status={pull_status}, cur_pairs={cur_pairs}, total_batches={total_batches}"
                    )
                    rp = self.ctx.rp_ctx.load_rp(
                        name=rp_name,
                        namespace=rp_namespace,
                        store_type=StoreTypes.ROLLPAIR_IN_MEMORY,
                        gc_enabled=False,
                    )

                    clear_future = self._receive_executor_pool.submit(
                        rp.pull_clear_status, transfer_tag_prefix
                    )
                    if data_type == "object":
                        result = b"".join(
                            map(
                                lambda t: t[1],
                                sorted(
                                    rp.get_all(),
                                    key=lambda x: int.from_bytes(x[0], "big"),
                                ),
                            )
                        )
                        rp.destroy()
                        L.debug(
                            f"pulled object: rs_key={rs_key}, rs_header={rs_header}, is_none={result is None}, "
                            f"elapsed={time.time() - start_time}"
                        )
                    else:
                        result = rp
                        rp.register_gc()

                        if L.isEnabledFor(logging.DEBUG):
                            L.debug(
                                f"pulled roll_pair: rs_key={rs_key}, rs_header={rs_header}, rp.count={rp.count()}, "
                                f"elapsed={time.time() - start_time}"
                            )

                    clear_future.result()
                    return result
                last_total_batches = total_batches
                last_cur_pairs = cur_pairs
            raise IOError(
                f"roll site pull failed. max try exceeded: {self.pull_max_retry}, rs_key={rs_key}, "
                f"rs_header={rs_header}, pull_status={pull_status}"
            )
        except Exception as e:
            L.exception(
                f"fatal error: when pulling rs_key={rs_key}, rs_header={rs_header}, attempts={pull_attempts}"
            )
            raise e
