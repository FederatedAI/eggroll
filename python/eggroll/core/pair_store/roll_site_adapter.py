#
#  Copyright 2019 The Eggroll Authors. All Rights Reserved.
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
import sys
import time

from eggroll.core.conf_keys import RollSiteConfKeys
from eggroll.core.error import GrpcCallError
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErEndpoint
from eggroll.core.pair_store.adapter import PairWriteBatch, PairIterator, \
    PairAdapter
from eggroll.core.pair_store.format import PairBinWriter, ArrayByteBuffer
from eggroll.core.proto import proxy_pb2, proxy_pb2_grpc
from eggroll.core.serdes import eggroll_serdes
from eggroll.core.transfer_model import ErRollSiteHeader
from eggroll.core.utils import get_static_er_conf
from eggroll.core.utils import stringify_charset
from eggroll.roll_site.utils.roll_site_utils import create_store_name
from eggroll.utils.log_utils import get_logger

L = get_logger()

_serdes = eggroll_serdes.PickleSerdes


class RollSiteAdapter(PairAdapter):
    def __init__(self, options: dict = None):
        if options is None:
            options = {}

        super().__init__(options)

        er_partition = options['er_partition']
        self.partition = er_partition
        self.store_locator = er_partition._store_locator
        self.partition_id = er_partition._id

        self.namespace = self.store_locator._namespace

        #_store_type = StoreTypes.ROLLPAIR_ROLLSITE
        # self._store_locator = meta_pb2.StoreLocator(storeType=_store_type,
        #                                             namespace=self.namespace,
        #                                             name=self.store_locator._name,
        #                                             partitioner=self.store_locator._partitioner,
        #                                             serdes=self.store_locator._serdes,
        #                                             totalPartitions=self.store_locator._total_partitions)

        self.roll_site_header_string = options.get('roll_site_header', None)
        self.is_writable = False
        if self.roll_site_header_string:
            self.roll_site_header = ErRollSiteHeader.from_proto_string(self.roll_site_header_string.encode(stringify_charset))
            self.proxy_endpoint = ErEndpoint.from_proto_string(options['proxy_endpoint'].encode(stringify_charset))
            self.obj_type = options['obj_type']
            self.is_writable = True

            L.info(f"writable RollSiteAdapter: {self.namespace}, {self.partition_id}. proxy_endpoint: {self.proxy_endpoint}, partition: {self.partition}")

    def close(self):
        pass

    def iteritems(self):
        return RollSiteIterator(self)

    def new_batch(self):
        if not self.is_writable:
            raise RuntimeError(f'RollSiteAdapter not writable for {self.partition}')
        return RollSiteWriteBatch(self)

    def get(self, key):
        pass

    def put(self, key, value):
        pass

    def destroy(self):
        self.is_writable = False


class RollSiteWriteBatch(PairWriteBatch):
    grpc_channel_factory = GrpcChannelFactory()

    # TODO:0: check if secure channel needed
    def __init__(self, adapter: RollSiteAdapter, options: dict = None):
        if options is None:
            options = {}
        self.adapter = adapter

        self.roll_site_header: ErRollSiteHeader = adapter.roll_site_header
        self.namespace = adapter.namespace
        self.name = create_store_name(self.roll_site_header)

        self.tagged_key = ''
        self.obj_type = adapter.obj_type

        self.proxy_endpoint = adapter.proxy_endpoint
        channel = self.grpc_channel_factory.create_channel(self.proxy_endpoint)
        self.stub = proxy_pb2_grpc.DataTransferServiceStub(channel)

        static_er_conf = get_static_er_conf()
        self.__bin_packet_len = int(options.get(
                RollSiteConfKeys.EGGROLL_ROLLSITE_ADAPTER_SENDBUF_SIZE.key,
                static_er_conf.get(RollSiteConfKeys.EGGROLL_ROLLSITE_ADAPTER_SENDBUF_SIZE.key,
                                   RollSiteConfKeys.EGGROLL_ROLLSITE_ADAPTER_SENDBUF_SIZE.default_value)))
        self.total_written = 0

        self.ba = bytearray(self.__bin_packet_len)
        self.buffer = ArrayByteBuffer(self.ba)
        self.writer = PairBinWriter(pair_buffer=self.buffer)

        self.push_cnt = 0

        self.topic_src = proxy_pb2.Topic(name=self.name, partyId=self.roll_site_header._src_party_id,
                                         role=self.roll_site_header._src_role, callback=None)
        self.topic_dst = proxy_pb2.Topic(name=self.name, partyId=self.roll_site_header._dst_party_id,
                                         role=self.roll_site_header._dst_role, callback=None)

    def __repr__(self):
        return f'<ErRollSiteWriteBatch(' \
               f'adapter={self.adapter}, ' \
               f'roll_site_header={self.roll_site_header}' \
               f'namespace={self.namespace}, ' \
               f'name={self.name}, ' \
               f'obj_type={self.obj_type}, ' \
               f'proxy_endpoint={self.proxy_endpoint}) ' \
               f'at {hex(id(self))}>'

    def generate_message(self, obj, metadata):
        data = proxy_pb2.Data(value=obj)
        metadata.seq += 1
        packet = proxy_pb2.Packet(header=metadata, body=data)
        yield packet

    # TODO:0: configurable
    def push(self, obj):
        L.debug(f'pushing for task: {self.name}, partition id: {self.adapter.partition_id}, push cnt: {self.get_push_count()}')
        task_info = proxy_pb2.Task(taskId=self.name, model=proxy_pb2.Model(name=self.adapter.roll_site_header_string, dataKey=self.namespace))

        command_test = proxy_pb2.Command()

        # TODO: conf test as config and use it
        conf_test = proxy_pb2.Conf(overallTimeout=200000,
                                   completionWaitTimeout=200000,
                                   packetIntervalTimeout=200000,
                                   maxRetries=10)

        metadata = proxy_pb2.Metadata(task=task_info,
                                      src=self.topic_src,
                                      dst=self.topic_dst,
                                      command=command_test,
                                      seq=0,
                                      ack=0)

        max_retry_cnt = 100
        exception = None
        for i in range(1, max_retry_cnt + 1):
            try:
                self.stub.push(self.generate_message(obj, metadata))
                exception = None
                self.increase_push_count()
                break
            except Exception as e:
                exception = e
                L.info(f'caught exception in pushing {self.name}, partition_id: {self.adapter.partition_id}: {e}. retrying. current retry count: {i}, max_retry_cnt: {max_retry_cnt}')
                time.sleep(min(0.1 * i, 30))

        if exception:
            raise GrpcCallError("error in push", self.proxy_endpoint, exception)

    def write(self):
        bin_data = bytes(self.ba[0:self.buffer.get_offset()])
        self.push(bin_data)
        self.buffer = ArrayByteBuffer(self.ba)

    def send_end(self):
        L.info(f"send_end tagged_key:{self.tagged_key}")
        task_info = proxy_pb2.Task(taskId=self.name, model=proxy_pb2.Model(name=self.adapter.roll_site_header_string, dataKey=self.namespace))

        command_test = proxy_pb2.Command(name="set_status")
        conf_test = proxy_pb2.Conf(overallTimeout=20000,
                                   completionWaitTimeout=20000,
                                   packetIntervalTimeout=20000,
                                   maxRetries=10)

        metadata = proxy_pb2.Metadata(task=task_info,
                                      src=self.topic_src,
                                      dst=self.topic_dst,
                                      command=command_test,
                                      operator="markEnd",
                                      seq=self.get_push_count(),
                                      ack=0)

        packet = proxy_pb2.Packet(header=metadata)

        try:
            # TODO:0: retry and sleep for all grpc call in RollSite
            self.stub.unaryCall(packet)
        except Exception as e:
            raise GrpcCallError('send_end', self.proxy_endpoint, e)

    def close(self):
        bin_batch = bytes(self.ba[0:self.buffer.get_offset()])
        self.push(bin_batch)
        self.send_end()
        L.info(f'closing RollSiteWriteBatch for name: {self.name}, '
               f'total push count: {self.push_cnt}')

    def put(self, k, v):
        if self.obj_type == 'object':
            L.debug(f"set tagged_key: {k}")
            self.tagged_key = _serdes.deserialize(k)
        try:
            self.writer.write(k, v)
        except IndexError as e:
            bin_batch = bytes(self.ba[0:self.buffer.get_offset()])
            self.push(bin_batch)
            # TODO:0: replace 1024 with constant
            self.ba = bytearray(max(self.__bin_packet_len, len(k) + len(v) + 1024))
            self.buffer = ArrayByteBuffer(self.ba)
            self.writer = PairBinWriter(pair_buffer=self.buffer)
            self.writer.write(k, v)
        except Exception as e:
            L.error(f"Unexpected error: {sys.exc_info()[0]}")
            raise e

    def increase_push_count(self):
        self.push_cnt += 1

    def get_push_count(self):
        return self.push_cnt


class RollSiteIterator(PairIterator):
    def __init__(self, adapter: RollSiteAdapter):
        self.adapter = adapter
        self.it = adapter.db.iteritems()
        self.it.seek_to_first()

    def first(self):
        print("first called")
        count = 0
        self.it.seek_to_first()
        for k, v in self.it:
            count += 1
        self.it.seek_to_first()
        return (count != 0)

    def last(self):
        count = 0
        self.it.seek_to_last()
        for k, v in self.it:
            count += 1
        self.it.seek_to_last()
        return (count != 0)

    def key(self):
        return self.it.get()[0]

    def close(self):
        pass

    def __iter__(self):
        return self.it
