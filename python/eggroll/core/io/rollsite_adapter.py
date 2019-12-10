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
import _io
import sys
import os
import pickle
import grpc

from eggroll.core.io.kv_adapter import SortedKvWriteBatch
from eggroll.core.io.kv_adapter import SortedKvIterator
from eggroll.core.io.kv_adapter import SortedKvAdapter

from eggroll.core.proto import meta_pb2
from eggroll.core.utils import _elements_to_proto

from eggroll.core.io.format import BinBatchWriter
from eggroll.core.proto import proxy_pb2, proxy_pb2_grpc

class RollsiteWriteBatch(SortedKvWriteBatch):
  def __init__(self, adapter):
    self.adapter = adapter
    self.cache = []
    self.name = adapter._name
    self.tag = adapter._tag
    self.__bin_packet_len = 1 << 20
    self.buffer = bytearray(self.__bin_packet_len)
    self.writer = BinBatchWriter({'buffer': self.buffer})
    self.cur_offset = self.writer.get_offset
    self.total_written = 0
    self.dst_host = adapter._dst_host
    self.dst_port = adapter._dst_port

  def generate_message(self, obj, metadata):
    data = proxy_pb2.Data(key="hello", value=obj)
    metadata.seq += 1
    packet = proxy_pb2.Packet(header=metadata, body=data)
    yield packet

  def push(self, obj, name: str):
    args = name.split("-", 9)
    print(args)
    tag = args[2]
    src_role = args[3]
    src_party_id = args[4]
    dst_role = args[5]
    dst_party_id = args[6]
    host = args[7]
    port = int(args[8])

    channel = grpc.insecure_channel(
        target="{}:{}".format(host, port),
        options=[('grpc.max_send_message_length', -1), ('grpc.max_receive_message_length', -1)])
    stub = proxy_pb2_grpc.DataTransferServiceStub(channel)

    task_info = proxy_pb2.Task(taskId="testTaskId", model=proxy_pb2.Model(name=name, dataKey="testKey"))
    topic_src = proxy_pb2.Topic(name=name, partyId="{}".format(src_party_id),
                                role=src_role, callback=None)
    topic_dst = proxy_pb2.Topic(name=name, partyId="{}".format(dst_party_id),
                                role=dst_role, callback=None)
    command_test = proxy_pb2.Command()
    conf_test = proxy_pb2.Conf(overallTimeout=2000,
                               completionWaitTimeout=2000,
                               packetIntervalTimeout=2000,
                               maxRetries=10)

    metadata = proxy_pb2.Metadata(task=task_info,
                                  src=topic_src,
                                  dst=topic_dst,
                                  command=command_test,
                                  seq=0, ack=0,
                                  conf=conf_test)

    stub.push(self.generate_message(obj, metadata))

  def write(self, bin_data):
    print(bin_data)
    print(self.name)
    self.push(bin_data, self.name)

  def send_end(self):
    channel = grpc.insecure_channel(
        target="{}:{}".format(host, port),
        options=[('grpc.max_send_message_length', -1), ('grpc.max_receive_message_length', -1)])
    stub = proxy_pb2_grpc.DataTransferServiceStub(channel)

    task_info = proxy_pb2.Task(taskId="testTaskId", model=proxy_pb2.Model(name="taskName", dataKey="testKey"))
    topic_src = proxy_pb2.Topic(name="test", partyId="{}".format(party_id),
                                role=src_role, callback=None)
    topic_dst = proxy_pb2.Topic(name="test", partyId=self.party_id,
                                role=self.role, callback=None)
    command_test = proxy_pb2.Command(name="set_status")
    conf_test = proxy_pb2.Conf(overallTimeout=1000,
                               completionWaitTimeout=1000,
                               packetIntervalTimeout=1000,
                               maxRetries=10)

    metadata = proxy_pb2.Metadata(task=task_info,
                                  src=topic_src,
                                  dst=topic_dst,
                                  command=command_test,
                                  seq=0, ack=0,
                                  conf=conf_test)
    data = proxy_pb2.Data(key="hello", value=obj.encode())
    packet = proxy_pb2.Packet(header=metadata, body=data)

    stub.unaryCall(packet);

  def close(self):
    # write last
    writer = self.writer
    self.cur_offset = writer.get_offset()
    bin_data = writer.get_batch(end=self.cur_offset)
    self.write(bin_data)
    self.send_end()

  def put(self, k, v):
    writer = self.writer
    try:
      writer.write_bytes(k, include_size=True)
      writer.write_bytes(v, include_size=True)
      self.total_written += 1
      if self.tag == '1-0':
        print(f'{self.tag} written {k_bytes}, total_written {self.total_written}, is closable {broker.is_closable()}')
    except IndexError as e:
      print('caught index error')
      bin_data = writer.get_batch(end=self.cur_offset)

      self.write(bin_data)

      self.writer = BinBatchWriter({'buffer': self.buffer})
      writer = self.writer
      writer.write_bytes(k, include_size=True)
      writer.write_bytes(v, include_size=True)
    except:
      print("Unexpected error:", sys.exc_info()[0])
      raise


class RollsiteIterator(SortedKvIterator):
  def __init__(self, adapter):
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

class RollsiteAdapter(SortedKvAdapter):
  def get_stream_meta(self):
    return ('store_type', self.options["store_type"]), \
           ('table_name', self.options["name"]), \
           ('name_space', self.options["namespace"]), \
           ('fragment', self.options["fragment"])

  def __init__(self, options):
    super().__init__(options)
    self.options = options

    self._namespace = ''
    self._name = options["name"]
    print(self._name)
    self._tag = 'tag'
    self._store_type = 'roll_site'
    self._path = ''
    self._partitioner = ''
    self._serdes = ''
    self._partitions = []
    self._dst_host = 'localhost'
    self._dst_port = '9394'
    self._store_locator = meta_pb2.StoreLocator(storeType=self._store_type,
                                                namespace=self._namespace,
                                                name=self._name,
                                                path=self._path,
                                                partitioner=self._partitioner,
                                                serdes=self._serdes)

    '''
    host = options["host"]
    port = options["port"]
    self.channel = grpc.insecure_channel(target="{}:{}".format(host, port),
                                         options=[('grpc.max_send_message_length', -1),
                                                  ('grpc.max_receive_message_length', -1)])
    self.kv_stub = kv_pb2_grpc.KVServiceStub(self.channel)
    '''

  def to_proto(self):
    return meta_pb2.Store(storeLocator=self._store_locator,
                          partitions=_elements_to_proto(self._partitions))

  def close(self):
    #self.channel.close()
    pass

  def iteritems(self):
    return RollsiteIterator(self)

  def new_batch(self):
    print("RollsiteWriteBatch")
    return RollsiteWriteBatch(self)

  def get(self, key):
    #item = self.kv_stub.get(kv_pb2.Operand(key=key))
    #return item.value
    pass

  def put(self, key, value):
    #item = kv_pb2.Operand(key=key, value=value)
    #self.kv_stub.put(item)
    pass