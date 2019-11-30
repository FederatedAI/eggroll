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
import os
import pickle

#sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from eggroll.roll_site.api import rollsite
from eggroll.core.io.kv_adapter import SortedKvWriteBatch
from eggroll.core.io.kv_adapter import SortedKvIterator
from eggroll.core.io.kv_adapter import SortedKvAdapter
from eggroll.core.proto import meta_pb2
from eggroll.core.utils import _elements_to_proto

class RollsiteWriteBatch(SortedKvWriteBatch):

  def __init__(self, adapter):
    self.adapter = adapter
    #self.kv_stub = self.adapter.kv_stub
    self.cache = []
    self.name = adapter._name
    self.tag = adapter._tag

  def write(self):
    if self.cache:
      #self.kv_stub.putAll(iter(self.cache), metadata=self.adapter.get_stream_meta())
      #rollsite.push(content, "model_A", tag="{}".format(_tag))
      print(self.cache)
      rollsite.push(str(pickle.dumps(self.cache)), self.name, self.tag)
      self.cache.clear()

  def close(self):
    # write last
    self.write()

  def put(self, k, v):
    self.cache.append((k, v))
    if len(self.cache) > 100000:
      self.write()


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
  # options:
  #   host
  #   port
  #   store_type
  #   name
  #   namespace
  #   fragment

  def get_stream_meta(self):
    return ('store_type', self.options["store_type"]), \
           ('table_name', self.options["name"]), \
           ('name_space', self.options["namespace"]), \
           ('fragment', self.options["fragment"])

  def __init__(self, options):
    super().__init__(options)
    self.options = options

    rollsite.init("atest",
                  "roll_site/test/role_conf.json",
                  "roll_site/test/server_conf.json",
                  "roll_site/test/transfer_conf.json")

    self._namespace = ''
    self._name = options["name"]
    self._tag = options["tag"]
    self._store_type = 'roll_site'
    self._path = ''
    self._partitioner = ''
    self._serdes = ''
    self._partitions = []
    self._store_locator = meta_pb2.StoreLocator(storeType=self._store_type,
                                                namespace=self._namespace,
                                                name=self._name,
                                                path=self._path,
                                                partitioner=self._partitioner,
                                                serdes=self._serdes)
    '''
    #这个是egg 中数据库的host和port
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
    return RollsiteWriteBatch(self)

  def get(self, key):
    #item = self.kv_stub.get(kv_pb2.Operand(key=key))
    #return item.value
    pass

  def put(self, key, value):
    #item = kv_pb2.Operand(key=key, value=value)
    #self.kv_stub.put(item)
    pass