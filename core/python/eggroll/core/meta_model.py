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

from eggroll.core.base_model import RpcMessage
from eggroll.core.proto import meta_pb2
from eggroll.core.utils import _listify_map, _repr_list, _elements_to_proto

DEFAULT_DELIM = '/'

class ErEndpoint(RpcMessage):
  def __init__(self, host, port):
    self._host = host
    self._port = port

  def to_proto(self):
    return meta_pb2.Endpoint(host=self._host, port=self._port)

  @staticmethod
  def from_proto(pb_message):
    return ErEndpoint(host=pb_message.host, port=pb_message.port)

  def __str__(self):
    return f'{self._host}:{self._port}'

  def __repr__(self):
    return f'ErEndpoint(host={self._host}, port={self._port})'


class ErServerNode(RpcMessage):
  def __init__(self, id: str, command_endpoint: ErEndpoint, data_endpoint: ErEndpoint, tag=''):
    self._id = id
    self._command_endpoint = command_endpoint
    self._data_endpoint = data_endpoint if data_endpoint else command_endpoint
    self._tag = tag

  def to_proto(self):
    return meta_pb2.ServerNode(id=self._id,
                               commandEndpoint=self._command_endpoint.to_proto(),
                               dataEndpoint=self._data_endpoint.to_proto(),
                               tag=self._tag)

  @staticmethod
  def from_proto(pb_message):
    return ErServerNode(id=pb_message.id,
                        command_endpoint=ErEndpoint.from_proto(pb_message.commandEndpoint),
                        data_endpoint=ErEndpoint.from_proto(pb_message.dataEndpoint),
                        tag=pb_message.tag)

  def __repr__(self):
    return f'ErServerNode(id={self._id}, command_endpoint={repr(self._command_endpoint)}, data_endpoint={repr(self._data_endpoint)}, tag={self._tag})'


class ErServerCluster(RpcMessage):
  def __init__(self, id: str, nodes=list(), tag: str = ''):
    self._id = id
    self._nodes = nodes
    self._tag = tag

  def to_proto(self):
    return meta_pb2.ServerCluster(id=self._id,
                                  nodes=_elements_to_proto(self._nodes),
                                  tag=self._tag)

  @staticmethod
  def from_proto(pb_message):
    return ErServerCluster(id=pb_message.id,
                           nodes=_listify_map(ErServerNode.from_proto,
                                              pb_message.nodes),
                           tag=pb_message.tag)

  def __repr__(self):
    return f'ErServerCluster(id={self._id}, nodes=[{_repr_list(self._nodes)}], tag={self._tag})'


class ErFunctor(RpcMessage):
  def __init__(self, name='', serdes='', body=b'', conf=dict()):
    self._name = name
    self._serdes = serdes
    self._body = body
    self._conf = conf

  def to_proto(self):
    return meta_pb2.Functor(name=self._name, serdes=self._serdes, body=self._body, conf=self._conf)

  @staticmethod
  def from_proto(pb_message):
    return ErFunctor(name=pb_message.name, serdes=pb_message.serdes, body=pb_message.body, conf=pb_message.conf)

  def __repr__(self):
    return f'ErFunctor(name={self._name}, serdes={self._serdes}, body=***;{len(self._body)}, conf={self._conf})'


class ErPair(RpcMessage):
  def __init__(self, key, value):
    self._key = key
    self._value = value

  def to_proto(self):
    return meta_pb2.Pair(key=self._key, value=self._value)

  @staticmethod
  def from_proto(pb_message):
    return ErPair(key=pb_message.key, value=pb_message.value)

  def __repr__(self):
    return f'ErPair(key={self._key}, value=***;{len(self._body)}'


class ErPairBatch(RpcMessage):
  def __init__(self, pairs=list()):
    self._pairs = pairs

  def to_proto(self):
    return meta_pb2.PairBatch(pairs=_elements_to_proto(self._pairs))

  @staticmethod
  def from_proto(pb_message):
    return ErPairBatch(_listify_map(ErPair.from_proto, pb_message.pairs))

  @staticmethod
  def from_proto_string(pb_string):
    pb_message = meta_pb2.PairBatch()
    msg_len = pb_message.ParseFromString(pb_string)
    return ErPairBatch.from_proto(pb_message)

  def __repr__(self):
    return f'ErPairBatch(pairs={_repr_list(self._pairs)})'


class ErStoreLocator(RpcMessage):
  def __init__(self, store_type: str, namespace: str, name: str,
      path: str = '', partitioner: str = '', serdes: str = ''):
    self._store_type = store_type
    self._namespace = namespace
    self._name = name
    self._path = path
    self._partitioner = partitioner
    self._serdes = serdes

  def to_proto(self):
    return meta_pb2.StoreLocator(storeType=self._store_type,
                                 namespace=self._namespace,
                                 name=self._name,
                                 path=self._path,
                                 partitioner=self._partitioner,
                                 serdes=self._serdes)

  @staticmethod
  def from_proto(pb_message):
    return ErStoreLocator(store_type=pb_message.storeType,
                          namespace=pb_message.namespace,
                          name=pb_message.name,
                          path=pb_message.path,
                          partitioner=pb_message.partitioner,
                          serdes=pb_message.serdes)

  def to_path(self, delim = DEFAULT_DELIM):
    if not self._path:
      delim.join([self._store_type, self._namespace, self._name])
    return self._path

  def __repr__(self):
    return f'ErStoreLocator(store_type={self._store_type}, namespace={self._namespace}, name={self._name}, path={self._path}, partitioner={self._partitioner}, serdes={self._serdes})'


class ErPartition(RpcMessage):
  def __init__(self, id: str, store_locator: ErStoreLocator,
      node: ErServerNode):
    self._id = id
    self._store_locator = store_locator
    self._node = node

  def to_proto(self):
    return meta_pb2.Partition(id=self._id,
                              storeLocator=self._store_locator.to_proto() if self._store_locator else None,
                              node=self._node.to_proto() if self._node else None)

  @staticmethod
  def from_proto(pb_message):
    return ErPartition(id=pb_message.id,
                       store_locator=ErStoreLocator.from_proto(
                         pb_message.storeLocator),
                       node=ErServerNode.from_proto(pb_message.node))

  def to_path(self, delim=DEFAULT_DELIM):
    return DEFAULT_DELIM.join([self._store_locator.to_path(delim=delim), self._id])

  def __repr__(self):
    return f'ErPartition(id={self._id}, store_locator={repr(self._store_locator)}, node={repr(self._node)})'


class ErStore(RpcMessage):
  def __init__(self, store_locator: ErStoreLocator, partitions=list()):
    self._store_locator = store_locator
    self._partitions = partitions

  def to_proto(self):
    return meta_pb2.Store(storeLocator=self._store_locator.to_proto(),
                          partitions=_elements_to_proto(self._partitions))

  def to_path(self, delim = DEFAULT_DELIM):
    return self._store_locator.to_path(DEFAULT_DELIM)

  @staticmethod
  def from_proto(pb_message):
    print(pb_message)
    return ErStore(
      store_locator=ErStoreLocator.from_proto(pb_message.storeLocator),
      partitions=_listify_map(ErPartition.from_proto, pb_message.partitions))

  @staticmethod
  def from_proto_string(pb_string):
    pb_message = meta_pb2.Store()
    msg_len = pb_message.ParseFromString(pb_string)
    return ErStore.from_proto(pb_message)

  def __repr__(self):
    return f'ErStore(store_locator={repr(self._store_locator)}, partitions=[{_repr_list(self._partitions)}])'


class ErJob(RpcMessage):
  def __init__(self, id: str, name: str = '', inputs=list(), outputs=list(),
      functors=list()):
    self._id = id
    self._name = name
    self._inputs = inputs
    self._outputs = outputs
    self._functors = functors

  def to_proto(self):
    return meta_pb2.Job(id=self._id,
                        name=self._name,
                        inputs=_elements_to_proto(self._inputs),
                        outputs=_elements_to_proto(self._outputs),
                        functors=_elements_to_proto(self._functors))

  @staticmethod
  def from_proto(pb_message):
    return ErJob(id=pb_message.id,
                 name=pb_message.name,
                 inputs=_listify_map(ErStore.from_proto, pb_message.inputs),
                 outputs=_listify_map(ErStore.from_proto, pb_message.outputs),
                 functors=_listify_map(ErFunctor.from_proto,
                                       pb_message.functors))

  def __repr__(self):
    return f'ErJob(id={self._id}, name={self._name}, inputs=[{_repr_list(self._inputs)}], outputs=[{_repr_list(self._outputs)}], functors=[{len(self._functors)}])'


class ErTask(RpcMessage):
  def __init__(self, id: str, name='', inputs=list(), outputs=list(),
      job: ErJob = None):
    self._id = id
    self._name = name
    self._inputs = inputs
    self._outputs = outputs
    self._job = job

  def to_proto(self):
    return meta_pb2.Task(id=self._id,
                         name=self._name,
                         inputs=_elements_to_proto(self._inputs),
                         outputs=_elements_to_proto(self._outputs),
                         job=self._job.to_proto())

  @staticmethod
  def from_proto(pb_message):
    return ErTask(id=pb_message.id,
                  name=pb_message.name,
                  inputs=_listify_map(ErPartition.from_proto,
                                      pb_message.inputs),
                  outputs=_listify_map(ErPartition.from_proto,
                                       pb_message.outputs),
                  job=ErJob.from_proto(pb_message.job))

  def __repr__(self):
    return f'ErTask(id={self._id}, name={self._name}, inputs=[{_repr_list(self._inputs)}], outputs=[{_repr_list(self._outputs)}], job={self._job})'

  def get_endpoint(self):
    if not inputs or len(inputs) == 0:
      raise ValueError("Partition input is empty")

    node = inputs[0]._node

    if not node:
      raise ValueError("Head node's input partition is null")

    return node._endpoint
