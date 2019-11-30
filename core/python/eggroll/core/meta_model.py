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
from eggroll.core.utils import _map_and_listify, _repr_list, _elements_to_proto, _to_proto, _from_proto

DEFAULT_DELIM = '/'

class ErEndpoint(RpcMessage):
  def __init__(self, host, port: int):
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
  def __init__(self, id: int = -1, name: str = '', cluster_id: int = 0, endpoint: ErEndpoint = None, node_type: str = '', status: str = ''):
    self._id = id
    self._name = name
    self._cluster_id = cluster_id
    self._endpoint = endpoint
    self._node_type = node_type
    self._status = status

  def to_proto(self):
    return meta_pb2.ServerNode(id=self._id,
                               name=self._name,
                               clusterId=self._cluster_id,
                               endpoint=_to_proto(self._endpoint),
                               nodeType=self._node_type,
                               status=self._status)

  def to_proto_string(self):
    return self.to_proto().SerializeToString()

  @staticmethod
  def from_proto(pb_message):
    return ErServerNode(id=pb_message.id,
                        name=pb_message.name,
                        cluster_id=pb_message.clusterId,
                        endpoint=_from_proto(ErEndpoint.from_proto, pb_message.endpoint),
                        node_type=pb_message.nodeType,
                        status=pb_message.status)

  @staticmethod
  def from_proto_string(pb_string):
    pb_message = meta_pb2.ServerNode()
    msg_len = pb_message.ParseFromString(pb_string)
    return ErServerNode.from_proto(pb_message)

  def __repr__(self):
    return f'ErServerNode(id={repr(self._id)}, name={self._name}, cluster_id={repr(self._cluster_id)}, endpoint={repr(self._endpoint)}, node_type={self._node_type}, status={self._status})'


class ErServerCluster(RpcMessage):
  def __init__(self, id: int, name: str, server_nodes = list(), tag: str = ''):
    self._id = id
    self._name = name
    self._server_nodes = server_nodes
    self._tag = tag

  def to_proto(self):
    return meta_pb2.ServerCluster(id=self._id,
                                  name=self._name,
                                  serverNodes=_elements_to_proto(self._server_nodes),
                                  tag=self._tag)

  @staticmethod
  def from_proto(pb_message):
    return ErServerCluster(id=pb_message.id,
                           name=pb_message.name,
                           server_nodes=_map_and_listify(ErServerNode.from_proto, pb_message.serverNodes),
                           tag=pb_message.tag)

  @staticmethod
  def from_proto_string(pb_string):
    pb_message = meta_pb2.ServerCluster()
    msg_len = pb_message.ParseFromString(pb_string)
    return ErServerCluster.from_proto(pb_message)

  def __repr__(self):
    return f'ErServerCluster(id={repr(self._id)}, name={self._name}, server_nodes={_repr_list(self._server_nodes)}, tag={self._tag})'


class ErProcessor(RpcMessage):
  def __init__(self,
      id: int = -1,
      name: str = '',
      processor_type = '',
      status = '',
      command_endpoint: ErEndpoint = None,
      data_endpoint: ErEndpoint = None,
      options = {},
      tag=''):
    self._id = id
    self._name = name
    self._processor_type = processor_type
    self._status = status
    self._command_endpoint = command_endpoint
    self._data_endpoint = data_endpoint if data_endpoint else command_endpoint
    self._options = options
    self._tag = tag

  def to_proto(self):
    return meta_pb2.Processor(id=self._id,
                              name=self._name,
                              processorType=self._processor_type,
                              status=self._status,
                              commandEndpoint=self._command_endpoint.to_proto(),
                              dataEndpoint=self._data_endpoint.to_proto(),
                              options=self._options,
                              tag=self._tag)

  def to_proto_string(self):
    return self.to_proto().SerializeToString()

  @staticmethod
  def from_proto(pb_message):
    return ErProcessor(id=pb_message.id,
                       name=pb_message.name,
                       processor_type=pb_message.processorType,
                       status=pb_message.status,
                       command_endpoint=ErEndpoint.from_proto(pb_message.commandEndpoint),
                       data_endpoint=ErEndpoint.from_proto(pb_message.dataEndpoint),
                       options=pb_message.options,
                       tag=pb_message.tag)

  @staticmethod
  def from_proto_string(pb_string):
    pb_message = meta_pb2.Processor()
    msg_len = pb_message.ParseFromString(pb_string)
    return ErProcessor.from_proto(pb_message)

  def __repr__(self):
    return f'ErProcessor(id={repr(self._id)}, name={self._name}, ' \
           f'processor_type={self._processor_type}, status={self._status}, ' \
           f'command_endpoint={repr(self._command_endpoint)},' \
           f' data_endpoint={repr(self._data_endpoint)}, ' \
           f'options=[{self._options}], tag={self._tag})'


class ErProcessorBatch(RpcMessage):
  def __init__(self, id: int, name: str = '', processors=list(), tag: str = ''):
    self._id = id
    self._name = name
    self._processors = processors
    self._tag = tag

  def to_proto(self):
    return meta_pb2.ProcessorBatch(id=self._id,
                                   name=self._name,
                                   processors=_elements_to_proto(self._processors),
                                   tag=self._tag)

  @staticmethod
  def from_proto(pb_message):
    return ErProcessorBatch(id=pb_message.id,
                            name=pb_message.name,
                            processors=_map_and_listify(ErProcessor.from_proto, pb_message.processors),
                            tag=pb_message.tag)

  @staticmethod
  def from_proto_string(pb_string):
    pb_message = meta_pb2.ProcessorBatch()
    msg_len = pb_message.ParseFromString(pb_string)
    return ErProcessorBatch.from_proto(pb_message)

  def __repr__(self):
    return f'ErProcessorBatch(id={repr(self._id)}, name={self._name}, processors=[{_repr_list(self._processors)}], tag={self._tag})'


class ErFunctor(RpcMessage):
  def __init__(self, name='', serdes='', body=b'', options=dict()):
    self._name = name
    self._serdes = serdes
    self._body = body
    self._options = options

  def to_proto(self):
    return meta_pb2.Functor(name=self._name, serdes=self._serdes, body=self._body, options=self._options)

  @staticmethod
  def from_proto(pb_message):
    return ErFunctor(name=pb_message.name, serdes=pb_message.serdes, body=pb_message.body, options=pb_message.options)

  def __repr__(self):
    return f'ErFunctor(name={self._name}, serdes={self._serdes}, body=***;{len(self._body)}, options=[{self._options}])'


class ErPair(RpcMessage):
  def __init__(self, key, value):
    self._key = key
    self._value = value

  def to_proto(self):
    return meta_pb2.Pair(key=self._key, value=self._value)

  def to_proto_string(self):
    return self.to_proto().SerializeToString()

  @staticmethod
  def from_proto(pb_message):
    return ErPair(key=pb_message.key, value=pb_message.value)

  @staticmethod
  def from_proto_string(pb_string):
    pb_message = meta_pb2.Pair()
    msg_len = pb_message.ParseFromString(pb_string)
    return ErPair.from_proto(pb_message)

  def __repr__(self):
    return f'ErPair(key={self._key}, value={self._value})'


class ErPairBatch(RpcMessage):
  def __init__(self, pairs=list()):
    self._pairs = pairs

  def to_proto(self):
    return meta_pb2.PairBatch(pairs=_elements_to_proto(self._pairs))

  @staticmethod
  def from_proto(pb_message):
    return ErPairBatch(_map_and_listify(ErPair.from_proto, pb_message.pairs))

  @staticmethod
  def from_proto_string(pb_string):
    pb_message = meta_pb2.PairBatch()
    msg_len = pb_message.ParseFromString(pb_string)
    return ErPairBatch.from_proto(pb_message)

  def __repr__(self):
    return f'ErPairBatch(pairs={_repr_list(self._pairs)})'


class ErStoreLocator(RpcMessage):
  def __init__(self, store_type: str, namespace: str, name: str,
      path: str = '', total_partitions = 0, partitioner: str = '', serdes: str = ''):
    self._store_type = store_type
    self._namespace = namespace
    self._name = name
    self._path = path
    self._total_partitions = total_partitions
    self._partitioner = partitioner
    self._serdes = serdes

  def to_proto(self):
    return meta_pb2.StoreLocator(storeType=self._store_type,
                                 namespace=self._namespace,
                                 name=self._name,
                                 path=self._path,
                                 totalPartitions=self._total_partitions,
                                 partitioner=self._partitioner,
                                 serdes=self._serdes)

  def to_proto_string(self):
    return self.to_proto().SerializeToString()

  @staticmethod
  def from_proto(pb_message):
    return ErStoreLocator(store_type=pb_message.storeType,
                          namespace=pb_message.namespace,
                          name=pb_message.name,
                          path=pb_message.path,
                          total_partitions=pb_message.totalPartitions,
                          partitioner=pb_message.partitioner,
                          serdes=pb_message.serdes)

  def to_path(self, delim = DEFAULT_DELIM):
    if not self._path:
      delim.join([self._store_type, self._namespace, self._name])
    return self._path

  def __repr__(self):
    return f'ErStoreLocator(store_type={self._store_type}, namespace={self._namespace}, name={self._name}, path={self._path}, total_partitions={self._total_partitions}, partitioner={self._partitioner}, serdes={self._serdes})'


class ErPartition(RpcMessage):
  def __init__(self, id: int, store_locator: ErStoreLocator,
      processor: ErProcessor):
    self._id = id
    self._store_locator = store_locator
    self._processor = processor

  def to_proto(self):
    return meta_pb2.Partition(id=self._id,
                              storeLocator=self._store_locator.to_proto() if self._store_locator else None,
                              processor=self._processor.to_proto() if self._processor else None)

  @staticmethod
  def from_proto(pb_message):
    return ErPartition(id=pb_message.id,
                       store_locator=ErStoreLocator.from_proto(
                         pb_message.storeLocator),
                       processor=ErProcessor.from_proto(pb_message.processor))

  def to_path(self, delim=DEFAULT_DELIM):
    return DEFAULT_DELIM.join([self._store_locator.to_path(delim=delim), self._id])

  def __repr__(self):
    return f'ErPartition(id={repr(self._id)}, store_locator={repr(self._store_locator)}, processor={repr(self._processor)})'


class ErStore(RpcMessage):
  def __init__(self, store_locator: ErStoreLocator, partitions=list()):
    self._store_locator = store_locator
    self._partitions = partitions

  def to_proto(self):
    return meta_pb2.Store(storeLocator=self._store_locator.to_proto(),
                          partitions=_elements_to_proto(self._partitions))

  def to_proto_string(self):
    return self.to_proto().SerializeToString()

  def to_path(self, delim = DEFAULT_DELIM):
    return self._store_locator.to_path(DEFAULT_DELIM)

  @staticmethod
  def from_proto(pb_message):
    return ErStore(
      store_locator=ErStoreLocator.from_proto(pb_message.storeLocator),
      partitions=_map_and_listify(ErPartition.from_proto, pb_message.partitions))

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

  def to_proto_string(self):
    return self.to_proto().SerializeToString()

  @staticmethod
  def from_proto(pb_message):
    return ErJob(id=pb_message.id,
                 name=pb_message.name,
                 inputs=_map_and_listify(ErStore.from_proto, pb_message.inputs),
                 outputs=_map_and_listify(ErStore.from_proto, pb_message.outputs),
                 functors=_map_and_listify(ErFunctor.from_proto,
                                       pb_message.functors))

  @staticmethod
  def from_proto_string(pb_string):
    pb_message = meta_pb2.Job()
    msg_len = pb_message.ParseFromString(pb_string)
    return ErJob.from_proto(pb_message)

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

  def to_proto_string(self):
    return self.to_proto().SerializeToString()

  @staticmethod
  def from_proto(pb_message):
    return ErTask(id=pb_message.id,
                  name=pb_message.name,
                  inputs=_map_and_listify(ErPartition.from_proto,
                                      pb_message.inputs),
                  outputs=_map_and_listify(ErPartition.from_proto,
                                       pb_message.outputs),
                  job=ErJob.from_proto(pb_message.job))

  @staticmethod
  def from_proto_string(pb_string):
    pb_message = meta_pb2.Task()
    msg_len = pb_message.ParseFromString(pb_string)
    return ErTask.from_proto(pb_message)

  def __repr__(self):
    return f'ErTask(id={self._id}, name={self._name}, inputs=[{_repr_list(self._inputs)}], outputs=[{_repr_list(self._outputs)}], job={self._job})'

  def get_endpoint(self):
    if not self._inputs or len(self._inputs) == 0:
      raise ValueError("Partition input is empty")

    node = self._inputs[0]._node

    if not node:
      raise ValueError("Head node's input partition is null")

    return node._endpoint

class ErSessionMeta(RpcMessage):
  def __init__(self, id = '', name: str = '', status: str = '', options = {}, tag: str = ''):
    self._id = id
    self._name = name
    self._status = status
    self._options = options
    self._tag = tag

  def to_proto(self):
    return meta_pb2.SessionMeta(id=self._id,
                                name=self._name,
                                status=self._status,
                                options=self._options,
                                tag=self._tag)

  def to_proto_string(self):
    return self.to_proto().SerializeToString()

  @staticmethod
  def from_proto(pb_message):
    return ErSessionMeta(id=pb_message.id,
                         name=pb_message.name,
                         status=pb_message.status,
                         options=pb_message.options,
                         tag=pb_message.tag)

  @staticmethod
  def from_proto_string(pb_string):
    pb_message = meta_pb2.Task()
    msg_len = pb_message.ParseFromString(pb_string)
    return ErSessionMeta.from_proto(pb_message)

  def __repr__(self):
    return f'ErSessionMeta(id={self._id}, name={self._name}, status={self._status}, options=[{self._options}], tag={self._tag})'
