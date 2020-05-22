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

from copy import deepcopy
from threading import Lock

from eggroll.core.base_model import RpcMessage
from eggroll.core.proto import meta_pb2
from eggroll.core.utils import _map_and_listify, _repr_list, _repr_bytes, \
    _elements_to_proto, _to_proto, _from_proto, _stringify_dict
from eggroll.core.utils import time_now_ns

DEFAULT_PATH_DELIM = '/'
DEFAULT_FORK_DELIM = '_'


class ErEndpoint(RpcMessage):
    def __init__(self, host, port: int):
        self._host = host
        self._port = port

    def is_valid(self):
        return self._host and self._port > 0

    def to_proto(self):
        return meta_pb2.Endpoint(host=self._host, port=self._port)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @classmethod
    def from_proto(cls, pb_message):
        return ErEndpoint(host=pb_message.host, port=pb_message.port)

    @classmethod
    def from_proto_string(cls, pb_string):
        pb_message = meta_pb2.Endpoint()
        msg_len = pb_message.ParseFromString(pb_string)
        return cls.from_proto(pb_message)

    def __str__(self):
        return f'{self._host}:{self._port}'

    def __repr__(self):
        return f'<ErEndpoint(host={self._host}, port={self._port}) at {hex(id(self))}>'


class ErServerNode(RpcMessage):
    def __init__(self,
            id: int = -1,
            name: str = '',
            cluster_id: int = 0,
            endpoint: ErEndpoint = None,
            node_type: str = '',
            status: str = ''):
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
        return f'<ErServerNode(' \
               f'id={repr(self._id)}, ' \
               f'name={self._name}, ' \
               f'cluster_id={repr(self._cluster_id)}, ' \
               f'endpoint={repr(self._endpoint)}, ' \
               f'node_type={self._node_type}, ' \
               f'status={self._status}) ' \
               f'at {hex(id(self))}>'


class ErServerCluster(RpcMessage):
    def __init__(self, id: int, name: str, server_nodes: list = None, tag: str = ''):
        if server_nodes is None:
            server_nodes = []
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
        return f'<ErServerCluster(' \
               f'id={repr(self._id)}, ' \
               f'name={self._name}, ' \
               f'server_nodes={_repr_list(self._server_nodes)}, ' \
               f'tag={self._tag}) ' \
               f'at {hex(id(self))}>'


class ErProcessor(RpcMessage):
    def __init__(self,
            id=-1,
            server_node_id: int = -1,
            name: str = '',
            processor_type='',
            status='',
            command_endpoint: ErEndpoint = None,
            transfer_endpoint: ErEndpoint = None,
            pid: int = -1,
            options: dict = None,
            tag=''):
        if options is None:
            options = {}
        self._id = id
        self._server_node_id = server_node_id
        self._name = name
        self._processor_type = processor_type
        self._status = status
        self._command_endpoint = command_endpoint
        self._transfer_endpoint = transfer_endpoint if transfer_endpoint else command_endpoint
        self._pid = pid
        self._options = options
        self._tag = tag

    def is_valid(self):
        return self._command_endpoint.is_valid() \
               and (self._transfer_endpoint and self._transfer_endpoint.is_valid()) \
               and self._server_node_id > 0

    def to_proto(self):
        return meta_pb2.Processor(id=self._id,
                                  serverNodeId=self._server_node_id,
                                  name=self._name,
                                  processorType=self._processor_type,
                                  status=self._status,
                                  commandEndpoint=self._command_endpoint.to_proto(),
                                  transferEndpoint=self._transfer_endpoint.to_proto(),
                                  pid=self._pid,
                                  options=_stringify_dict(self._options),
                                  tag=self._tag)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ErProcessor(id=pb_message.id,
                           server_node_id=pb_message.serverNodeId,
                           name=pb_message.name,
                           processor_type=pb_message.processorType,
                           status=pb_message.status,
                           command_endpoint=ErEndpoint.from_proto(pb_message.commandEndpoint),
                           transfer_endpoint=ErEndpoint.from_proto(pb_message.transferEndpoint),
                           pid=pb_message.pid,
                           options=dict(pb_message.options),
                           tag=pb_message.tag)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = meta_pb2.Processor()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErProcessor.from_proto(pb_message)

    def __repr__(self):
        return f'<ErProcessor(id={repr(self._id)}, ' \
               f'server_node_id={self._server_node_id}, ' \
               f'name={self._name}, ' \
               f'processor_type={self._processor_type}, ' \
               f'status={self._status}, ' \
               f'command_endpoint={repr(self._command_endpoint)}, ' \
               f'transfer_endpoint={repr(self._transfer_endpoint)}, ' \
               f'pid={self._pid}, ' \
               f'options=[{repr(self._options)}], ' \
               f'tag={self._tag}) ' \
               f'at {hex(id(self))}>'


class ErProcessorBatch(RpcMessage):
    def __init__(self, id=-1, name: str = '', processors: list = None, tag: str = ''):
        if processors is None:
            processors = []
        self._id = id
        self._name = name
        self._processors = processors
        self._tag = tag

    def to_proto(self):
        return meta_pb2.ProcessorBatch(id=self._id,
                                       name=self._name,
                                       processors=_elements_to_proto(self._processors),
                                       tag=self._tag)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

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
        return f'<ErProcessorBatch(' \
               f'id={repr(self._id)}, ' \
               f'name={self._name}, ' \
               f'processors=[{_repr_list(self._processors)}], ' \
               f'tag={self._tag}) ' \
               f'at {hex(id(self))}>'


class ErFunctor(RpcMessage):
    def __init__(self, name='', serdes='', body=b'', options: dict = None):
        if options is None:
            options = {}
        self._name = name
        self._serdes = serdes
        self._body = body
        self._options = options

    def to_proto(self):
        return meta_pb2.Functor(name=self._name, serdes=self._serdes, body=self._body, options=_stringify_dict(self._options))

    def to_proto_string(self):
        return self.to_proto().SerializeT

    @staticmethod
    def from_proto(pb_message):
        return ErFunctor(name=pb_message.name, serdes=pb_message.serdes, body=pb_message.body, options=dict(pb_message.options))

    def __repr__(self):
        return f'<ErFunctor(' \
               f'name={repr(self._name)}, ' \
               f'serdes={repr(self._serdes)}, ' \
               f'body={_repr_bytes(self._body)}, ' \
               f'options=[{repr(self._options)}]) ' \
               f'at {hex(id(self))}>'


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
        return f'<ErPair(' \
               f'key={_repr_bytes(self._key)}, ' \
               f'value={_repr_bytes(self._value)}) ' \
               f'at {hex(id(self))}>'


class ErPairBatch(RpcMessage):
    def __init__(self, pairs: list = None):
        if pairs is None:
            pairs = []
        self._pairs = pairs

    def to_proto(self):
        return meta_pb2.PairBatch(pairs=_elements_to_proto(self._pairs))

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ErPairBatch(_map_and_listify(ErPair.from_proto, pb_message.pairs))

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = meta_pb2.PairBatch()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErPairBatch.from_proto(pb_message)

    def __repr__(self):
        return f'<ErPairBatch(pairs={_repr_list(self._pairs)}) ' \
               f'at {hex(id(self))}>'


class ErStoreLocator(RpcMessage):
    seq = 0
    seq_lock = Lock()
    def __init__(self, id=-1, store_type: str = '', namespace: str = '', name: str = '',
            path: str = '', total_partitions=0, partitioner: str = '', serdes: str = ''):
        self._id = id
        self._store_type = store_type
        self._namespace = namespace
        self._name = name
        self._path = path
        self._total_partitions = total_partitions
        self._partitioner = partitioner
        self._serdes = serdes

    def to_proto(self):
        return meta_pb2.StoreLocator(id=self._id,
                                     storeType=self._store_type,
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
        return ErStoreLocator(id=pb_message.id,
                              store_type=pb_message.storeType,
                              namespace=pb_message.namespace,
                              name=pb_message.name,
                              path=pb_message.path,
                              total_partitions=pb_message.totalPartitions,
                              partitioner=pb_message.partitioner,
                              serdes=pb_message.serdes)

    def to_path(self, delim=DEFAULT_PATH_DELIM):
        if not self._path:
            delim.join([self._store_type, self._namespace, self._name])
        return self._path

    def fork(self, postfix='', delim=DEFAULT_FORK_DELIM):
        duplicate = deepcopy(self)
        prefix = duplicate._name[:duplicate._name.rfind(delim)]
        with self.seq_lock:
            self.seq += 1
            final_postfix = postfix if postfix else f'{time_now_ns()}.{self.seq}'

        duplicate._name = f'{prefix}{delim}{final_postfix}'
        return duplicate

    def __repr__(self):
        return f'<ErStoreLocator(id={self._id}, store_type={self._store_type}, namespace={self._namespace}, name={self._name}, path={self._path}, total_partitions={self._total_partitions}, partitioner={self._partitioner}, serdes={self._serdes}) at {hex(id(self))}>'


class ErPartition(RpcMessage):
    def __init__(self, id: int, store_locator: ErStoreLocator,
            processor: ErProcessor=None, rank_in_node=-1):
        self._id = id
        self._store_locator = store_locator
        self._processor = processor
        self._rank_in_node = rank_in_node

    def to_proto(self):
        return meta_pb2.Partition(id=self._id,
                                  storeLocator=self._store_locator.to_proto() if self._store_locator else None,
                                  processor=self._processor.to_proto() if self._processor else None,
                                  rankInNode=self._rank_in_node)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ErPartition(id=pb_message.id,
                           store_locator=ErStoreLocator.from_proto(
                                   pb_message.storeLocator),
                           processor=ErProcessor.from_proto(pb_message.processor),
                           rank_in_node=pb_message.rankInNode)

    def to_path(self, delim=DEFAULT_PATH_DELIM):
        return DEFAULT_PATH_DELIM.join([self._store_locator.to_path(delim=delim), self._id])

    def __repr__(self):
        return f'<ErPartition(' \
               f'id={repr(self._id)}, ' \
               f'store_locator={repr(self._store_locator)}, ' \
               f'processor={repr(self._processor)}, ' \
               f'rank_in_node={repr(self._rank_in_node)}) ' \
               f'at {hex(id(self))}>'


class ErStore(RpcMessage):
    def __init__(self, store_locator: ErStoreLocator, partitions: list = None, options: dict = None):
        if partitions is None:
            partitions = []
        if options is None:
            options = {}
        self._store_locator = store_locator
        self._partitions = partitions
        self._options = options

    def to_proto(self):
        return meta_pb2.Store(storeLocator=self._store_locator.to_proto(),
                              partitions=_elements_to_proto(self._partitions),
                              options=_stringify_dict(self._options))

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    def to_path(self, delim=DEFAULT_PATH_DELIM):
        return self._store_locator.to_path(delim)

    @staticmethod
    def from_proto(pb_message):
        return ErStore(
                store_locator=ErStoreLocator.from_proto(pb_message.storeLocator),
                partitions=_map_and_listify(ErPartition.from_proto, pb_message.partitions),
                options=dict(pb_message.options))

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = meta_pb2.Store()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErStore.from_proto(pb_message)

    def fork(self, postfix='', delim=DEFAULT_FORK_DELIM):
        final_store_locator = self._store_locator.fork(postfix, delim)
        final_partitions = map(
                lambda p: ErPartition(id=-1,
                                      store_locator=final_store_locator,
                                      processor=p._processor),
                self._partitions)
        return ErStore(store_locator=final_store_locator,
                       partitions=list(final_partitions),
                       options=self._options)

    def __str__(self):
        return f'<ErStore(' \
               f'store_locator={repr(self._store_locator)}, ' \
               f'partitions=[***, len={len(self._partitions)}], ' \
               f'options=[{repr(self._options)}]) ' \
               f'at {hex(id(self))}>'

    def __repr__(self):
        return self.__str__()
        # return f'<ErStore(' \
        #        f'store_locator={repr(self._store_locator)}, ' \
        #        f'partitions=[{_repr_list(self._partitions)}], ' \
        #        f'options=[{repr(self._options)}]) ' \
        #        f'at {hex(id(self))}>'


class ErStoreList(RpcMessage):
    def __init__(self, stores=list()):
        self._stores = stores

    def to_proto(self):
        return meta_pb2.StoreList(stores=_elements_to_proto(self._stores))

    @staticmethod
    def from_proto(pb_message):
        return ErStoreList(_map_and_listify(ErStore.from_proto, pb_message.stores))

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = meta_pb2.StoreList()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErStoreList.from_proto(pb_message)

    def __repr__(self):
        return f'ErStoreList(stores={_repr_list(self._stores)})'

class ErJob(RpcMessage):
    def __init__(self,
            id: str,
            name: str = '',
            inputs: list = None,
            outputs: list = None,
            functors: list = None,
            options: dict = None):
        if inputs is None:
            inputs = []
        if outputs is None:
            outputs = []
        if functors is None:
            functors = []
        if options is None:
            options = {}

        self._id = id
        self._name = name
        self._inputs = inputs
        self._outputs = outputs
        self._functors = functors
        self._options = options

    def to_proto(self):
        return meta_pb2.Job(id=self._id,
                            name=self._name,
                            inputs=_elements_to_proto(self._inputs),
                            outputs=_elements_to_proto(self._outputs),
                            functors=_elements_to_proto(self._functors),
                            options=_stringify_dict(self._options))

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ErJob(id=pb_message.id,
                     name=pb_message.name,
                     inputs=_map_and_listify(ErStore.from_proto, pb_message.inputs),
                     outputs=_map_and_listify(ErStore.from_proto, pb_message.outputs),
                     functors=_map_and_listify(ErFunctor.from_proto,
                                               pb_message.functors),
                     options=dict(pb_message.options))

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = meta_pb2.Job()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErJob.from_proto(pb_message)

    def __repr__(self):
        return f'<ErJob(' \
               f'id={self._id}, ' \
               f'name={self._name}, ' \
               f'inputs=[{_repr_list(self._inputs)}], ' \
               f'outputs=[{_repr_list(self._outputs)}], ' \
               f'functors=[{len(self._functors)}], ' \
               f'options={repr(self._options)}) ' \
               f'at {hex(id(self))}>'


class ErTask(RpcMessage):
    def __init__(self,
            id: str,
            name='',
            inputs: list = None,
            outputs: list = None,
            job: ErJob = None):
        if inputs is None:
            inputs = []
        if outputs is None:
            outputs = []
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
                             job=_to_proto(self._job))

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
        return f'<ErTask(' \
               f'id={self._id}, ' \
               f'name={self._name}, ' \
               f'inputs=[{_repr_list(self._inputs)}], ' \
               f'outputs=[{_repr_list(self._outputs)}], ' \
               f'job={repr(self._job)}) ' \
               f'at {hex(id(self))}>'

    def get_endpoint(self):
        if not self._inputs or len(self._inputs) == 0:
            raise ValueError("Partition input is empty")

        node = self._inputs[0]._node

        if not node:
            raise ValueError("Head node's input partition is null")

        return node._endpoint


class ErSessionMeta(RpcMessage):
    def __init__(self,
            id='',
            name: str = '',
            status: str = '',
            tag: str = '',
            processors: list = None,
            options: dict = None):
        if processors is None:
            processors = []
        if options is None:
            options = {}
        self._id = id
        self._name = name
        self._status = status
        self._tag = tag
        self._processors = processors
        self._options = options

    def is_processors_valid(self):
        for p in self._processors:
            if not p.is_valid():
                return False
        return True

    def to_proto(self):
        return meta_pb2.SessionMeta(id=self._id,
                                    name=self._name,
                                    status=self._status,
                                    tag=self._tag,
                                    processors=_elements_to_proto(self._processors),
                                    options=_stringify_dict(self._options))

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ErSessionMeta(id=pb_message.id,
                             name=pb_message.name,
                             status=pb_message.status,
                             tag=pb_message.tag,
                             processors=_map_and_listify(ErProcessor.from_proto,
                                                         pb_message.processors),
                             options=dict(pb_message.options))

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = meta_pb2.SessionMeta()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErSessionMeta.from_proto(pb_message)

    def __str__(self):
        return f'<ErSessionMeta(' \
               f'id={self._id}, ' \
               f'name={self._name}, ' \
               f'status={self._status}, ' \
               f'tag={self._tag}, ' \
               f'processors=[***, len={len(self._processors)}], ' \
               f'options=[{repr(self._options)}]) ' \
               f'at {hex(id(self))}>'

    def __repr__(self):
        return self.__str__()
        # return f'<ErSessionMeta(' \
        #        f'id={self._id}, ' \
        #        f'name={self._name}, ' \
        #        f'status={self._status}, ' \
        #        f'tag={self._tag}, ' \
        #        f'processors=[{_repr_list(self._processors)}], ' \
        #        f'options=[{repr(self._options)}]) ' \
        #        f'at {hex(id(self))}>'
