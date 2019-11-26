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

from eggroll.core.command.command_model import ErCommandRequest, ErCommandResponse, CommandURI
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor, ErTask, ErEndpoint
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.core.command.command_client import CommandClient
from eggroll.cluster_manager.cluster_manager_client import ClusterManagerClient
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.constants import StoreTypes, SerdesTypes, PartitionerTypes


class RollPair(object):
  __uri_prefix = 'v1/roll-pair'
  MAP = 'map'
  MAP_VALUES = 'mapValues'
  REDUCE = 'reduce'
  JOIN = 'join'
  AGGREGATE = 'aggregate'
  RUNJOB = 'runJob'

  def __init__(self, er_store: ErStore, options = {'cluster_manager_host': 'localhost', 'cluster_manager_port': 4670}):
    _grpc_channel_factory = GrpcChannelFactory()

    self.__roll_pair_service_endpoint = ErEndpoint(host = options['roll_pair_service_host'], port = options['roll_pair_service_port'])
    self.__cluster_manager_channel = _grpc_channel_factory.create_channel(ErEndpoint(options['cluster_manager_host'], options['cluster_manager_port']))
    self.__roll_pair_service_channel = _grpc_channel_factory.create_channel(self.__roll_pair_service_endpoint)

    self.__command_serdes = options.get('serdes', SerdesTypes.PROTOBUF)
    self.__roll_pair_command_client = CommandClient()

    self.__roll_pair_service_stub = command_pb2_grpc.CommandServiceStub(self.__roll_pair_service_channel)
    self.__cluster_manager_client = ClusterManagerClient(options)
    self._parent_opts = options

    # todo: integrate with session mechanism
    self.__seq = 1
    self.__session_id = '1'

    self.land(er_store, options)

  def __repr__(self):
    return f'python RollPair(_store={self.__store})'

  def land(self, er_store: ErStore, options = {}):
    if er_store:
      final_store = er_store
    else:
      store_type = options.get('store_type', StoreTypes.ROLLPAIR_LEVELDB)
      namespace = options.get('namespace', '')
      name = options.get('name', '')
      total_partiitons = options.get('total_partitions', 0)
      partitioner = options.get('partitioner', PartitionerTypes.BYTESTRING_HASH)
      serdes = options.get('serdes', SerdesTypes.PICKLE)

      final_store = ErStore(
          store_locator = ErStoreLocator(
              store_type = store_type,
              namespace = namespace,
              name = name,
              total_partiitons = total_partiitons,
              partitioner = partitioner,
              serdes = serdes))

    self.__store = self.__cluster_manager_client.get_or_create_store(final_store)
    return self

  def __get_seq(self):
    self.__seq = self.__seq + 1
    return self.__seq

  # computing api
  def map_values(self, func, output = None, options = {}):
    functor = ErFunctor(name=RollPair.MAP_VALUES, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.MAP_VALUES,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
        input = job,
        output_type = ErJob,
        endpoint = self.__roll_pair_service_endpoint,
        command_uri = CommandURI(f'{RollPair.__uri_prefix}/{RollPair.MAP_VALUES}'),
        serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, options=self._parent_opts)

  def map(self, func, output = None, options = {}):
    functor = ErFunctor(name=RollPair.MAP, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.MAP,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
        input = job,
        output_type = ErJob,
        endpoint = self.__roll_pair_service_endpoint,
        command_uri = CommandURI(f'{RollPair.__uri_prefix}/{RollPair.MAP}'),
        serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, options=self._parent_opts)

  def reduce(self, func, output = None, options = {}):
    functor = ErFunctor(name=RollPair.REDUCE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))

    outputs = []
    if output:
      outpus.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.REDUCE,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
        input = job,
        output_type = ErJob,
        endpoint = self.__roll_pair_service_endpoint,
        command_uri = CommandURI(f'{RollPair.__uri_prefix}/{RollPair.REDUCE}'),
        serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]

    return RollPair(er_store, options=self._parent_opts)

  def aggregate(self, zero_value, seq_op, comb_op, output = None, options = {}):
    zero_value_functor = ErFunctor(name=RollPair.AGGREGATE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(zero_value))
    seq_op_functor = ErFunctor(name=RollPair.AGGREGATE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(seq_op))
    comb_op_functor = ErFunctor(name=RollPair.AGGREGATE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(comb_op))

    outputs = []
    if output:
      outpus.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.AGGREGATE,
                inputs=[self.__store],
                outputs=outputs,
                functors=[zero_value_functor, seq_op_functor, comb_op_functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
        input = job,
        output_type = ErJob,
        endpoint = self.__roll_pair_service_endpoint,
        command_uri = CommandURI(f'{RollPair.__uri_prefix}/{RollPair.RUNJOB}'),
        serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]

    return RollPair(er_store, options=self._parent_opts)

  def join(self, other, func):
    functor = ErFunctor(name=RollPair.JOIN, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))

    job = ErJob(id=self.__session_id, name=RollPair.JOIN,
                inputs=[self.__store, other._store],
                functors=[functor])

    request = ErCommandRequest(id=self.__get_seq(),
                              uri=f'{RollPair.__uri_prefix}{RollPair.JOIN}',
                              args=[job.to_proto().SerializeToString()])

    response = self.__roll_pair_service_stub.call(request.to_proto())

    des_response = ErCommandResponse.from_proto(response)

    des_store = ErStore.from_proto_string(des_response._results[0])

    return RollPair(des_store)
