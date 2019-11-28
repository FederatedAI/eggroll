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
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor, ErTask, ErEndpoint, ErPair, ErPartition, \
  ErProcessor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle, eggroll_serdes
from eggroll.core.command.command_client import CommandClient
from eggroll.cluster_manager.cluster_manager_client import ClusterManagerClient
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.constants import StoreTypes, SerdesTypes, PartitionerTypes
from eggroll.core.serdes.eggroll_serdes import PickleSerdes, CloudPickleSerdes
from eggroll.core.utils import string_to_bytes
from eggroll.roll_pair.egg_pair import EggPair


class RollPair(object):
  __uri_prefix = 'v1/roll-pair'
  GET = "get"
  PUT = "put"
  MAP = 'map'
  MAP_VALUES = 'mapValues'
  REDUCE = 'reduce'
  JOIN = 'join'
  AGGREGATE = 'aggregate'
  COLLAPSEPARTITIONS = 'collapsePartitions'
  MAPPARTITIONS = 'mapPartitions'
  GLOM = 'glom'
  FLATMAP = 'flatMap'
  SAMPLE = 'sample'
  FILTER = 'filter'
  SUBTRACTBYKEY = 'subtractByKey'
  UNION = 'union'
  RUNJOB = 'runJob'

  def __init__(self, er_store: ErStore, options = {'cluster_manager_host': 'localhost', 'cluster_manager_port': 4670}):
    _grpc_channel_factory = GrpcChannelFactory()

    if options['pair_type'] == RollPair.__uri_prefix:
      self.__roll_pair_service_endpoint = ErEndpoint(host = options['roll_pair_service_host'], port = options['roll_pair_service_port'])
      self.__roll_pair_service_channel = _grpc_channel_factory.create_channel(self.__roll_pair_service_endpoint)
      self.__roll_pair_service_stub = command_pb2_grpc.CommandServiceStub(self.__roll_pair_service_channel)
    elif options['pair_type'] == EggPair.uri_prefix:
      self.__egg_pair_service_endpoint = ErEndpoint(host=options['egg_pair_service_host'], port=options['egg_pair_service_port'])
      print("init endpoint:{}".format(self.__egg_pair_service_endpoint))
      self.__egg_pair_service_channel = _grpc_channel_factory.create_channel(self.__egg_pair_service_endpoint)
      self.__egg_pair_service_stub = command_pb2_grpc.CommandServiceStub(self.__egg_pair_service_channel)

    self.__cluster_manager_channel = _grpc_channel_factory.create_channel(ErEndpoint(options['cluster_manager_host'], options['cluster_manager_port']))

    self.__command_serdes = options.get('serdes', SerdesTypes.CLOUD_PICKLE)
    self.__roll_pair_command_client = CommandClient()

    self.__cluster_manager_client = ClusterManagerClient(options)
    self._parent_opts = options

    # todo: integrate with session mechanism
    self.__seq = 1
    self.__session_id = '1'
    self.value_serdes = eggroll_serdes.get_serdes()

    self.land(er_store, options)

  def __repr__(self):
    return f'python RollPair(_store={self.__store})'

  def get_serdes(self):
    if self.__store._store_locator._serdes == SerdesTypes.PROTOBUF:
      return CloudPickleSerdes
    elif self.__store._store_locator._serdes == SerdesTypes.PICKLE:
      return PickleSerdes
    else:
      return CloudPickleSerdes

  def land(self, er_store: ErStore, options = {}):
    if er_store:
      final_store = er_store
    else:
      store_type = options.get('store_type', StoreTypes.ROLLPAIR_LEVELDB)
      namespace = options.get('namespace', '')
      name = options.get('name', '')
      total_partitions = options.get('total_partitions', 0)
      partitioner = options.get('partitioner', PartitionerTypes.BYTESTRING_HASH)
      serdes = options.get('serdes', SerdesTypes.CLOUD_PICKLE)

      final_store = ErStore(
          store_locator=ErStoreLocator(
              store_type=store_type,
              namespace=namespace,
              name=name,
              total_partitions=total_partitions,
              partitioner=partitioner,
              serdes=serdes))

    self.__store = self.__cluster_manager_client.get_or_create_store(final_store)
    return self

  def __get_seq(self):
    self.__seq = self.__seq + 1
    return self.__seq

  def kv_to_bytes(self, **kwargs):
    use_serialize = kwargs.get("use_serialize", True)
    # can not use is None
    if "k" in kwargs and "v" in kwargs:
      k, v = kwargs["k"], kwargs["v"]
      return (self.value_serdes.serialize(k), self.value_serdes.serialize(v)) if use_serialize \
        else (string_to_bytes(k), string_to_bytes(v))
    elif "k" in kwargs:
      k = kwargs["k"]
      return self.value_serdes.serialize(k) if use_serialize else string_to_bytes(k)
    elif "v" in kwargs:
      v = kwargs["v"]
      return self.value_serdes.serialize(v) if use_serialize else string_to_bytes(v)

  """
  
    storage api
  
  """
  def get(self, k, opt = {}):
    k = self.get_serdes().serialize(k)
    er_pair = ErPair(key=k, value=None)
    outputs = []
    value = None
    print("count:", self.__store._store_locator._total_partitions)
    for i in range(self.__store._store_locator._total_partitions):
      inputs = [ErPartition(id=i, store_locator=self.__store._store_locator,
                            processor=ErProcessor(id=1,command_endpoint=self.__egg_pair_service_endpoint,
                                                  data_endpoint=self.__egg_pair_service_endpoint))]
      output = [ErPartition(id=i, store_locator=self.__store._store_locator,
                            processor=ErProcessor(id=1,command_endpoint=self.__egg_pair_service_endpoint,
                                                  data_endpoint=self.__egg_pair_service_endpoint))]

      job = ErJob(id=self.__session_id, name=RollPair.GET,
                  inputs=[self.__store],
                  outputs=outputs,
                  functors=[ErFunctor(body=cloudpickle.dumps(er_pair))])
      task = ErTask(id=self.__session_id, name=RollPair.GET, inputs=inputs, outputs=output, job=job)
      print("start send req")
      job_resp = self.__roll_pair_command_client.simple_sync_send(
        input=task,
        output_type=ErPair,
        endpoint=self.__egg_pair_service_endpoint,
        command_uri=CommandURI(f'{EggPair.uri_prefix}/{EggPair.GET}'),
        serdes_type=self.__command_serdes
      )
      print("get resp:{}".format(ErPair.from_proto_string(job_resp._value)))

      if value is not None and value != b'':
        value = self.value_serdes.deserialize(job_resp._value)
        print(value)
        break

    return value

  def put(self, k, v, opt = {}):
    k, v = self.get_serdes().serialize(k), self.get_serdes().serialize(v)
    er_pair = ErPair(key=k, value=v)

    inputs = [ErPartition(id=1, store_locator=self.__store._store_locator,
                          processor=ErProcessor(id=1,command_endpoint=self.__egg_pair_service_endpoint,
                                                data_endpoint=self.__egg_pair_service_endpoint))]
    output = [ErPartition(id=1, store_locator=self.__store._store_locator,
                          processor=ErProcessor(id=1,command_endpoint=self.__egg_pair_service_endpoint,
                                                data_endpoint=self.__egg_pair_service_endpoint))]

    job = ErJob(id=self.__session_id, name=RollPair.PUT,
                inputs=[self.__store],
                outputs=output,
                functors=[ErFunctor(body=cloudpickle.dumps(er_pair))])
    task = ErTask(id=self.__session_id, name=RollPair.PUT, inputs=inputs, outputs=output, job=job)
    print("start send req")
    job_resp = self.__roll_pair_command_client.simple_sync_send(
      input=task,
      output_type=ErPair,
      endpoint=self.__egg_pair_service_endpoint,
      command_uri=CommandURI(f'{EggPair.uri_prefix}/{EggPair.PUT}'),
      serdes_type=self.__command_serdes
    )
    print("get resp:{}".format(ErPair.from_proto_string(job_resp._value)))
    value = job_resp._value
    return value

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

  def map_partitions(self, func, output = None, options = {}):
    functor = ErFunctor(name=RollPair.MAPPARTITIONS, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.MAPPARTITIONS,
                 inputs=[self.__store],
                 outputs=outputs,
                 functors=[functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.MAPPARTITIONS}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, options=self._parent_opts)

  def collapse_partitions(self, func, output = None, opt = {}):
    functor = ErFunctor(name=RollPair.COLLAPSEPARTITIONS, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)

    job = ErJob(id=self.__session_id, name=RollPair.COLLAPSEPARTITIONS,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.COLLAPSEPARTITIONS}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, options=self._parent_opts)

  def flat_map(self, func, output=None, opt={}):
    functor = ErFunctor(name=RollPair.FLATMAP, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)

    job = ErJob(id=self.__session_id, name=RollPair.FLATMAP,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.FLATMAP}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, options=self._parent_opts)

  def reduce(self, func, output = None, options = {}):
    functor = ErFunctor(name=RollPair.REDUCE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))

    outputs = []
    if output:
      outputs.append(output)
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
      outputs.append(output)
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

  def glom(self, output=None, opt={}):
    functor = ErFunctor(name=RollPair.GLOM, serdes=SerdesTypes.CLOUD_PICKLE)
    outputs = []
    if output:
      outputs.append(output)

    job = ErJob(id=self.__session_id, name=RollPair.GLOM,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.GLOM}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, options=self._parent_opts)

  def sample(self, fraction, seed=None, output=None, opt={}):
    er_fraction = ErFunctor(name=RollPair.REDUCE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(fraction))
    er_seed  = ErFunctor(name=RollPair.REDUCE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(seed))

    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.SAMPLE,
                inputs=[self.__store],
                outputs=outputs,
                functors=[er_fraction, er_seed])

    job_result = self.__roll_pair_command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.SAMPLE}'),
      serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, options=self._parent_opts)

  def filter(self, func, output=None, opt={}):
    functor = ErFunctor(name=RollPair.FILTER, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))

    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.FILTER,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.FILTER}'),
      serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, options=self._parent_opts)

  def subtract_by_key(self, other, output=None, opt={}):
    functor = ErFunctor(name=RollPair.SUBTRACTBYKEY, serdes=SerdesTypes.CLOUD_PICKLE)
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.SUBTRACTBYKEY,
                inputs=[self.__store, other.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.SUBTRACTBYKEY}'),
      serdes_type=self.__command_serdes)
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, options=self._parent_opts)

  def union(self, other, func=lambda v1, v2: v1, output=None, opt={}):
    functor = ErFunctor(name=RollPair.SUBTRACTBYKEY, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.SUBTRACTBYKEY,
                inputs=[self.__store, other.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.SUBTRACTBYKEY}'),
      serdes_type=self.__command_serdes)
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, options=self._parent_opts)

  def join(self, other, func, output=None, opt={}):
    functor = ErFunctor(name=RollPair.JOIN, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.JOIN,
                inputs=[self.__store, other.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.JOIN}'),
      serdes_type=self.__command_serdes)
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, options=self._parent_opts)
