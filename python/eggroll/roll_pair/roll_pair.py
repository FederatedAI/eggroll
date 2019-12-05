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
import uuid
from typing import Iterable

import grpc
from grpc._cython import cygrpc

from eggroll.core.command.command_model import ErCommandRequest, ErCommandResponse, CommandURI
from eggroll.core.conf_keys import DeployConfKeys
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.io.format import BinBatchReader
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor, ErTask, ErEndpoint, ErPair, ErPartition, \
  ErProcessor, ErSessionMeta, ErServerCluster, ErProcessorBatch
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle, eggroll_serdes
from eggroll.core.client import ClusterManagerClient, CommandClient
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.constants import StoreTypes, SerdesTypes, PartitionerTypes, ProcessorTypes, DeployType
from eggroll.core.serdes.eggroll_serdes import PickleSerdes, CloudPickleSerdes, EmptySerdes
from eggroll.core.session import ErSession
from eggroll.core.transfer.transfer_service import GrpcTransferServicer, TransferClient
from eggroll.core.utils import string_to_bytes, hash_code
from eggroll.roll_pair.egg_pair import EggPair
from eggroll.roll_pair.shuffler import DefaultShuffler
from concurrent import futures
from eggroll.core.io.kv_adapter import LmdbSortedKvAdapter, RocksdbSortedKvAdapter
from eggroll.core.io.io_utils import get_db_path
from eggroll.core.conf_keys import SessionConfKeys
from eggroll.core.proto import transfer_pb2_grpc

from eggroll.utils import log_utils

log_utils.setDirectory()
LOGGER = log_utils.getLogger()

class ErServerSessionDeployment(object):
  def __init__(self, session_id: str, server_cluster: ErServerCluster, rolls: list, eggs: dict):
    self.session_id = session_id,
    self.server_cluster = server_cluster
    self.rolls = rolls
    self.eggs = eggs    # [server_node_id : list[ErProcessor]]

  @staticmethod
  def from_cm_response(session_id: str, server_cluster: ErServerCluster, roll_processor_batch: ErProcessorBatch, egg_processor_batch: ErProcessorBatch):
    eggs = dict()
    for er_processor in egg_processor_batch._processors:
      server_node_id = er_processor._server_node_id
      if server_node_id not in eggs:
        eggs[server_node_id] = list()
      eggs[server_node_id].append(er_processor)

    return ErServerSessionDeployment(
        session_id=session_id,
        server_cluster=server_cluster,
        rolls=roll_processor_batch._processors,
        eggs=eggs)

  def __repr__(self):
    return f'ErServerSessionDeployment(session_id: {self.session_id}, server_cluster: {repr(self.server_cluster)}, rolls: {self.rolls}, eggs: {self.eggs})'


class RollPairContext(object):

  def __init__(self, session: ErSession):
    self.__session = session
    self.session_id = session.get_session_id()
    # self.default_store_type = StoreTypes.ROLLPAIR_LMDB
    self.default_store_type = StoreTypes.ROLLPAIR_LMDB
    self.deploy_mode = session.get_option(DeployConfKeys.CONFKEY_DEPLOY_MODE)
    self._bindings = {}    # dict[binding_id, list[ErProcessor]]

    _server_cluster = self.__session.cm_client.get_session_server_nodes(self.__session.session_meta)
    _rolls = self.__session.cm_client.get_session_rolls(self.__session.session_meta)
    _eggs = self.__session.cm_client.get_session_eggs(self.__session.session_meta)
    self.__server_session_deployment = ErServerSessionDeployment.from_cm_response(
        self.__session.get_session_id(),
        _server_cluster,
        _rolls,
        _eggs)

    print(f'server_session_deployment: {self.__server_session_deployment}')



  def get_roll_endpoint(self):
    return self.__session._rolls[0]._command_endpoint

  # TODO: return transfer endpoint
  def get_egg_endpoint(self, egg_id):
    return self.__session._eggs[0][0]._command_endpoint

  def get_roll(self):
    return self.__session._rolls[0]

  def route_to_egg(self, partition: ErPartition):
    deployment = self.__server_session_deployment
    target_server_node = partition._processor._server_node_id
    target_egg_processors = len(deployment.eggs[target_server_node])
    target_processor = (partition._id // target_egg_processors) % target_egg_processors

    return deployment.eggs[target_server_node][target_processor]


  def load(self, namespace=None, name=None, options={}):
    store_type = options.get('store_type', self.default_store_type)
    total_partitions = options.get('total_partitions', 1)
    partitioner = options.get('partitioner', PartitionerTypes.BYTESTRING_HASH)
    serdes = options.get('serdes', SerdesTypes.CLOUD_PICKLE)
    create_if_missing = options.get('create_if_missing', True)
    # todo:0: add combine options to pass it through
    store_options = self.__session.get_all_options()
    store_options.update(options)
    store = ErStore(
        store_locator=ErStoreLocator(
            store_type=store_type,
            namespace=namespace,
            name=name,
            total_partitions=total_partitions,
            partitioner=partitioner,
            serdes=serdes),
        options=store_options)

    if create_if_missing:
      result = self.__session.cm_client.get_or_create_store(store)
    else:
      result = self.__session.cm_client.get_store(store)
      if result is None:
        raise EnvironmentError(
          "result is None, please check whether the store:{} has been created before".format(store))
    return RollPair(result, self)

  def parallelize(self, data, options={}):
    namespace = options.get("namespace", None)
    name = options.get("name", None)
    create_if_missing = options.get("create_if_missing", True)

    if namespace is None:
      namespace = self.session_id
    if name is None:
      name = str(uuid.uuid1())
    store = self.load(namespace=namespace, name=name, options=options)
    return store.put_all(data, options=options)

def default_partitioner(k):
  return 0
def default_egg_router(k):
  return 0

class RollPair(object):
  __uri_prefix = 'v1/roll-pair'
  GET = "get"
  PUT = "put"
  GET_ALL = "getAll"
  PUT_ALL = "put_all"
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

  def __partitioner(self, hash_func, total_partitions):
    def hash_mod(k):
      return hash_func(k) % total_partitions
    return hash_mod

  def __init__(self, er_store: ErStore, rp_ctx: RollPairContext):
    self.__command_serdes =  SerdesTypes.PROTOBUF
    self.__roll_pair_command_client = CommandClient()
    self.__store = er_store
    self.value_serdes = self.get_serdes()
    self.key_sedes = self.get_serdes()
    self.partitioner = self.__partitioner(hash_code, self.__store._store_locator._total_partitions)
    self.egg_router = default_egg_router
    self.ctx = rp_ctx
    self.__session_id = self.ctx.session_id
    #TODO: config or auto
    self._transfer_server_endpoint = "localhost:60668"

  def __repr__(self):
    return f'python RollPair(_store={self.__store})'

  def __get_unary_input_adapter(self, options={}):
    input_adapter = None
    if self.ctx.default_store_type == StoreTypes.ROLLPAIR_LMDB:
      input_adapter = LmdbSortedKvAdapter(options)
    elif self.ctx.default_store_type == StoreTypes.ROLLPAIR_LEVELDB:
      input_adapter = RocksdbSortedKvAdapter(options)
    return input_adapter

  def get_serdes(self):
    serdes_type = self.__store._store_locator._serdes
    LOGGER.info(f'serdes type: {serdes_type}')
    if serdes_type == SerdesTypes.CLOUD_PICKLE or serdes_type == SerdesTypes.PROTOBUF:
      return CloudPickleSerdes
    elif serdes_type == SerdesTypes.PICKLE:
      return PickleSerdes
    else:
      return EmptySerdes

  def get_partitions(self):
    return self.__store._store_locator._total_partitions

  def get_name(self):
    return self.__store._store_locator._name

  def get_namespace(self):
    return self.__store._store_locator._namespace


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
  def get(self, k, options={}):
    k = self.get_serdes().serialize(k)
    er_pair = ErPair(key=k, value=None)
    outputs = []
    value = None
    partition_id = self.partitioner(k)
    egg = self.ctx.route_to_egg(self.__store._partitions[partition_id])
    print(egg._command_endpoint)
    print("count:", self.__store._store_locator._total_partitions)
    inputs = [ErPartition(id=partition_id, store_locator=self.__store._store_locator)]
    output = [ErPartition(id=partition_id, store_locator=self.__store._store_locator)]

    job = ErJob(id=self.__session_id, name=RollPair.GET,
                inputs=[self.__store],
                outputs=outputs,
                functors=[ErFunctor(body=cloudpickle.dumps(er_pair))])
    task = ErTask(id=self.__session_id, name=RollPair.GET, inputs=inputs, outputs=output, job=job)
    LOGGER.info("start send req")
    job_resp = self.__roll_pair_command_client.simple_sync_send(
        input=task,
        output_type=ErPair,
        endpoint=egg._command_endpoint,
        command_uri=CommandURI(f'{EggPair.uri_prefix}/{EggPair.GET}'),
        serdes_type=self.__command_serdes
    )
    LOGGER.info("get resp:{}".format(ErPair.from_proto_string(job_resp._value)))

    value = self.value_serdes.deserialize(job_resp._value)

    return value

  def put(self, k, v, options = {}):
    k, v = self.get_serdes().serialize(k), self.get_serdes().serialize(v)
    er_pair = ErPair(key=k, value=v)
    outputs = []
    part_id = self.partitioner(k)
    egg = self.ctx.route_to_egg(self.__store._partitions[part_id])
    inputs = [ErPartition(id=part_id, store_locator=self.__store._store_locator)]
    output = [ErPartition(id=0, store_locator=self.__store._store_locator)]

    job = ErJob(id=self.__session_id, name=RollPair.PUT,
                inputs=[self.__store],
                outputs=outputs,
                functors=[ErFunctor(body=cloudpickle.dumps(er_pair))])
    task = ErTask(id=self.__session_id, name=RollPair.PUT, inputs=inputs, outputs=output, job=job)
    LOGGER.info("start send req")
    job_resp = self.__roll_pair_command_client.simple_sync_send(
        input=task,
        output_type=ErPair,
        endpoint=egg._command_endpoint,
        command_uri=CommandURI(f'{EggPair.uri_prefix}/{EggPair.PUT}'),
        serdes_type=self.__command_serdes
    )
    LOGGER.info("get resp:{}".format(ErPair.from_proto_string(job_resp._value)))
    value = job_resp._value
    return value

  def __get_all_local(self, options={}):
    #path = get_db_path(self.__store._partitions[0])[:-2]
    result = []
    total_partitions  = self.__store._store_locator._total_partitions
    LOGGER.info("get all total partitions:{}".format(total_partitions))
    for i in range(total_partitions):
      path = get_db_path(self.__store._partitions[i])
      options = {"path": path}
      LOGGER.info("db path:{}".format(path))
      if self.ctx.default_store_type == StoreTypes.ROLLPAIR_LMDB:
        input_adapter = LmdbSortedKvAdapter(options)
      elif self.ctx.default_store_type == StoreTypes.ROLLPAIR_LEVELDB:
        input_adapter = RocksdbSortedKvAdapter(options)
      input_iterator = input_adapter.iteritems()

      for k, v in input_iterator:
        #yield (self.get_serdes().deserialize(k), self.get_serdes().deserialize(v))
        result.append((self.get_serdes().deserialize(k), self.get_serdes().deserialize(v)))
      input_adapter.close()
    return result

  def __get_all_cluster(self, options={}):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1),
                         options=[
                           (cygrpc.ChannelArgKey.max_send_message_length, -1),
                           (cygrpc.ChannelArgKey.max_receive_message_length, -1)])
    transfer_servicer = GrpcTransferServicer()
    transfer_pb2_grpc.add_TransferServiceServicer_to_server(transfer_servicer,
                                                            server)

    port = server.add_insecure_port(f'[::]:{0}')
    LOGGER.info("starting roll pair recv service, port:{}".format(port))
    server.start()

    functor = ErFunctor(name=EggPair.GET_ALL, serdes=SerdesTypes.CLOUD_PICKLE,
                        body=cloudpickle.dumps(ErEndpoint(host="localhost", port=port)))
    outputs = []
    LOGGER.info("session id:{}".format(self.__session_id))
    job = ErJob(id=self.__session_id, name=EggPair.GET_ALL,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])
    task = ErTask(id=self.__session_id, name=RollPair.GET_ALL, inputs=self.__store._partitions, outputs=[], job=job)
    #TODO: send to all eggs
    LOGGER.info("get egg port:{}".format(self.ctx.get_egg_endpoint()._port))
    LOGGER.info("job id:{}".format(job._id))
    job_result = self.__roll_pair_command_client.simple_sync_send(
        input=task,
        output_type=ErTask,
        endpoint=self.ctx.get_egg_endpoint(),
        command_uri=CommandURI(f'{EggPair.uri_prefix}/{EggPair.GET_ALL}'),
        serdes_type=self.__command_serdes)

    broker = GrpcTransferServicer.get_broker(str(job._id))
    while not broker.is_closable():
      proto_transfer_batch = broker.get(block=True, timeout=1)
      if proto_transfer_batch:
        bin_data = proto_transfer_batch.data
      reader = BinBatchReader(pair_batch=bin_data)
      try:
        while reader.has_remaining():
          key_size = reader.read_int()
          key = reader.read_bytes(size=key_size)
          value_size = reader.read_int()
          value = reader.read_bytes(size=value_size)
          yield key, value
      except IndexError as e:
        LOGGER.info('finish processing an output')
    GrpcTransferServicer.remove_broker(job._id)

  def __put_all_local(self, items, output=None, options={"include_key": False}):
    if items is None:
      raise EnvironmentError("input items is None")
    include_key = options.get("include_key", False)
    total_partitions = self.__store._store_locator._total_partitions
    adapter_dict = {}

    def put_all_wrapper(k_bytes, total_partitions):
      adapter_options = {}
      _partition_id = self.partitioner(k_bytes)
      if _partition_id >= total_partitions:
        raise EnvironmentError("partitions id:{} out of range:{}".format(_partition_id, total_partitions))
      adapter_options["path"] = get_db_path(self.__store._partitions[_partition_id])
      LOGGER.info("path:{}".format(adapter_options["path"]))
      _input_adapter = self.__get_unary_input_adapter(
        options=adapter_options)
      _input_adapter.put(k_bytes, v_bytes)
      return _input_adapter, _partition_id

    if isinstance(items, Iterable):
      k = 0
      for v in items:
        if include_key:
          LOGGER.info("put k:{}, v:{}".format(v[0], v[1]))
          k_bytes, v_bytes = self.value_serdes.serialize(v[0]), self.value_serdes.serialize(v[1])
          input_adapter, partition_id = put_all_wrapper(k_bytes, total_partitions)
          adapter_dict[partition_id] = input_adapter
        else:
          LOGGER.info("put k:{}, v:{}".format(k, v))
          k_bytes, v_bytes = self.value_serdes.serialize(k), self.value_serdes.serialize(v)
          input_adapter, partition_id = put_all_wrapper(k_bytes, total_partitions)
          adapter_dict[partition_id] = input_adapter
          k = k + 1
    else:
      raise EnvironmentError("iterable obj is required")
    for key in adapter_dict:
      adapter_dict[key].close()

    return self

  def __put_all_cluster(self, items, output=None, options={"include_key": False}):
    inputs = [ErPartition(id=part_id, store_locator=self.__store._store_locator)]
    outputs = []
    if output:
      outputs.append(output)

    job = ErJob(id=self.__session_id, name="putAll",
                inputs=[self.__store],
                outputs=outputs,
                functors=[])
    task = ErTask(id=self.__session_id, name=RollPair.PUT, inputs=inputs, outputs=output, job=job)
    LOGGER.info("start send req")
    job_result = self.__roll_pair_command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.MAP_VALUES}'),
      serdes_type=self.__command_serdes)
    # TODO: all aggs ?
    shuffle_broker = FifoBroker()
    shuffler = DefaultShuffler(task._job._id, shuffle_broker, output_store, output_partition, p)

    for k1, v1 in items:
      shuffle_broker.put(self.serde.serialize(k1), self.serde.serialize(v1))
    LOGGER.info('finish calculating')
    shuffle_broker.signal_write_finish()
    # TODO: async ?
    shuffler.start()

    shuffle_finished = shuffler.wait_until_finished(600)
    return self.__store

  def get_all(self, options={}):
    if self.ctx.deploy_mode == DeployType.STANDALONE:
      return self.__get_all_local(options=options)
    else:
      return self.__get_all_cluster(options=options)

  def put_all(self, items, output=None, options={}):
    if self.ctx.deploy_mode == DeployType.STANDALONE:
      return self.__put_all_local(items, options=options)
    else:
      return self.__put_all_cluster(items, options=options)

  def __count_local(self):
    total_partitions = self.__store._store_locator._total_partitions
    LOGGER.info("total partitions:{}".format(total_partitions))
    total_count = 0
    for i in range(total_partitions):
      path = get_db_path(self.__store._partitions[i])
      LOGGER.info('db path:{}'.format(get_db_path(self.__store._partitions[i])))
      adapter_options = {"path": path}
      input_adapter = self.__get_unary_input_adapter(options=adapter_options)
      total_count = total_count + input_adapter.count()
    return total_count

  def __count_cluster(self):
    return 0

  def count(self):
    if self.ctx.deploy_mode == DeployType.STANDALONE:
      return self.__count_local()
    else:
      return self.__count_cluster()

  # computing api
  def mapValues(self, func, output = None, options = {}):
    functor = ErFunctor(name=RollPair.MAP_VALUES, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    # todo:0: options issues
    final_options = {}
    final_options.update(self.__store._options)
    final_options.update(options)
    job = ErJob(id=self.__session_id, name=RollPair.MAP_VALUES,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor],
                options=final_options)

    job_result = self.__roll_pair_command_client.simple_sync_send(
        input=job,
        output_type=ErJob,
        endpoint=self.ctx.get_roll_endpoint(),
        command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.MAP_VALUES}'),
        serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

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
        endpoint = self.ctx.get_roll_endpoint(),
        command_uri = CommandURI(f'{RollPair.__uri_prefix}/{RollPair.MAP}'),
        serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

  def mapPartitions(self, func, output = None, options = {}):
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
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.MAPPARTITIONS}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

  def collapsePartitions(self, func, output = None, options = {}):
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
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.COLLAPSEPARTITIONS}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

  def flatMap(self, func, output=None, options={}):
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
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.FLATMAP}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

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
        endpoint = self.ctx.get_roll_endpoint(),
        command_uri = CommandURI(f'{RollPair.__uri_prefix}/{RollPair.REDUCE}'),
        serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]

    return RollPair(er_store, self.ctx)

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
        endpoint = self.ctx.get_roll_endpoint(),
        command_uri = CommandURI(f'{RollPair.__uri_prefix}/{RollPair.RUNJOB}'),
        serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]

    return RollPair(er_store, self.ctx)

  def glom(self, output=None, options={}):
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
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.GLOM}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

  def sample(self, fraction, seed=None, output=None, options={}):
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
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.SAMPLE}'),
      serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

  def filter(self, func, output=None, options={}):
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
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.FILTER}'),
      serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

  def subtractByKey(self, other, output=None, options={}):
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
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.SUBTRACTBYKEY}'),
      serdes_type=self.__command_serdes)
    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

  def union(self, other, func=lambda v1, v2: v1, output=None, options={}):
    functor = ErFunctor(name=RollPair.UNION, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.UNION,
                inputs=[self.__store, other.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__roll_pair_command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.UNION}'),
      serdes_type=self.__command_serdes)
    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

  def join(self, other, func, output=None, options={}):
    functor = ErFunctor(name=RollPair.JOIN, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    final_options = {}
    final_options.update(self.__store._options)
    final_options.update(options)
    job = ErJob(id=self.__session_id, name=RollPair.JOIN,
                inputs=[self.__store, other.__store],
                outputs=outputs,
                functors=[functor],
                options=final_options)

    job_result = self.__roll_pair_command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.JOIN}'),
      serdes_type=self.__command_serdes)
    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)
