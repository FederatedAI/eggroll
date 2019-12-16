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
from threading import Thread

from eggroll.core.client import CommandClient
from eggroll.core.command.command_model import CommandURI
from eggroll.core.conf_keys import DeployConfKeys
from eggroll.core.constants import StoreTypes, SerdesTypes, PartitionerTypes, \
  DeployType
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.io.io_utils import get_db_path
from eggroll.core.io.kv_adapter import LmdbSortedKvAdapter, \
  RocksdbSortedKvAdapter
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor, \
  ErTask, ErPair, ErPartition, \
  ErServerCluster, ErProcessorBatch
from eggroll.core.pair_store.adapter import BrokerAdapter
from eggroll.core.serdes import cloudpickle
from eggroll.core.serdes.eggroll_serdes import PickleSerdes, CloudPickleSerdes, \
  EmptySerdes
from eggroll.core.io.format import BinBatchReader
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor, ErTask, ErEndpoint, ErPair, ErPartition, \
  ErServerCluster, ErProcessorBatch

from eggroll.core.serdes import cloudpickle, eggroll_serdes
from eggroll.core.client import ClusterManagerClient, CommandClient

from eggroll.core.constants import StoreTypes, SerdesTypes, PartitionerTypes, DeployType
from eggroll.core.serdes.eggroll_serdes import PickleSerdes, CloudPickleSerdes, EmptySerdes
from eggroll.core.session import ErSession
from eggroll.core.utils import string_to_bytes, hash_code
from eggroll.roll_pair import create_serdes
from eggroll.roll_pair.egg_pair import EggPair
from eggroll.roll_pair.transfer_pair import TransferPair
from concurrent import futures
from eggroll.core.io.kv_adapter import LmdbSortedKvAdapter, RocksdbSortedKvAdapter

from eggroll.core.proto import transfer_pb2_grpc
from eggroll.roll_pair.utils.pair_utils import partitioner, get_db_path

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

  def populate_processor(self, store: ErStore):
    populated_partitions = list()
    for p in store._partitions:
      pp = ErPartition(id=p._id, store_locator=p._store_locator, processor=self.route_to_egg(p))
      populated_partitions.append(pp)
    return ErStore(store_locator=store._store_locator, partitions=populated_partitions, options=store._options)

  def load(self, namespace=None, name=None, options={}):
    store_type = options.get('store_type', self.default_store_type)
    total_partitions = options.get('total_partitions', 1)
    partitioner = options.get('partitioner', PartitionerTypes.BYTESTRING_HASH)
    serdes = options.get('serdes', SerdesTypes.CLOUD_PICKLE)
    create_if_missing = options.get('create_if_missing', True)
    # todo:0: add combine options to pass it through
    store_options = self.__session.get_all_options()
    store_options.update(options)
    final_options = store_options.copy()
    if 'create_if_missing' in final_options:
      del final_options['create_if_missing']
    if 'include_key' in final_options:
      del final_options['include_key']
    if 'total_partitions' in final_options:
      del final_options['total_partitions']
    if 'name' in final_options:
      del final_options['name']
    if 'namespace' in final_options:
      del final_options['namespace']
    print("final_options:{}".format(final_options))
    store = ErStore(
        store_locator=ErStoreLocator(
            store_type=store_type,
            namespace=namespace,
            name=name,
            total_partitions=total_partitions,
            partitioner=partitioner,
            serdes=serdes),
        options=final_options)

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
  _uri_prefix = 'v1/roll-pair'
  _egg_uri_prefix = 'v1/egg-pair'
  GET = "get"
  PUT = "put"
  GET_ALL = "getAll"
  PUT_ALL = "putAll"
  DESTROY = "destroy"
  DELETE = "delete"
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
  RUNTASK = 'runTask'

  def __init__(self, er_store: ErStore, rp_ctx: RollPairContext):
    self.__command_serdes = SerdesTypes.PROTOBUF
    self.__command_client = CommandClient()
    self.__store = er_store
    self.value_serdes = self.get_serdes()
    self.key_serdes = self.get_serdes()
    self.partitioner = partitioner(hash_code, self.__store._store_locator._total_partitions)
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
    k = create_serdes(self.__store._store_locator._serdes).serialize(k)
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
    job_resp = self.__command_client.simple_sync_send(
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
    k, v = create_serdes(self.__store._store_locator._serdes).serialize(k),\
           create_serdes(self.__store._store_locator._serdes).serialize(v)
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
    job_resp = self.__command_client.simple_sync_send(
        input=task,
        output_type=ErPair,
        endpoint=egg._command_endpoint,
        command_uri=CommandURI(f'{EggPair.uri_prefix}/{EggPair.PUT}'),
        serdes_type=self.__command_serdes
    )
    LOGGER.info("get resp:{}".format(ErPair.from_proto_string(job_resp._value)))
    value = job_resp._value
    return value

  def get_all(self, options={}):
    print('get all functor')

    def send_command():
      job = ErJob(id=self.__session_id,
                  name=RollPair.GET_ALL,
                  inputs=[self.__store],
                  outputs=[self.__store],
                  functors=[])

      result = self.__command_client.simple_sync_send(
              input=job,
              output_type=ErJob,
              endpoint=self.ctx.get_roll_endpoint(),
              command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.GET_ALL}'),
              serdes_type=SerdesTypes.PROTOBUF)

      return result

    def pair_generator(broker_adapter: BrokerAdapter, key_serdes, value_serdes, cleanup_func):
      for k, v in broker_adapter.iteritems():
        yield key_serdes.deserialize(k), value_serdes.deserialize(v)
      #broker_adapter.close()
      cleanup_func()


    def cleanup():
      nonlocal transfer_pair
      nonlocal command_thread

      command_thread.join()
      transfer_pair.join()

    command_thread = Thread(target=send_command)
    command_thread.start()

    populated_store = self.ctx.populate_processor(self.__store)
    transfer_pair = TransferPair(transfer_id=self.__session_id, output_store=populated_store)

    adapter = BrokerAdapter(FifoBroker(write_signals=self.__store._store_locator._total_partitions))
    transfer_pair.start_pull(adapter)

    return pair_generator(adapter, self.key_serdes, self.value_serdes, cleanup)

  def put_all(self, items, output=None, options={}):
    include_key = options.get("include_key", False)
    job_id = self.__session_id

    # TODO:0: consider multiprocessing scenario. parallel size should be sent to egg_pair to set write signal count
    def send_command():
      job = ErJob(id=job_id,
                  name=RollPair.PUT_ALL,
                  inputs=[self.__store],
                  outputs=[self.__store],
                  functors=[])

      result = self.__command_client.simple_sync_send(
              input=job,
              output_type=ErJob,
              endpoint=self.ctx.get_roll_endpoint(),
              command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.PUT_ALL}'),
              serdes_type=SerdesTypes.PROTOBUF)

      return result

    command_thread = Thread(target=send_command)
    command_thread.start()

    populated_store = self.ctx.populate_processor(self.__store)

    shuffler = TransferPair(job_id, populated_store)

    broker = FifoBroker()
    shuffler.start_push(input_broker=broker, partition_function=self.partitioner)

    key_serdes = self.key_serdes
    value_serdes = self.value_serdes

    try:
      if include_key:
        for k, v in items:
          broker.put(item=(key_serdes.serialize(k), value_serdes.serialize(v)))
      else:
        k = 0
        for v in items:
          broker.put(item=(key_serdes.serialize(k), value_serdes.serialize(v)))
          k += 1
    except StopIteration as e:
      pass
    finally:
      broker.signal_write_finish()

    shuffler.join()
    command_thread.join()

    return RollPair(populated_store, self.ctx)

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

  def destroy(self):
    total_partitions = self.__store._store_locator._total_partitions
    for i in range(total_partitions):
      job_outputs = []
      #part_id = self.partitioner(i)
      egg = self.ctx.route_to_egg(self.__store._partitions[i])
      task_inputs = [ErPartition(id=i, store_locator=self.__store._store_locator)]
      task_outputs = []

      job = ErJob(id=self.__session_id, name=RollPair.DESTROY,
                  inputs=[self.__store],
                  outputs=job_outputs,
                  functors=[ErFunctor(body=None)])
      task = ErTask(id=self.__session_id, name=RollPair.DESTROY, inputs=task_inputs, outputs=task_outputs, job=job)
      LOGGER.info("start send req")
      job_resp = self.__command_client.simple_sync_send(
        input=task,
        output_type=ErPair,
        endpoint=egg._command_endpoint,
        command_uri=CommandURI(f'{EggPair.uri_prefix}/{EggPair.DESTROY}'),
        serdes_type=self.__command_serdes
      )

  def delete(self, k, options={}):
    k = create_serdes(self.__store).serialize(k)
    er_pair = ErPair(key=k, value=None)
    outputs = []
    value = None
    partition_id = self.partitioner(k)
    egg = self.ctx.route_to_egg(self.__store._partitions[partition_id])
    print(egg._command_endpoint)
    print("count:", self.__store._store_locator._total_partitions)
    inputs = [ErPartition(id=partition_id, store_locator=self.__store._store_locator)]
    output = [ErPartition(id=partition_id, store_locator=self.__store._store_locator)]

    job = ErJob(id=self.__session_id, name=RollPair.DELETE,
                inputs=[self.__store],
                outputs=outputs,
                functors=[ErFunctor(body=cloudpickle.dumps(er_pair))])
    task = ErTask(id=self.__session_id, name=RollPair.DELETE, inputs=inputs, outputs=output, job=job)
    LOGGER.info("start send req")
    job_resp = self.__command_client.simple_sync_send(
      input=task,
      output_type=ErPair,
      endpoint=egg._command_endpoint,
      command_uri=CommandURI(f'{EggPair.uri_prefix}/{EggPair.DELETE}'),
      serdes_type=self.__command_serdes
    )
    LOGGER.info("get resp:{}".format(ErPair.from_proto_string(job_resp._value)))

  def save_as(self, name, namespace, partition):
    store = ErStore(store_locator=ErStoreLocator(store_type=self.ctx.default_store_type, namespace=namespace,
                                                 name=name, total_partitions=partition))
    return self.map_values(lambda v: v, output=store)

  # computing api
  def map_values(self, func, output = None, options = {}):
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

    job_result = self.__command_client.simple_sync_send(
        input=job,
        output_type=ErJob,
        endpoint=self.ctx.get_roll_endpoint(),
        command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.MAP_VALUES}'),
        serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

  def map(self, func, output=None, options={}):
    functor = ErFunctor(name=RollPair.MAP, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.MAP,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
        input=job,
        output_type=ErJob,
        endpoint=self.ctx.get_roll_endpoint(),
        command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.MAP}'),
        serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    LOGGER.info(er_store)
    print(er_store)

    return RollPair(er_store, self.ctx)

  def map_partitions(self, func, output = None, options = {}):
    functor = ErFunctor(name=RollPair.MAPPARTITIONS, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.MAPPARTITIONS,
                 inputs=[self.__store],
                 outputs=outputs,
                 functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.MAPPARTITIONS}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

  def collapse_partitions(self, func, output = None, options = {}):
    functor = ErFunctor(name=RollPair.COLLAPSEPARTITIONS, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)

    job = ErJob(id=self.__session_id, name=RollPair.COLLAPSEPARTITIONS,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.COLLAPSEPARTITIONS}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

  def flat_map(self, func, output=None, options={}):
    functor = ErFunctor(name=RollPair.FLATMAP, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)

    job = ErJob(id=self.__session_id, name=RollPair.FLATMAP,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.FLATMAP}'),
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

    job_result = self.__command_client.simple_sync_send(
        input = job,
        output_type = ErJob,
        endpoint = self.ctx.get_roll_endpoint(),
        command_uri = CommandURI(f'{RollPair._uri_prefix}/{RollPair.REDUCE}'),
        serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]

    return RollPair(er_store, self.ctx)

  def aggregate(self, zero_value, seq_op, comb_op, output=None, options={}):
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

    job_result = self.__command_client.simple_sync_send(
        input=job,
        output_type=ErJob,
        endpoint=self.ctx.get_roll_endpoint(),
        command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.RUNJOB}'),
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

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.GLOM}'),
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

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.SAMPLE}'),
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

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.FILTER}'),
      serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)

  def subtract_by_key(self, other, output=None, options={}):
    functor = ErFunctor(name=RollPair.SUBTRACTBYKEY, serdes=SerdesTypes.CLOUD_PICKLE)
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.SUBTRACTBYKEY,
                inputs=[self.__store, other.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.SUBTRACTBYKEY}'),
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

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.UNION}'),
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

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.ctx.get_roll_endpoint(),
      command_uri=CommandURI(f'{RollPair._uri_prefix}/{RollPair.JOIN}'),
      serdes_type=self.__command_serdes)
    er_store = job_result._outputs[0]
    LOGGER.info(er_store)

    return RollPair(er_store, self.ctx)
