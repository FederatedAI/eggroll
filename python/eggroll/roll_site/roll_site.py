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

from eggroll.utils import file_utils
from eggroll.utils.log_utils import getLogger
from eggroll.core.meta_model import ErStoreLocator, ErStore
from eggroll.core.constants import StoreTypes
from eggroll.roll_pair.roll_pair import RollPair
from eggroll.core.session import ErSession
from eggroll.roll_pair.roll_pair import RollPairContext
from eggroll.core.proto import proxy_pb2


class RollSiteContext:
  def __init__(self, job_id, options):
    global LOGGER
    LOGGER = getLogger()
    self.job_id = job_id

    runtime_conf_path = options["runtime_conf_path"]
    server_conf_path = options["server_conf_path"]
    transfer_conf_path = options["transfer_conf_path"]

    server_conf = file_utils.load_json_conf(server_conf_path)
    if CONF_KEY_SERVER not in server_conf:  #CONF_KEY_SERVER = "servers"
      raise EnvironmentError("server_conf should contain key {}".format(CONF_KEY_SERVER))
    if CONF_KEY_TARGET not in server_conf.get(CONF_KEY_SERVER):  #CONF_KEY_TARGET = "clustercomm"
      raise EnvironmentError(
          "The {} should be a json file containing key: {}".format(server_conf_path, CONF_KEY_TARGET))

    self.trans_conf = file_utils.load_json_conf(transfer_conf_path)

    self.dst_host = server_conf.get(CONF_KEY_SERVER).get(CONF_KEY_TARGET).get("host")
    self.dst_port = server_conf.get(CONF_KEY_SERVER).get(CONF_KEY_TARGET).get("port")
    self.runtime_conf = file_utils.load_json_conf(runtime_conf_path)
    if CONF_KEY_LOCAL not in self.runtime_conf:
      raise EnvironmentError("runtime_conf should be a dict containing key: {}".format(CONF_KEY_LOCAL))

    self.party_id = self.runtime_conf.get(CONF_KEY_LOCAL).get("party_id")
    self.role = self.runtime_conf.get(CONF_KEY_LOCAL).get("role")

  def load(self, name: str, tag: str, role=None, idx=-1):
    return RollSite(name, tag, role, idx, self)


ERROR_STATES = [proxy_pb2.STOP, proxy_pb2.KILL]
OBJECT_STORAGE_NAME = "__federation__"
CONF_KEY_TARGET = "rollsite"
CONF_KEY_LOCAL = "local"
CONF_KEY_SERVER = "servers"


def _thread_receive(check_func, packet):
  ret_packet = check_func(packet)
  while ret_packet.transferStatus != proxy_pb2.STOP:
    if ret_packet.transferStatus in ERROR_STATES:
      raise IOError("receive terminated")
  return ret_packet


class RollSite:
  def __init__(self, name: str, tag: str, role, idx, rs_ctx: RollSiteContext):
    self.ctx = rs_ctx
    self.trans_conf = self.ctx.trans_conf
    self.runtime_conf = self.ctx.runtime_conf
    self.party_id = self.ctx.party_id
    self.dst_host = self.ctx.dst_host
    self.dst_port = self.ctx.dst_port
    self.job_id = self.ctx.job_id
    self.src_role = self.ctx.role
    self.name = name
    self.dst_role = role
    self.tag = tag
    self.idx = idx

  @staticmethod
  def __remote__object_key(*args):
    return "-".join(["{}".format(arg) for arg in args])

  def __check_authorization(self, name, is_send=True):
    if is_send and self.trans_conf.get('src') != self.src_role:
      print(self.src_role)
      raise ValueError("{} is not allow to send from {}".format(self.src_role))
    elif not is_send and self.src_role not in self.trans_conf.get('dst'):
      raise ValueError("{} is not allow to receive from {}".format(self.src_role))

  def __get_parties(self, role):
    return self.runtime_conf.get('role').get(role)

  def push(self, obj):
    storage_options = {'cluster_manager_host': 'localhost',
                       'cluster_manager_port': 4670,
                       'pair_type': 'v1/egg-pair',
                       'egg_pair_service_host': 'localhost',
                       'egg_pair_service_port': 20001}

    self.__check_authorization(self.name)
    print("type of obj:", type(obj))

    if self.idx >= 0:
      if self.dst_role is None:
        raise ValueError("{} cannot be None if idx specified".format(self.dst_role))
      parties = {self.dst_role: [self.__get_parties(self.dst_role)[self.idx]]}
    elif self.dst_role is not None:
      if self.dst_role not in self.trans_conf.get('dst'):
        raise ValueError("{} is not allowed to receive {}".format(self.dst_role, self.name))
      parties = {self.dst_role: self.__get_parties(self.dst_role)}
    else:
      parties = {}
      for _role in self.trans_conf.get('dst'):
        print(_role)
        parties[_role] = self.__get_parties(_role)
        print ("type of parties:", type(parties))

    session = ErSession(session_id='testing', options={"eggroll.deploy.mode": "standalone"})
    context = RollPairContext(session)

    for _role, _partyInfos in parties.items():
      for _partyId in _partyInfos:
        _tagged_key = self.__remote__object_key(self.job_id, self.name, self.tag, self.src_role, self.party_id, _role,
                                                _partyId, self.dst_host, self.dst_port)
        if isinstance(obj, RollPair):
          '''
          If it is a table, send the meta right away.
          '''
          name = obj._name
          namespace = obj._namespace
        else:
          '''
          If it is a object, put the object in the table and send the table meta.
          '''
          object_storage_table_name = '{}.{}'.format(OBJECT_STORAGE_NAME, '-'.join([self.src_role, str(self.party_id), _role, str(_partyId)]))
          name = object_storage_table_name
          namespace = self.job_id

          rp = context.load(namespace, name)
          rp.put(_tagged_key, obj)

        LOGGER.debug("[REMOTE] Sending {}".format(_tagged_key))
        rp = context.load(namespace, name)
        print("############map_values#########")
        ret = rp.map_values(lambda v: v, output=ErStore(store_locator =
                                                        ErStoreLocator(store_type=StoreTypes.ROLLPAIR_ROLLSITE,
                                                                       namespace=namespace,
                                                                       name=_tagged_key)))
        print(ret)

        LOGGER.debug("[REMOTE] Sent {}".format(_tagged_key))
        break


  def pull(self):
    storage_options = {'cluster_manager_host': 'localhost',
                       'cluster_manager_port': 4670,
                       'pair_type': 'v1/egg-pair',
                       'egg_pair_service_host': 'localhost',
                       'egg_pair_service_port': 20001}
    algorithm, sub_name = self.__check_authorization(self.name, is_send=False)

    auth_dict = self.trans_conf.get(algorithm)

    src_role = auth_dict.get(sub_name).get('src')

    src_party_ids = self.__get_parties(src_role)

    if 0 <= self.idx < len(src_party_ids):
      # idx is specified, return the remote object
      party_ids = [src_party_ids[self.idx]]
    else:
      # idx is not valid, return remote object list
      party_ids = src_party_ids

    LOGGER.debug(
        "[GET] {} {} getting remote object {} from {} {}".format(self.role, self.party_id, self.tag, src_role,
                                                                 party_ids))

    results = []
    for party_id in party_ids:
      task_info = proxy_pb2.Task(taskId="testTaskId", model=proxy_pb2.Model(name="taskName", dataKey="testKey"))
      topic_src = proxy_pb2.Topic(name="test", partyId="{}".format(party_id),
                                  role=src_role, callback=None)
      topic_dst = proxy_pb2.Topic(name="test", partyId=self.party_id,
                                  role=self.role, callback=None)
      command_test = proxy_pb2.Command(name="get_status")
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

      results.append(self.__pool.submit(_thread_receive, packet))

    results = [r.result() for r in results]
    rtn = []
    for packet in results:
      namespace = self.__remote__object_key(self.job_id, name, tag, self.role, self.party_id)
      store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace=namespace,
                                     name=name))
      rp = RollPair(store, options=storage_options)
      rtn.append(rp)

    if 0 <= self.idx < len(src_party_ids):
      return rtn[0]
    return rtn




