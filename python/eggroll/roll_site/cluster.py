#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
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

import asyncio
import concurrent

import grpc

#from arch.api.proto import federation_pb2
#from eggroll.roll_site.api.proto import basic_meta_pb2, storage_basic_pb2
from eggroll.utils import file_utils
from eggroll.roll_site.utils import eggroll_serdes
from eggroll.utils.log_utils import getLogger
from eggroll.core.meta_model import ErStoreLocator, ErStore, ErFunctor
from eggroll.core.constants import StoreTypes
#from eggroll.roll_pair.roll_pair import RollPair


_serdes = eggroll_serdes.PickleSerdes

OBJECT_STORAGE_NAME = "__federation__"

CONF_KEY_TARGET = "rollsite"
CONF_KEY_LOCAL = "local"
CONF_KEY_SERVER = "servers"

'''
ERROR_STATES = [federation_pb2.CANCELLED, federation_pb2.ERROR]


async def _async_receive(stub, transfer_meta):
    #LOGGER.debug("start receiving {}".format(transfer_meta))
    resp_meta = stub.recv(transfer_meta)
    while resp_meta.transferStatus != federation_pb2.COMPLETE:
        if resp_meta.transferStatus in ERROR_STATES:
            raise IOError(
                "receive terminated, state: {}".format(federation_pb2.TransferStatus.Name(resp_meta.transferStatus)))
        resp_meta = stub.checkStatusNow(resp_meta)
        await asyncio.sleep(1)
    #LOGGER.info("finish receiving {}".format(resp_meta))
    return resp_meta


def _thread_receive(receive_func, check_func, transfer_meta):
    #LOGGER.debug("start receiving {}".format(transfer_meta))
    resp_meta = receive_func(transfer_meta)
    while resp_meta.transferStatus != federation_pb2.COMPLETE:
        if resp_meta.transferStatus in ERROR_STATES:
            raise IOError(
                "receive terminated, state: {}".format(federation_pb2.TransferStatus.Name(resp_meta.transferStatus)))
        resp_meta = check_func(resp_meta)
    #LOGGER.info("finish receiving {}".format(resp_meta))
    return resp_meta
'''

def init(job_id, runtime_conf_path, server_conf_path, transfer_conf_path):
    global LOGGER
    LOGGER = getLogger()
    server_conf = file_utils.load_json_conf(server_conf_path)
    if CONF_KEY_SERVER not in server_conf:  #CONF_KEY_SERVER = "servers"
        raise EnvironmentError("server_conf should contain key {}".format(CONF_KEY_SERVER))
    if CONF_KEY_TARGET not in server_conf.get(CONF_KEY_SERVER):  #CONF_KEY_TARGET = "clustercomm"
        raise EnvironmentError(
            "The {} should be a json file containing key: {}".format(server_conf_path, CONF_KEY_TARGET))

    _host = server_conf.get(CONF_KEY_SERVER).get(CONF_KEY_TARGET).get("host")
    _port = server_conf.get(CONF_KEY_SERVER).get(CONF_KEY_TARGET).get("port")
    runtime_conf = file_utils.load_json_conf(runtime_conf_path)
    if CONF_KEY_LOCAL not in runtime_conf: #CONF_KEY_LOCAL = "local" 这里从role的角色角度，本地的角色？
        raise EnvironmentError("runtime_conf should be a dict containing key: {}".format(CONF_KEY_LOCAL))

    _party_id = runtime_conf.get(CONF_KEY_LOCAL).get("party_id")
    _role = runtime_conf.get(CONF_KEY_LOCAL).get("role")  #获取local的角色
    return RollSiteRuntime(job_id, _party_id, _role, runtime_conf, transfer_conf_path,  _host, _port)


class RollSiteRuntime(object):
    instance = None

    @staticmethod
    def __remote__object_key(*args):
        return "-".join(["{}".format(arg) for arg in args])

    @staticmethod
    def get_instance():
        if RollSiteRuntime.instance is None:
            raise EnvironmentError("federation should be initialized before use")
        return RollSiteRuntime.instance

    #def __init__(self, job_id, party_id, role, runtime_conf, host, port):
    def __init__(self, job_id, party_id, role, runtime_conf, transfer_conf_path, host, port):
        self.trans_conf = file_utils.load_json_conf(transfer_conf_path)
        self.job_id = job_id
        self.party_id = party_id
        self.role = role
        self.runtime_conf = runtime_conf
        self.tag = True
        self.dst_host = host
        self.dst_port = port
        #guest_list = pull("guest_list", host)  通过发布订阅机制获取的partyId,IP，port列表
        print("__init__")

    '''
    def __get_locator(self, obj, name=None):
        if isinstance(obj, _DTable):
            return storage_basic_pb2.StorageLocator(type=obj._type, namespace=obj._namespace, name=obj._name,
                                                    fragment=obj._partitions)
        else:
            return storage_basic_pb2.StorageLocator(type=storage_basic_pb2.LMDB, namespace=self.job_id,
                                                    name=name)
    '''

    def __check_authorization(self, name, is_send=True):
        #name传进来的是*号，暂时不做判断
        if is_send and self.trans_conf.get('src') != self.role:
            print(self.role)
            raise ValueError("{} is not allow to send from {}".format(self.role))
        elif not is_send and self.role not in self.trans_conf.get('dst'):
            raise ValueError("{} is not allow to receive from {}".format(self.role))

    def __get_parties(self, role):
        return self.runtime_conf.get('role').get(role)

    def remote(self, obj, name: str, tag: str, role=None, idx=-1):
        options = {'cluster_manager_host': 'localhost',
                   'cluster_manager_port': 4670,
                   'pair_type': 'v1/roll-pair',
                   'roll_pair_service_host': 'localhost',
                   'roll_pair_service_port': 20000}

        self.__check_authorization(name)

        if idx >= 0:
            if role is None:
                raise ValueError("{} cannot be None if idx specified".format(role))
            parties = {role: [self.__get_parties(role)[idx]]}
        elif role is not None:
            if role not in self.trans_conf.get('dst'):
                raise ValueError("{} is not allowed to receive {}".format(role, name))
            parties = {role: self.__get_parties(role)}
        else:
            parties = {}
            for _role in self.trans_conf.get('dst'):   #这里获取到"guest"
                print(_role)
                #从runtime_conf的“role”里获取guest列表，这里要改成从注册列表里获取guest的party列表
                parties[_role] = self.__get_parties(_role)
                print ("type of parties:", type(parties))

        for _role, _partyInfos in parties.items():
            print("_role:", _role, "_partyIds:", _partyInfos)
            for _partyId in _partyInfos:
                _tagged_key = self.__remote__object_key(self.job_id, name, tag, self.role, self.party_id, _role,
                                                        _partyId, self.dst_host, self.dst_port)
                print(_tagged_key)
                if isinstance(obj, RollPair):
                    '''
                    If it is a table, send the meta right away.
                    '''
                    # added by bryce
                    #obj.set_gc_disable()
                    #desc = federation_pb2.TransferDataDesc(transferDataType=federation_pb2.DTABLE,
                    #                                       storageLocator=self.__get_locator(obj),
                    #                                       taggedVariableName=_serdes.serialize(_tagged_key))
                    name = obj._name
                    namespace = obj._namespace
                else:
                    '''
                    If it is a object, put the object in the table and send the table meta.
                    '''
                    object_storage_table_name = '{}.{}'.format(OBJECT_STORAGE_NAME, '-'.join([self.role, str(self.party_id), _role, str(_partyId)]))
                    #_table = _EggRoll.get_instance().table(object_storage_table_name, self.job_id)
                    name = object_storage_table_name
                    namespace = self.job_id

                    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LEVELDB, namespace=namespace,
                                                   name=name, total_partitions=4))
                    rp = RollPair(er_store=store, options=options)
                    rp.put(b'a', b'1')

                    #storage_locator = self.__get_locator(_table)
                    #desc = federation_pb2.TransferDataDesc(transferDataType=federation_pb2.OBJECT,
                    #                                       storageLocator=storage_locator,
                    #                                       taggedVariableName=_serdes.serialize(_tagged_key))

                LOGGER.debug("[REMOTE] Sending {}".format(_tagged_key))

                #dst = federation_pb2.Party(partyId="{}".format(_partyId), name=_role)
                #job = basic_meta_pb2.Job(jobId=self.job_id, name=name)
                #self.stub.send(federation_pb2.TransferMeta(job=job, tag=tag, src=src, dst=dst, dataDesc=desc,
                #                                           type=federation_pb2.SEND))

                src_store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LEVELDB, namespace=namespace,
                                               name=name))
                rp = RollPair(src_store, opts=options)

                res = rp.map_values(lambda v: v, output=ErStore(store_locator =
                                                                ErStoreLocator(store_type=StoreTypes.ROLLPAIR_ROLLSITE,
                                                                               namespace='namespace',
                                                                               name=_tagged_key)))

                print('res: ', res)

                LOGGER.debug("[REMOTE] Sent {}".format(_tagged_key))


    '''
    def get(self, name, tag, idx=-1):
        algorithm, sub_name = self.__check_authorization(name, is_send=False)

        auth_dict = self.trans_conf.get(algorithm)

        src_role = auth_dict.get(sub_name).get('src')

        src_party_ids = self.__get_parties(src_role)

        if 0 <= idx < len(src_party_ids):
            # idx is specified, return the remote object
            party_ids = [src_party_ids[idx]]
        else:
            # idx is not valid, return remote object list
            party_ids = src_party_ids

        job = basic_meta_pb2.Job(jobId=self.job_id, name=name)

        LOGGER.debug(
            "[GET] {} {} getting remote object {} from {} {}".format(self.role, self.party_id, tag, src_role,
                                                                     party_ids))

        # loop = asyncio.get_event_loop()
        # tasks = []
        results = []
        for party_id in party_ids:
            src = federation_pb2.Party(partyId="{}".format(party_id), name=src_role)
            dst = federation_pb2.Party(partyId="{}".format(self.party_id), name=self.role)
            trans_meta = federation_pb2.TransferMeta(job=job, tag=tag, src=src, dst=dst,
                                                     type=federation_pb2.RECV)
            # tasks.append(_receive(self.stub, trans_meta))
            results.append(self.__pool.submit(_thread_receive, self.stub.recv, self.stub.checkStatus, trans_meta))
        # results = loop.run_until_complete(asyncio.gather(*tasks))
        # loop.close()
        results = [r.result() for r in results]
        rtn = []
        for recv_meta in results:
            desc = recv_meta.dataDesc
            _persistent = desc.storageLocator.type != storage_basic_pb2.IN_MEMORY
            dest_table = _EggRoll.get_instance().table(name=desc.storageLocator.name,
                                                       namespace=desc.storageLocator.namespace,
                                                       persistent=_persistent)
            if recv_meta.dataDesc.transferDataType == federation_pb2.OBJECT:
                __tagged_key = _serdes.deserialize(desc.taggedVariableName)
                rtn.append(dest_table.get(__tagged_key))
                LOGGER.debug("[GET] Got remote object {}".format(__tagged_key))
            else:
                rtn.append(dest_table)
                src = recv_meta.src
                dst = recv_meta.dst
                LOGGER.debug(
                    "[GET] Got remote table {} from {} {} to {} {}".format(dest_table, src.name, src.partyId, dst.name,
                                                                           dst.partyId))
        if 0 <= idx < len(src_party_ids):
            return rtn[0]
        return rtn
    '''