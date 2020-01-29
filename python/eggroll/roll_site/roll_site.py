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
#
#

import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION

from eggroll.core.conf_keys import SessionConfKeys
from eggroll.core.constants import StoreTypes
from eggroll.core.error import GrpcCallError
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErStoreLocator, ErStore
from eggroll.core.pair_store.roll_site_adapter import DELIM
from eggroll.core.proto import proxy_pb2, proxy_pb2_grpc
from eggroll.core.serdes import eggroll_serdes
from eggroll.roll_pair.roll_pair import RollPair, RollPairContext
from eggroll.utils import log_utils

L = log_utils.get_logger()
_serdes = eggroll_serdes.PickleSerdes

STATUS_TABLE_NAME = "__roll_site_standalone_status__"


class RollSiteContext:
    grpc_channel_factory = GrpcChannelFactory()

    def __init__(self, federation_session_id, options, rp_ctx: RollPairContext):
        self.federation_session_id = federation_session_id
        self.rp_ctx = rp_ctx

        self.role = options["self_role"]
        self.party_id = options["self_party_id"]
        self.proxy_endpoint = options["proxy_endpoint"]
        self.is_standalone = self.rp_ctx.get_session().get_option(SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE) == "standalone"
        if self.is_standalone:
            self.stub = None
        else:
            channel = self.grpc_channel_factory.create_channel(self.proxy_endpoint)
            self.stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
            self.init_job_session_pair(self.federation_session_id, self.rp_ctx.session_id)
        L.info(f"inited RollSiteContext: {self.__dict__}")

    # todo:1: add options?
    def load(self, name: str, tag: str, options={}):
        return RollSite(name, tag, self, options=options)

    # todo:1: try-except as decorator
    def init_job_session_pair(self, federation_session_id, session_id):
        try:
            task_info = proxy_pb2.Task(model=proxy_pb2.Model(name=federation_session_id, dataKey=bytes(session_id, encoding='utf8')))
            topic_src = proxy_pb2.Topic(name="init_job_session_pair", partyId="{}".format(self.party_id),
                                        role=self.role, callback=None)
            topic_dst = proxy_pb2.Topic(name="init_job_session_pair", partyId="{}".format(self.party_id),
                                        role=self.role, callback=None)
            command_test = proxy_pb2.Command(name="init_job_session_pair")
            conf_test = proxy_pb2.Conf(overallTimeout=1000,
                                       completionWaitTimeout=1000,
                                       packetIntervalTimeout=1000,
                                       maxRetries=10)

            metadata = proxy_pb2.Metadata(task=task_info,
                                          src=topic_src,
                                          dst=topic_dst,
                                          command=command_test,
                                          operator="init_job_session_pair",
                                          seq=0, ack=0,
                                          conf=conf_test)
            packet = proxy_pb2.Packet(header=metadata)

            self.stub.unaryCall(packet)
            L.info(f"send RollSiteContext init to Proxy: {packet}")
        except Exception as e:
            raise GrpcCallError("init_job_session_pair", self.proxy_endpoint, e)


ERROR_STATES = [proxy_pb2.STOP, proxy_pb2.KILL]
OBJECT_STORAGE_NAME = "__federation__"
CONF_KEY_TARGET = "rollsite"
CONF_KEY_LOCAL = "local"
CONF_KEY_SERVER = "servers"


class RollSite:
    def __init__(self, name: str, tag: str, rs_ctx: RollSiteContext, options={}):
        self.ctx = rs_ctx
        self.party_id = self.ctx.party_id
        self.dst_host = self.ctx.proxy_endpoint._host
        self.dst_port = self.ctx.proxy_endpoint._port
        self.federation_session_id = self.ctx.federation_session_id
        self.local_role = self.ctx.role
        self.name = name
        self.tag = tag
        self.stub = self.ctx.stub
        self.process_pool = ThreadPoolExecutor(10)
        self.complete_pool = ThreadPoolExecutor(10)

    @staticmethod
    def __remote__object_key(*args):
        return DELIM.join(["{}".format(arg) for arg in args])

    def __gen_store_name(self):
        pass

    def _thread_receive(self, packet, namespace, _tagged_key):
        try:
            is_standalone = self.ctx.rp_ctx.get_session().get_option(SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE) \
                            == "standalone"
            if is_standalone:
                status_rp = self.ctx.rp_ctx.load(namespace, STATUS_TABLE_NAME + DELIM + self.ctx.federation_session_id)
                retry_cnt = 0
                # TODO:0: sleep retry count and timeout
                while True:
                    msg = f"retry pull: retry_cnt: {retry_cnt}," + \
                          f" tagged_key: '{_tagged_key}', packet: {packet}, namespace: {namespace}"
                    if retry_cnt % 10 == 0:
                        L.info(msg)
                    else:
                        L.debug(msg)
                    retry_cnt += 1
                    ret_list = status_rp.get(_tagged_key)
                    if ret_list:
                        table_namespace = ret_list[2]
                        table_name = ret_list[1]
                        obj_type = ret_list[0]
                        break
                    time.sleep(min(0.1*retry_cnt, 60))

            else:
                retry_cnt = 0
                ret_packet = self.stub.unaryCall(packet)
                while ret_packet.header.ack != 123:
                    msg = f"retry pull: retry_cnt: {retry_cnt}," + \
                          f" tagged_key: '{_tagged_key}', packet: {packet}, namespace: {namespace}"
                    if retry_cnt % 10 == 0:
                        L.info(msg)
                    else:
                        L.debug(msg)
                    retry_cnt += 1
                    if ret_packet.header.ack in ERROR_STATES:
                        raise IOError("receive terminated")
                    ret_packet = self.stub.unaryCall(packet)
                    time.sleep(min(0.1*retry_cnt, 60))
                obj_type = ret_packet.body.value
                table_name = DELIM.join([OBJECT_STORAGE_NAME,
                                         self.federation_session_id,
                                         self.name,
                                         self.tag,
                                         ret_packet.header.src.role,
                                         ret_packet.header.src.partyId,
                                         ret_packet.header.dst.role,
                                         ret_packet.header.dst.partyId])

                table_namespace = self.federation_session_id
            L.debug(f"pull status done: tagged_key:{_tagged_key}, packet:{packet}, namespace:{namespace}")
            rp = self.ctx.rp_ctx.load(namespace=table_namespace, name=table_name)
            result = rp.get(_tagged_key) if obj_type == b'object' else rp
            L.info(f"pull succuess: {_tagged_key}")
            return result
        except Exception as e:
            L.exception(f"pull error:{e}")
            raise GrpcCallError("push", self.ctx.proxy_endpoint, e)

    def push(self, obj, parties: list = None):
        L.info(f"pushing: self:{self.__dict__}, obj_type:{type(obj)}, parties:{parties}")
        futures = []
        for role_party_id in parties:
            # for _partyId in _partyIds:
            _role = role_party_id[0]
            _party_id = role_party_id[1]
            _tagged_key = self.__remote__object_key(self.federation_session_id, self.name, self.tag, self.local_role,
                                                    self.party_id, _role, _party_id)
            L.debug(f"pushing start party:{type(obj)}, {_tagged_key}")
            namespace = self.federation_session_id
            obj_type = 'rollpair' if isinstance(obj, RollPair) else 'object'

            if isinstance(obj, RollPair):
                rp = obj
            else:
                # If it is a object, put the object in the table and send the table meta.
                name = DELIM.join([OBJECT_STORAGE_NAME,
                                   self.federation_session_id,
                                   self.name,
                                   self.tag,
                                   self.local_role,
                                   str(self.party_id),
                                   _role,
                                   str(_party_id)])
                rp = self.ctx.rp_ctx.load(namespace, name)
                rp.put(_tagged_key, obj)

            L.info(f"pushing prepared: {type(obj)}, tag_key:{_tagged_key}")

            def map_values(_tagged_key):
                is_standalone = self.ctx.rp_ctx.get_session().get_option(SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE) \
                                == "standalone"
                if is_standalone:
                    dst_name = DELIM.join([OBJECT_STORAGE_NAME,
                                           self.federation_session_id,
                                           self.name,
                                           self.tag,
                                           self.local_role,
                                           str(self.party_id),
                                           _role,
                                           str(_party_id)])
                    store_type = rp.get_store_type()
                else:
                    dst_name = DELIM.join([OBJECT_STORAGE_NAME,
                                           self.federation_session_id,
                                           self.name,
                                           self.tag,
                                           self.local_role,
                                           str(self.party_id),
                                           _role,
                                           str(_party_id),
                                           self.dst_host,
                                           str(self.dst_port),
                                           obj_type])
                    store_type = StoreTypes.ROLLPAIR_ROLLSITE
                if is_standalone:
                    status_rp = self.ctx.rp_ctx.load(namespace, STATUS_TABLE_NAME + DELIM + self.federation_session_id, self)
                    if isinstance(obj, RollPair):
                        status_rp.put(_tagged_key, (obj_type.encode("utf-8"), rp.get_name(), rp.get_namespace()))
                    else:
                        status_rp.put(_tagged_key, (obj_type.encode("utf-8"), dst_name, namespace))
                else:
                    store = rp.get_store()
                    store_locator = store._store_locator
                    new_store_locator = ErStoreLocator(store_type=store_type,
                                                       namespace=namespace,
                                                       name=dst_name,
                                                       total_partitions=store_locator._total_partitions,
                                                       partitioner=store_locator._partitioner,
                                                       serdes=store_locator._serdes)

                    rp.map_values(lambda v: v,
                        output=ErStore(store_locator=new_store_locator))

                L.info(f"pushing map_values done:{type(obj)}, tag_key:{_tagged_key}")
                return _tagged_key

            future = self.process_pool.submit(map_values, _tagged_key)
            futures.append(future)

        self.process_pool.shutdown(wait=False)

        return futures

    def wait_futures(self, futures):
        # TODO:0: configurable
        ret_future = self.complete_pool.submit(wait, futures, timeout=1000, return_when=FIRST_EXCEPTION)
        self.complete_pool.shutdown(wait=False)
        return ret_future

    def pull(self, parties: list = None):
        futures = []
        for src_role, party_id in parties:
            _tagged_key = self.__remote__object_key(self.federation_session_id, self.name, self.tag, src_role, str(party_id),
                                                    self.local_role, str(self.party_id))

            name = DELIM.join([OBJECT_STORAGE_NAME,
                               self.federation_session_id,
                               self.name,
                               self.tag,
                               src_role,
                               str(party_id),
                               self.local_role,
                               str(self.party_id)])

            task_info = proxy_pb2.Task(taskId=name)
            topic_src = proxy_pb2.Topic(name="get_status", partyId="{}".format(party_id),
                                        role=src_role, callback=None)
            topic_dst = proxy_pb2.Topic(name="get_status", partyId="{}".format(self.party_id),
                                        role=self.local_role, callback=None)
            get_status_command = proxy_pb2.Command(name="get_status")
            conf_test = proxy_pb2.Conf(overallTimeout=1000,
                                       completionWaitTimeout=1000,
                                       packetIntervalTimeout=1000,
                                       maxRetries=10)

            metadata = proxy_pb2.Metadata(task=task_info,
                                          src=topic_src,
                                          dst=topic_dst,
                                          command=get_status_command,
                                          operator="getStatus",
                                          seq=0,
                                          ack=0)

            packet = proxy_pb2.Packet(header=metadata)
            namespace = self.federation_session_id
            L.info(f"pulling prepared tagged_key: {_tagged_key}, packet:{packet}")
            futures.append(self.process_pool.submit(RollSite._thread_receive, self, packet, namespace, _tagged_key))

        self.process_pool.shutdown(wait=False)
        return futures
