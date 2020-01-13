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

import grpc

from eggroll.core.conf_keys import SessionConfKeys
from eggroll.core.constants import StoreTypes
from eggroll.core.meta_model import ErStoreLocator, ErStore
from eggroll.core.proto import proxy_pb2, proxy_pb2_grpc
from eggroll.core.serdes import eggroll_serdes
from eggroll.roll_pair.roll_pair import RollPair
from eggroll.utils import file_utils
from eggroll.utils import log_utils

LOGGER = log_utils.get_logger()

_serdes = eggroll_serdes.PickleSerdes

STATUS_TABLE_NAME = "__roll_site_standalone_status__"

class RollSiteContext:
    def __init__(self, job_id, self_role, self_partyId, rs_ip, rs_port, rp_ctx):
        self.job_id = job_id
        self.rp_ctx = rp_ctx

        self.role = self_role
        self.party_id = self_partyId 
        self.dst_host = rs_ip 
        self.dst_port = rs_port 
        
        channel = grpc.insecure_channel(
            target="{}:{}".format(self.dst_host, self.dst_port),
            options=[('grpc.max_send_message_length', -1), ('grpc.max_receive_message_length', -1)])
        self.stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
        self.is_standalone = self.rp_ctx.get_session().get_option(SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE) == "standalone"
        if not self.is_standalone:
            self.init_job_session_pair(self.job_id, self.rp_ctx.session_id)

    def load(self, name: str, tag: str):
        return RollSite(name, tag, self)

    def init_job_session_pair(self, job_id, session_id):
        task_info = proxy_pb2.Task(model=proxy_pb2.Model(name=job_id, dataKey=bytes(session_id, encoding='utf8')))
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


ERROR_STATES = [proxy_pb2.STOP, proxy_pb2.KILL]
OBJECT_STORAGE_NAME = "__federation__"
CONF_KEY_LOCAL = "local"
CONF_KEY_SERVER = "servers"


class RollSite:
    def __init__(self, name: str, tag: str, rs_ctx: RollSiteContext):
        self.ctx = rs_ctx
        self.party_id = self.ctx.party_id
        self.dst_host = self.ctx.dst_host
        self.dst_port = self.ctx.dst_port
        self.job_id = self.ctx.job_id
        self.local_role = self.ctx.role
        self.name = name
        self.tag = tag
        self.stub = self.ctx.stub
        self.process_pool = ThreadPoolExecutor(10)
        self.complete_pool = ThreadPoolExecutor(10)
        #self.init_job_session_pair(self.job_id, self.ctx.rp_ctx.session_id)


    @staticmethod
    def __remote__object_key(*args):
        return "-".join(["{}".format(arg) for arg in args])

    
    def _thread_receive(self, packet, namespace, _tagged_key):
        try:
            is_standalone = self.ctx.rp_ctx.get_session().get_option(SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE) == "standalone"
            if is_standalone:
                status_rp = self.ctx.rp_ctx.load(namespace, STATUS_TABLE_NAME)

                while True:
                    ret_list = status_rp.get(_tagged_key)
                    if ret_list:
                        table_namespace = ret_list[2]
                        table_name = ret_list[1]
                        obj_type = ret_list[0]
                        break
                    time.sleep(0.1)
            else:
                ret_packet = self.stub.unaryCall(packet)
                while ret_packet.header.ack != 123:
                    if ret_packet.header.ack in ERROR_STATES:
                        raise IOError("receive terminated")
                    ret_packet = self.stub.unaryCall(packet)
                    time.sleep(0.1)
                obj_type = ret_packet.body.value 
                table_name = '{}-{}'.format(OBJECT_STORAGE_NAME, '-'.join([self.job_id, self.name, self.tag,
                                                                           ret_packet.header.src.role,
                                                                           ret_packet.header.src.partyId,
                                                                           ret_packet.header.dst.role,
                                                                           ret_packet.header.dst.partyId]))
                table_namespace = self.job_id

            rp = self.ctx.rp_ctx.load(namespace=table_namespace, name=table_name)
            if obj_type == b'object':
                ret_obj = rp.get(_tagged_key)
                return ret_obj
            else:
                return rp
        except:
            LOGGER.exception("thread recv error")
        finally:
            LOGGER.debug("done")


    def push(self, obj, parties: list = None):
        futures = []
        LOGGER.info("push parties:{}".format(parties))
        LOGGER.info("push session_id:{}".format(self.ctx.rp_ctx.session_id))
        for role_party_id in parties:
            # for _partyId in _partyIds:
            _role = role_party_id[0]
            _party_id = role_party_id[1]
            _tagged_key = self.__remote__object_key(self.job_id, self.name, self.tag, self.local_role, self.party_id,
                                                    _role,
                                                    _party_id)
            LOGGER.debug(f"_tagged_key:{_tagged_key}")
            namespace = self.job_id
            obj_type = 'rollpair' if isinstance(obj, RollPair) else 'object'

            if isinstance(obj, RollPair):
                rp = obj
            else:
                # If it is a object, put the object in the table and send the table meta.
                name = '{}-{}'.format(OBJECT_STORAGE_NAME, '-'.join([self.job_id, self.name, self.tag,
                                                                     self.local_role, str(self.party_id),
                                                                     _role, str(_party_id)]))

                rp = self.ctx.rp_ctx.load(namespace, name)
                rp.put(_tagged_key, obj)
                LOGGER.debug("[REMOTE] Sending {}".format(_tagged_key))

            def map_values(_tagged_key):
                is_standalone = self.ctx.rp_ctx.get_session().get_option(SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE) == "standalone"
                #is_standalone = True
                if is_standalone:
                    dst_name = '{}-{}'.format(OBJECT_STORAGE_NAME, '-'.join([self.job_id, self.name, self.tag,
                                                                             self.local_role, str(self.party_id),
                                                                            _role, str(_party_id)]))
                    store_type = rp.get_store_type()
                else:
                    dst_name = '{}-{}'.format(OBJECT_STORAGE_NAME, '-'.join([self.job_id, self.name,
                                                                             self.tag, self.local_role,
                                                                             str(self.party_id),
                                                                             _role, str(_party_id),
                                                                             self.dst_host,
                                                                             str(self.dst_port),
                                                                             obj_type]))
                    store_type = StoreTypes.ROLLPAIR_ROLLSITE

                LOGGER.info("namespace:{}".format(namespace))
                LOGGER.info("name:{}".format(dst_name))
                if is_standalone is False:
                    rp.map_values(
                        lambda v: v,
                        output=ErStore(store_locator=
                                       ErStoreLocator(store_type=store_type,
                                                      namespace=namespace,
                                                      name=dst_name)))

                if is_standalone:
                    status_rp = self.ctx.rp_ctx.load(namespace, STATUS_TABLE_NAME)

                    if isinstance(obj, RollPair):
                        LOGGER.debug(f"_tagged_key:{_tagged_key}")
                        LOGGER.debug(f"push:{obj_type},{rp.get_name()}, {rp.get_namespace()}")
                        status_rp.put(_tagged_key, (obj_type.encode("utf-8"), rp.get_name(), rp.get_namespace()))
                    else:
                        status_rp.put(_tagged_key, (obj_type.encode("utf-8"), dst_name, namespace))
                    _a = status_rp.get(_tagged_key)

                return role_party_id

            future = self.process_pool.submit(map_values, _tagged_key)
            futures.append(future)

        self.process_pool.shutdown(wait=False)

        return futures

    def wait_futures(self, futures):
        ret_future = self.complete_pool.submit(wait, futures, timeout=10, return_when=FIRST_EXCEPTION)
        self.complete_pool.shutdown(wait=False)
        return ret_future

    def pull(self, parties: list = None):
        futures = []
        for src_role, party_id in parties:
            _tagged_key = self.__remote__object_key(self.job_id, self.name, self.tag, src_role, str(party_id),
                                                    self.local_role, str(self.party_id))

            name = '{}-{}'.format(OBJECT_STORAGE_NAME, '-'.join([self.job_id, self.name, self.tag,
                                                                 src_role, str(party_id),
                                                                 self.local_role, str(self.party_id)]))

            LOGGER.info("pull _tagged_key: {}".format(_tagged_key))
            task_info = proxy_pb2.Task(taskId=name)
            topic_src = proxy_pb2.Topic(name="get_status", partyId="{}".format(party_id),
                                        role=src_role, callback=None)
            topic_dst = proxy_pb2.Topic(name="get_status", partyId="{}".format(self.party_id),
                                        role=self.local_role, callback=None)
            command_test = proxy_pb2.Command(name="get_status")
            conf_test = proxy_pb2.Conf(overallTimeout=1000,
                                       completionWaitTimeout=1000,
                                       packetIntervalTimeout=1000,
                                       maxRetries=10)

            metadata = proxy_pb2.Metadata(task=task_info,
                                          src=topic_src,
                                          dst=topic_dst,
                                          command=command_test,
                                          operator="getStatus",
                                          seq=0, ack=0,
                                          conf=conf_test)

            packet = proxy_pb2.Packet(header=metadata)
            namespace = self.job_id
            futures.append(self.process_pool.submit(RollSite._thread_receive, self, packet, namespace, _tagged_key))

        self.process_pool.shutdown(wait=False)
        return futures
