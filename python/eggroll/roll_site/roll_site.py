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

import functools
import time
from concurrent.futures import ThreadPoolExecutor

from eggroll.core.conf_keys import SessionConfKeys, RollSiteConfKeys
from eggroll.core.constants import DeployModes
from eggroll.core.constants import StoreTypes
from eggroll.core.error import GrpcCallError
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErStoreLocator, ErStore
from eggroll.core.proto import proxy_pb2, proxy_pb2_grpc
from eggroll.core.serdes import eggroll_serdes
from eggroll.core.transfer_model import ErRollSiteHeader
from eggroll.core.utils import _stringify
from eggroll.core.utils import to_one_line_string
from eggroll.roll_pair.roll_pair import RollPair, RollPairContext
from eggroll.roll_site.utils.roll_site_utils import create_store_name, DELIM
from eggroll.utils import log_utils

L = log_utils.get_logger()
P = log_utils.get_logger('profile')
_serdes = eggroll_serdes.PickleSerdes

STATUS_TABLE_NAME = "__roll_site_standalone_status__"


class RollSiteContext:
    grpc_channel_factory = GrpcChannelFactory()

    def __init__(self, roll_site_session_id, rp_ctx: RollPairContext, options: dict = None):
        if options is None:
            options = {}
        self.roll_site_session_id = roll_site_session_id
        self.rp_ctx = rp_ctx

        self.role = options["self_role"]
        self.party_id = str(options["self_party_id"])
        self.proxy_endpoint = options["proxy_endpoint"]
        self.is_standalone = self.rp_ctx.get_session().get_option(SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE) == "standalone"
        if self.is_standalone:
            self.stub = None
        else:
            channel = self.grpc_channel_factory.create_channel(self.proxy_endpoint)
            self.stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
            self.init_job_session_pair(self.roll_site_session_id, self.rp_ctx.session_id)

        self.pushing_task_count = 0
        self.rp_ctx.get_session().add_exit_task(self.push_complete)

        L.info(f"inited RollSiteContext: {self.__dict__}")

    def push_complete(self):
        session_id = self.rp_ctx.get_session().get_session_id()
        L.info(f"running roll site exit func for er session: {session_id}, roll site session id: {self.roll_site_session_id}")
        try_count = 0
        max_try_count = 800
        while True:
            if try_count >= max_try_count:
                L.warn(f"try times reach {max_try_count} for session: {session_id}, exiting")
                return
            if self.pushing_task_count:
                L.info(f"session: {session_id} "
                       f"waiting for all push tasks complete. "
                       f"current try_count: {try_count}, "
                       f"current pushing task count: {self.pushing_task_count}")
                try_count += 1
                time.sleep(min(0.1 * try_count, 60))
            else:
                L.info(f"session: {session_id} finishes all pushing tasks")
                return

    # todo:1: add options?
    def load(self, name: str, tag: str, options: dict = None):
        if options is None:
            options = {}
        return RollSite(name, tag, self, options=options)

    # todo:1: try-except as decorator
    def init_job_session_pair(self, roll_site_session_id, er_session_id):
        try:
            task_info = proxy_pb2.Task(model=proxy_pb2.Model(name=roll_site_session_id, dataKey=bytes(er_session_id, encoding='utf8')))
            topic_src = proxy_pb2.Topic(name="init_job_session_pair", partyId=self.party_id,
                                        role=self.role, callback=None)
            topic_dst = proxy_pb2.Topic(name="init_job_session_pair", partyId=self.party_id,
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
                                          seq=0,
                                          ack=0)
            packet = proxy_pb2.Packet(header=metadata)

            self.stub.unaryCall(packet)
            L.info(f"send RollSiteContext init to Proxy: {to_one_line_string(packet)}")
        except Exception as e:
            raise GrpcCallError("init_job_session_pair", self.proxy_endpoint, e)


ERROR_STATES = [proxy_pb2.STOP, proxy_pb2.KILL]
OBJECT_STORAGE_NAME = "__federation__"
CONF_KEY_TARGET = "rollsite"
CONF_KEY_LOCAL = "local"
CONF_KEY_SERVER = "servers"


class RollSite:
    receive_exeutor_pool = None
    def __init__(self, name: str, tag: str, rs_ctx: RollSiteContext, options: dict = None):
        if options is None:
            options = {}
        self.ctx = rs_ctx
        self.party_id = self.ctx.party_id
        self.dst_host = self.ctx.proxy_endpoint._host
        self.dst_port = self.ctx.proxy_endpoint._port
        self.roll_site_session_id = self.ctx.roll_site_session_id
        self.local_role = self.ctx.role
        self.name = name
        self.tag = tag
        self.stub = self.ctx.stub
        if RollSite.receive_exeutor_pool is None:
            receive_executor_pool_size = int(RollSiteConfKeys.EGGROLL_ROLLSITE_RECEIVE_EXECUTOR_POOL_MAX_SIZE.get_with(options))
            RollSite.receive_exeutor_pool = ThreadPoolExecutor(receive_executor_pool_size, thread_name_prefix="rollsite-receive")
        self._push_start_wall_time = None
        self._push_start_cpu_time = None
        self._pull_start_wall_time = None
        self._pull_start_cpu_time = None
        self._is_standalone = self.ctx.rp_ctx.get_session().get_option(
                SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE) == DeployModes.STANDALONE
        L.info(f'inited RollSite. my party id: {self.ctx.party_id}. proxy endpoint: {self.dst_host}:{self.dst_port}')

    def _push_callback(self, fn, tmp_rp):
        #if tmp_rp:
        #tmp_rp.destroy()
        if self.ctx.pushing_task_count <= 0:
            raise ValueError(f'pushing task count <= 0: {tmp_rp}')
        self.ctx.pushing_task_count -= 1
        end_wall_time = time.time()
        end_cpu_time = time.perf_counter()

        P.info(f'{{"metric_type": "func_profile", "qualname": "RollSite.push", "cpu_time": {end_cpu_time - self._push_start_cpu_time}, "wall_time": {end_wall_time - self._push_start_wall_time}}}')

    def _thread_receive(self, packet, namespace, roll_site_header: ErRollSiteHeader):
        try:
            table_name = create_store_name(roll_site_header)
            if self._is_standalone:
                status_rp = self.ctx.rp_ctx.load(namespace, STATUS_TABLE_NAME + DELIM + self.ctx.roll_site_session_id)
                retry_cnt = 0
                # TODO:0: sleep retry count and timeout
                while True:
                    msg = f"retry pull: retry_cnt: {retry_cnt}," + \
                          f" tagged_key: '{table_name}', packet: {to_one_line_string(packet)}, namespace: {namespace}"
                    if retry_cnt % 10 == 0:
                        L.info(msg)
                    else:
                        L.debug(msg)
                    retry_cnt += 1
                    ret_list = status_rp.get(table_name)
                    if ret_list:
                        table_namespace = ret_list[2]
                        table_name = ret_list[1]
                        obj_type = ret_list[0]
                        break
                    time.sleep(min(0.1 * retry_cnt, 30))
            else:
                retry_cnt = 0
                ret_packet = self.stub.unaryCall(packet)
                while ret_packet.header.ack != 123:
                    msg = f"retry pull: retry_cnt: {retry_cnt}," + \
                          f" store_name: '{table_name}', packet: {to_one_line_string(packet)}, namespace: {namespace}"
                    if retry_cnt % 10 == 0:
                        L.info(msg)
                    else:
                        L.debug(msg)
                    retry_cnt += 1
                    if ret_packet.header.ack in ERROR_STATES:
                        raise IOError("receive terminated")
                    ret_packet = self.stub.unaryCall(packet)
                    time.sleep(min(0.1 * retry_cnt, 30))
                obj_type = ret_packet.body.value

                table_namespace = self.roll_site_session_id
            L.info(f"pull status done: table_name:{table_name}, packet:{to_one_line_string(packet)}, namespace:{namespace}")
            rp = self.ctx.rp_ctx.load(namespace=table_namespace, name=table_name)
            success_msg_prefix = f'RollSite.Pull: pull {roll_site_header} success.'
            if obj_type == b'object':
                result = rp.get(table_name)
                if result is not None:
                    empty = "NOT empty"
                else:
                    empty = "empty"
                if not self._is_standalone:
                    rp.destroy()
                L.info(f"{success_msg_prefix} type: {type(result)}, empty_or_not: {empty}")
            else:
                result = rp
                L.info(f"{success_msg_prefix} type: {obj_type}, count: {rp.count()}")
            return result
        except Exception as e:
            L.exception(f"pull error:{e}")
            raise GrpcCallError("push", self.ctx.proxy_endpoint, e)
        finally:
            end_wall_time = time.time()
            end_cpu_time = time.perf_counter()

            P.info(f'{{"metric_type": "func_profile", "qualname": "RollSite.pull", "cpu_time": {end_cpu_time - self._pull_start_cpu_time}, "wall_time": {end_wall_time - self._pull_start_wall_time}}}')

    def push(self, obj, parties: list = None):
        L.info(f"pushing: self:{self.__dict__}, obj_type:{type(obj)}, parties:{parties}")
        self._push_start_wall_time = time.time()
        self._push_start_cpu_time = time.perf_counter()
        futures = []
        for role_party_id in parties:
            self.ctx.pushing_task_count += 1
            _role = role_party_id[0]
            _party_id = str(role_party_id[1])

            _options = {}
            obj_type = 'rollpair' if isinstance(obj, RollPair) else 'object'
            roll_site_header = ErRollSiteHeader(
                roll_site_session_id=self.roll_site_session_id,
                name=self.name,
                tag=self.tag,
                src_role=self.local_role,
                src_party_id=self.party_id,
                dst_role=_role,
                dst_party_id=_party_id,
                data_type=obj_type,
                options=_options)
            _tagged_key = create_store_name(roll_site_header)
            L.debug(f"pushing start party:{type(obj)}, {_tagged_key}")
            namespace = self.roll_site_session_id

            if isinstance(obj, RollPair):
                rp = obj
            else:
                rp = self.ctx.rp_ctx.load(namespace, _tagged_key)
                rp.put(_tagged_key, obj)
            rp.disable_gc()
            L.info(f"pushing prepared: {type(obj)}, tag_key:{_tagged_key}")

            def map_values(_tagged_key, is_standalone, roll_site_header):
                if is_standalone:
                    dst_name = _tagged_key
                    store_type = rp.get_store_type()
                else:
                    dst_name = DELIM.join([_tagged_key,
                                           self.dst_host,
                                           str(self.dst_port),
                                           obj_type])
                    store_type = StoreTypes.ROLLPAIR_ROLLSITE
                if is_standalone:
                    status_rp = self.ctx.rp_ctx.load(namespace, STATUS_TABLE_NAME + DELIM + self.roll_site_session_id, options=_options)
                    status_rp.disable_gc()
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

                    # TODO:0: move options from job to store when database modification finished

                    options = {"roll_site_header": roll_site_header,
                               "proxy_endpoint": self.ctx.proxy_endpoint,
                               "obj_type": obj_type}

                    if isinstance(obj, RollPair):
                        roll_site_header._options['total_partitions'] = obj.get_store()._store_locator._total_partitions
                        L.info(f"RollSite.push: pushing {roll_site_header}, type: RollPair, count: {obj.count()}")
                    else:
                        L.info(f"RollSite.push: pushing {roll_site_header}, type: object")
                    rp.map_values(lambda v: v,
                                  output=ErStore(store_locator=new_store_locator),
                                  options=options)

                L.info(f"RollSite.push: push {roll_site_header} done. type:{type(obj)}")
                return _tagged_key

            future = RollSite.receive_exeutor_pool.submit(map_values, _tagged_key, self._is_standalone, roll_site_header)
            if not self._is_standalone and (obj_type == 'object' or obj_type == b'object'):
                tmp_rp = rp
            else:
                tmp_rp = None

            future.add_done_callback(functools.partial(self._push_callback, tmp_rp=tmp_rp))
            futures.append(future)

        return futures

    # def wait_futures(self, futures):
    #     # TODO:0: configurable
    #     ret_future = self.complete_executor_pool.submit(wait, futures, timeout=1000, return_when=FIRST_EXCEPTION)
    #     self.complete_executor_pool.shutdown(wait=False)
    #     return ret_future

    def pull(self, parties: list = None):
        self._pull_start_wall_time = time.time()
        self._pull_start_cpu_time = time.perf_counter()
        futures = []
        for src_role, src_party_id in parties:
            src_party_id = str(src_party_id)
            roll_site_header = ErRollSiteHeader(
                roll_site_session_id=self.roll_site_session_id,
                name=self.name,
                tag=self.tag,
                src_role=src_role,
                src_party_id=src_party_id,
                dst_role=self.local_role,
                dst_party_id=self.party_id)
            _tagged_key = create_store_name(roll_site_header)

            name = _tagged_key

            model = proxy_pb2.Model(name=_stringify(roll_site_header))
            task_info = proxy_pb2.Task(taskId=name, model=model)
            topic_src = proxy_pb2.Topic(name="get_status", partyId=src_party_id,
                                        role=src_role, callback=None)
            topic_dst = proxy_pb2.Topic(name="get_status", partyId=self.party_id,
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
            namespace = self.roll_site_session_id
            L.info(f"pulling prepared tagged_key: {_tagged_key}, packet:{to_one_line_string(packet)}")
            futures.append(RollSite.receive_exeutor_pool.submit(RollSite._thread_receive, self, packet, namespace, roll_site_header))

        return futures
