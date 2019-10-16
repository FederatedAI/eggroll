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
import _io

import grpc
import queue
from api.proto import proxy_pb2, proxy_pb2_grpc
from api.utils import file_utils
from api.utils.log_utils import getLogger

CONF_KEY_TARGET = "rollsite"
CONF_KEY_LOCAL = "local"
CONF_KEY_SERVER = "servers"

queue = queue.Queue()

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
    def __init__(self, job_id, party_id, role, runtime_conf, transfer_conf_path, host, port):
        self.trans_conf = file_utils.load_json_conf(transfer_conf_path)
        self.job_id = job_id
        self.party_id = party_id
        self.role = role
        self.runtime_conf = runtime_conf
        self.channel = grpc.insecure_channel(
            target="{}:{}".format(host, port),
            options=[('grpc.max_send_message_length', -1), ('grpc.max_receive_message_length', -1)])
        self.stub = proxy_pb2_grpc.DataTransferServiceStub(self.channel)
        self.tag = True
        #guest_list = pull("guest_list", host)  通过发布订阅机制获取的partyId,IP，port列表
        print("__init__")


    def generate_message(self, obj, metadata):
        print (type(obj))
        if isinstance(obj, _io.TextIOWrapper):
            print('-----1----')
            fp = obj
            content = fp.read(35)
            while True:
                print('-----2----')
                if not content:
                    content = 'finished'
                    data = proxy_pb2.Data(key="hello", value=content.encode())
                    metadata.command.name = 'finished'
                    metadata.seq += 1
                    packet = proxy_pb2.Packet(header=metadata, body=data)
                    yield packet
                    break
                else:
                    data = proxy_pb2.Data(key="hello", value=content.encode())
                    metadata.seq += 1
                    packet = proxy_pb2.Packet(header=metadata, body=data)
                    yield packet
                content = fp.read(35)
                print('----3-----')
        elif isinstance(obj, str):
            print('-----1##----')
            chunk_size = 10
            #full_iter = iter(obj)
            print(len(obj))
            begin = 0
            while True:
                #content = islice(full_iter, chunk_size)
                content = obj[begin:(begin + chunk_size)]
                print(content)
                data = proxy_pb2.Data(key="hello", value=content.encode())
                metadata.seq += 1
                packet = proxy_pb2.Packet(header=metadata, body=data)
                begin += chunk_size
                if begin > len(obj):
                    content = 'finished'
                    data = proxy_pb2.Data(key="hello", value=content.encode())
                    metadata.command.name = 'finished'
                    metadata.seq += 1
                    packet = proxy_pb2.Packet(header=metadata, body=data)
                    yield packet
                    break
                yield packet


    def __check_authorization(self, name, is_send=True):
        #name传进来的是*号，暂时不做判断
        if is_send and self.trans_conf.get('src') != self.role:
            print(self.role)
            raise ValueError("{} is not allow to send from {}".format(self.role))
        elif not is_send and self.role not in self.trans_conf.get('dst'):
            raise ValueError("{} is not allow to receive from {}".format(self.role))

    def __get_parties(self, role):
        return self.runtime_conf.get('role').get(role)

    #https://www.codercto.com/a/49586.html
    def push(self, obj, name: str, tag: str, role=None, idx=-1):
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
                task_info = proxy_pb2.Task(taskId="testTaskId", model=proxy_pb2.Model(name=name, dataKey="testKey"))
                topic_src = proxy_pb2.Topic(name=name, partyId="{}".format(self.party_id),
                                            role=self.role, callback=None)
                topic_dst = proxy_pb2.Topic(name=name, partyId="{}".format(_partyId),
                                            role=_role, callback=None)
                command_test = proxy_pb2.Command()
                conf_test = proxy_pb2.Conf(overallTimeout=2000,
                                           completionWaitTimeout=2000,
                                           packetIntervalTimeout=2000,
                                           maxRetries=10)
    
                metadata = proxy_pb2.Metadata(task=task_info,
                                              src=topic_src,
                                              dst=topic_dst,
                                              command=command_test,
                                              seq=0, ack=0,
                                              conf=conf_test)
                '''
                data = proxy_pb2.Data(key="hello", value=obj.encode())
                packet = proxy_pb2.Packet(header=metadata, body=data)
                queue.put(packet)
                print("cluster push!!!")
                self.stub.push(self.generate_message())
                '''
                self.stub.push(self.generate_message(obj, metadata))

    def unaryCall(self, obj):
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
                task_info = proxy_pb2.Task(taskId="testTaskId", model=proxy_pb2.Model(name="taskName", dataKey="testKey"))
                topic_src = proxy_pb2.Topic(name="test", partyId="{}".format(self.party_id),
                                            role="host", callback=None)
                topic_dst = proxy_pb2.Topic(name="test", partyId="10002",
                                            role="guest", callback=None)
                command_test = proxy_pb2.Command()
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
                self.stub.unaryCall(packet)


    def pull(self, obj, name: str):
        #先从本地取，判断返回值，不是complete状态,如果dst是local，从本地取
        #if  self.party_id != _partyId:
        #    self.unaryCall(name)

        #先发送unaryCall，等待对方push数据过来。

        #wait()等待完成条件，表示已经接收到结果，然后get结果。
        task_info = proxy_pb2.Task(taskId="testTaskId", model=proxy_pb2.Model(name=name, dataKey="testKey"))
        topic_src = proxy_pb2.Topic(name="test", partyId="{}".format(self.party_id),
                                    role="host", callback=None)
        topic_dst = proxy_pb2.Topic(name="test", partyId="{}".format(self.party_id),
                                    role="host", callback=None)
        command_test = proxy_pb2.Command()
        conf_test = proxy_pb2.Conf(overallTimeout=2000,
                                   completionWaitTimeout=2000,
                                   packetIntervalTimeout=2000,
                                   maxRetries=10)

        metadata = proxy_pb2.Metadata(task=task_info,
                                      src=topic_src,
                                      dst=topic_dst,
                                      command=command_test,
                                      seq=0, ack=0,
                                      conf=conf_test)
        ret_packets = self.stub.pull(metadata)
        ret_data = bytes(0)
        for packet in ret_packets:
            print(packet.body.value)
            print(packet.header.command.name)
            if packet.header.command.name == 'finished':
                return True
            ret_data = packet.body.value
            obj.write(ret_data.decode())
        if ret_data is b'':
            return None




