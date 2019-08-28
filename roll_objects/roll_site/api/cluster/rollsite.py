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

import grpc
import queue
from api.proto import proxy_pb2, proxy_pb2_grpc, basic_meta_pb2

queue = queue.Queue()


def init(job_id, runtime_conf, server_conf_path):
    #获取IP，端口号
    return RollSiteRuntime("localhost", 9370)


class RollSiteRuntime(object):
    def __init__(self, host, port):
        self.channel = grpc.insecure_channel(
            target="{}:{}".format(host, port),
            options=[('grpc.max_send_message_length', -1), ('grpc.max_receive_message_length', -1)])
        self.stub = proxy_pb2_grpc.DataTransferServiceStub(self.channel)
        self.tag = True

        print("__init__")

    def generate_message(self):
        while self.tag:
            packet_input = queue.get()
            #print(packet_input)
            self.tag = False
            yield proxy_pb2.Packet(header=packet_input.header, body=packet_input.body)


    #https://www.codercto.com/a/49586.html
    def push_sync(self, obj):
        task_info = proxy_pb2.Task(taskId="testTaskId", model=proxy_pb2.Model(name="taskName", dataKey="testKey"))
        topic_src = proxy_pb2.Topic(name="test", partyId="10001",
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
        queue.put(packet)
        self.stub.push(self.generate_message())

    def pull(self):
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
            response = stub.pull()


