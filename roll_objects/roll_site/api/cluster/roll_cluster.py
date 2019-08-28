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

import asyncio
import concurrent

import grpc

from api.proto import basic_meta_pb2, proxy_pb2, proxy_pb2_grpc


class roll_cluster():
  def init(name_space, role_conf_path):

    #global LOGGER
    #LOGGER = getLogger()


  def push_sync(data, pub_key, tag):
    #根据pub_key获取对端IP和端口号？pub_key的格式是什么样子的？
    with grpc.insecure_channel('localhost:8888') as channel:
        stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
        response = stub.push(data)


  def pull(pub_key, tag, idx=-1):
    with grpc.insecure_channel('localhost:50051') as channel:
      stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
      response = stub.pull(data)


