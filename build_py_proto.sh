#!/usr/bin/env bash

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

BASEDIR=$(dirname "$0")/..
cd $BASEDIR



python -m grpc_tools.protoc -Ieggroll/proto --python_out=./eggroll/api/proto basic-meta.proto

python -m grpc_tools.protoc -Ieggroll/proto --python_out=./eggroll/api/proto computing-basic.proto

python -m grpc_tools.protoc -Ieggroll/proto --python_out=./eggroll/api/proto storage-basic.proto

python -m grpc_tools.protoc -Ieggroll/proto --python_out=./eggroll/api/proto --grpc_python_out=./eggroll/api/proto kv.proto

python -m grpc_tools.protoc -Ieggroll/proto --python_out=./eggroll/api/proto --grpc_python_out=./eggroll/api/proto processor.proto

python -m grpc_tools.protoc -Ieggroll/proto --python_out=./eggroll/api/proto --grpc_python_out=./eggroll/api/proto cluster-comm.proto

python -m grpc_tools.protoc -Ieggroll/proto --python_out=./eggroll/api/proto --grpc_python_out=./eggroll/api/proto proxy.proto

python -m grpc_tools.protoc -Ieggroll/proto --python_out=./eggroll/api/proto --grpc_python_out=./eggroll/api/proto node-manager.proto
