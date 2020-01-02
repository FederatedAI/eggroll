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

#BASEDIR=$(dirname "$0")/..
#cd $BASEDIR

PROTO_PATH="./core"
OUTPUT_PATH="../python/eggroll/core/proto"

echo "proto_path: ${PROTO_PATH}"
echo "output_path: ${OUTPUT_PATH}"
mkdir -p ${OUTPUT_PATH}

for file in `ls ${PROTO_PATH}`; do
  echo "file: ${file}"
  python -m grpc_tools.protoc -I${PROTO_PATH} --python_out=${OUTPUT_PATH} --grpc_python_out=${OUTPUT_PATH} ${PROTO_PATH}/${file}
done
