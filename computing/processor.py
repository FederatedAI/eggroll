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

import argparse
import sys
import time
from concurrent import futures
from eggroll.api.utils import log_utils, cloudpickle, eggroll_serdes
import grpc
import lmdb
from cachetools import cached
from grpc._cython import cygrpc
from cachetools import LRUCache
from eggroll.api.proto import kv_pb2, processor_pb2, processor_pb2_grpc, storage_basic_pb2, basic_meta_pb2
from eggroll.api.proto import node_manager_pb2, node_manager_pb2_grpc
from enum import IntEnum
import os
import numpy as np
import hashlib
import threading
import socket

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_ONE_MIN_IN_SECONDS = 60

log_utils.setDirectory()
LOGGER = log_utils.getLogger()

PROCESS_RECV_FORMAT = "session {} method {} receive task: {}"

PROCESS_DONE_FORMAT = "session {} method {} done response: {}"
LMDB_MAP_SIZE = 16 * 4_096 * 244_140        # follows storage-service-cxx's config here
DEFAULT_DB = b'main'
DELIMETER = '-'
DELIMETER_ENCODED = DELIMETER.encode()


def generator(serdes: eggroll_serdes.ABCSerdes, cursor):
    for k, v in cursor:
        yield serdes.deserialize(k), serdes.deserialize(v)

class LogStage(IntEnum):
    START = 0
    END = 1

def gen_log(stage: LogStage, session, method, content):
    if stage == LogStage.START:
        template = PROCESS_RECV_FORMAT
    elif stage == LogStage.END:
        template = PROCESS_DONE_FORMAT
    else:
        raise ValueError('illegal stage')

    return template.format(session, method, content)

class Processor(processor_pb2_grpc.ProcessServiceServicer):

    def __init__(self, data_dir):
        self._serdes = eggroll_serdes.get_serdes()
        Processor.TEMP_DIR = os.sep.join([data_dir, 'in_memory '])
        Processor.LMDB_DIR = os.sep.join([data_dir, 'lmdb'])
        Processor.LEVEL_DB_DIR = os.sep.join([data_dir, 'level_db'])

    @cached(cache=LRUCache(maxsize=100))
    def get_function(self, function_bytes):
        eggroll_serdes.bytes_security_check(function_bytes)
        try:
            return cloudpickle.loads(function_bytes)
        except:
            import pickle
            return pickle._loads(function_bytes)

    def map(self, request, context):
        LOGGER.debug(gen_log(LogStage.START, request.session, 'map', request))
        task_info = request.info
        _mapper, _serdes = self.get_function_and_serdes(task_info)
        op = request.operand
        rtn = self.__create_output_storage_locator(op, task_info, request.session, False)

        src_db_path = Processor.get_path(op)
        dst_db_path = Processor.get_path(rtn)
        with MDBEnv(dst_db_path, create_if_missing=True) as dst_env, \
                MDBEnv(src_db_path, create_if_missing=True) as src_env:
            with src_env.begin(db=Processor.get_default_db(src_env)) as src_txn, \
                    dst_env.begin(db=Processor.get_default_db(dst_env), write=True) as dst_txn:
                cursor = src_txn.cursor()
                for k_bytes, v_bytes in cursor:
                    k, v = _serdes.deserialize(k_bytes), _serdes.deserialize(v_bytes)
                    k1, v1 = _mapper(k, v)
                    dst_txn.put(_serdes.serialize(k1), _serdes.serialize(v1))
                cursor.close()
        LOGGER.debug(gen_log(LogStage.END, request.session, 'map', rtn))
        return rtn

    def mapPartitions(self, request, context):
        LOGGER.debug(gen_log(LogStage.START, request.session, 'mapPartitions', request))
        task_info = request.info
        _mapper, _serdes = self.get_function_and_serdes(task_info)
        op = request.operand

        rtn = self.__create_output_storage_locator(op, task_info, request.session, True)

        src_db_path = Processor.get_path(op)
        dst_db_path = Processor.get_path(rtn)
        with MDBEnv(dst_db_path, create_if_missing=True) as dst_env, \
                MDBEnv(src_db_path, create_if_missing=True) as src_env:
            with src_env.begin(db=Processor.get_default_db(src_env)) as src_txn, \
                    dst_env.begin(db=Processor.get_default_db(dst_env), write=True) as dst_txn:
                cursor = src_txn.cursor()
                v = _mapper(generator(_serdes, cursor))
                if cursor.last():
                    k_bytes = cursor.key()
                    dst_txn.put(k_bytes, _serdes.serialize(v))
                cursor.close()
        LOGGER.debug(gen_log(LogStage.END, request.session, 'mapPartitions', rtn))
        return rtn

    def mapValues(self, request, context):
        LOGGER.debug(gen_log(LogStage.START, request.session, 'mapValues', request))
        task_info = request.info
        is_in_place_computing = self.__get_in_place_computing(request)

        _mapper, _serdes = self.get_function_and_serdes(task_info)
        op = request.operand

        rtn = self.__create_output_storage_locator(op, task_info, request.session, True)
        src_db_path = Processor.get_path(op)
        dst_db_path = Processor.get_path(rtn)

        with MDBEnv(dst_db_path, create_if_missing=True) as dst_env, \
                MDBEnv(src_db_path, create_if_missing=True) as src_env:
            with src_env.begin(db=Processor.get_default_db(src_env)) as src_txn, \
                    dst_env.begin(db=Processor.get_default_db(dst_env), write=True) as dst_txn:
                cursor = src_txn.cursor()
                for k_bytes, v_bytes in cursor:
                    v = _serdes.deserialize(v_bytes)

                    v1 = _mapper(v)
                    serialized_value = _serdes.serialize(v1)
                    dst_txn.put(k_bytes, serialized_value)
                cursor.close()
        LOGGER.debug(gen_log(LogStage.END, request.session, 'mapValues', rtn))
        return rtn

    def join(self, request, context):
        LOGGER.debug(gen_log(LogStage.START, request.session, 'join', request))
        task_info = request.info
        is_in_place_computing = self.__get_in_place_computing(request)
        _joiner, _serdes = self.get_function_and_serdes(task_info)
        left_op = request.left
        right_op = request.right

        rtn = self.__create_output_storage_locator(left_op, task_info, request.session, True)
        with MDBEnv(Processor.get_path(left_op), create_if_missing=True) as left_env, \
                MDBEnv(Processor.get_path(right_op), create_if_missing=True) as right_env, \
                MDBEnv(Processor.get_path(rtn), create_if_missing=True) as dst_env:
            small_env, big_env, is_swapped = self._rearrage_binary_envs(left_env, right_env)
            with small_env.begin(db=Processor.get_default_db(small_env)) as left_txn, \
                    big_env.begin(db=Processor.get_default_db(big_env)) as right_txn, \
                    dst_env.begin(db=Processor.get_default_db(dst_env), write=True) as dst_txn:
                cursor = left_txn.cursor()
                for k_bytes, v1_bytes in cursor:
                    v2_bytes = right_txn.get(k_bytes)
                    if v2_bytes is None:
                        if is_in_place_computing:
                            dst_txn.delete(k_bytes)
                        continue
                    v1 = _serdes.deserialize(v1_bytes)
                    v2 = _serdes.deserialize(v2_bytes)
                    v3 = self._run_user_binary_logic(_joiner, v1, v2, is_swapped)
                    dst_txn.put(k_bytes, _serdes.serialize(v3))
                cursor.close()
        LOGGER.debug(gen_log(LogStage.END, request.session, 'join', rtn))
        return rtn

    def reduce(self, request, context):
        LOGGER.debug(gen_log(LogStage.START, request.session, 'reduce', request))
        task_info = request.info

        _reducer, _serdes = self.get_function_and_serdes(task_info)
        op = request.operand
        value = None
        source_db_path = Processor.get_path(op)
        with MDBEnv(source_db_path, create_if_missing=True) as src_env:
            result_key_bytes = None
            with src_env.begin(db=Processor.get_default_db(src_env)) as src_txn:
                cursor = src_txn.cursor()
                for k_bytes, v_bytes in cursor:
                    v = _serdes.deserialize(v_bytes)
                    if value is None:
                        value = v
                    else:
                        value = _reducer(value, v)
                    result_key_bytes = k_bytes
            rtn = kv_pb2.Operand(key=result_key_bytes, value=_serdes.serialize(value))
            yield rtn
            LOGGER.debug(gen_log(LogStage.END, request.session, 'reduce', value))

    def glom(self, request, context):
        LOGGER.debug(gen_log(LogStage.START, request.session, 'glom', request))
        task_info = request.info

        op = request.operand
        _serdes = self._serdes
        src_db_path = Processor.get_path(op)
        rtn = self.__create_output_storage_locator(op, task_info, request.session, False)
        with MDBEnv(src_db_path, create_if_missing=True) as src_env, \
                MDBEnv(Processor.get_path(rtn), create_if_missing=True) as dst_env:
            with src_env.begin(db=Processor.get_default_db(src_env)) as src_txn, \
                    dst_env.begin(db=Processor.get_default_db(dst_env), write=True) as dst_txn:
                cursor = src_txn.cursor()
                v_list = []
                k_bytes = None
                for k, v in cursor:
                    v_list.append((_serdes.deserialize(k), _serdes.deserialize(v)))
                    k_bytes = k
                if k_bytes is not None:
                    dst_txn.put(k_bytes, _serdes.serialize(v_list))
        LOGGER.debug(gen_log(LogStage.END, request.session, 'glom', rtn))
        return rtn

    def sample(self, request, context):
        LOGGER.debug(gen_log(LogStage.START, request.session, 'send', request))
        task_info = request.info

        op = request.operand
        _func, _serdes = self.get_function_and_serdes(task_info)
        fraction, seed = _func()
        source_db_path = Processor.get_path(op)
        rtn = self.__create_output_storage_locator(op, task_info, request.session, False)

        with MDBEnv(Processor.get_path(rtn), create_if_missing=True) as dst_env, \
                MDBEnv(source_db_path, create_if_missing=True) as src_env:
            with src_env.begin(db=Processor.get_default_db(src_env)) as src_txn:
                with dst_env.begin(db=Processor.get_default_db(dst_env), write=True) as dst_txn:
                    cursor = src_txn.cursor()
                    cursor.first()
                    random_state = np.random.RandomState(seed)
                    for k, v in cursor:
                        if random_state.rand() < fraction:
                            dst_txn.put(k, v)
        LOGGER.debug(gen_log(LogStage.END, request.session, 'sample', rtn))
        return rtn

    def subtractByKey(self, request, context):
        LOGGER.debug(gen_log(LogStage.START, request.session, 'subtractByKey', request))
        task_info = request.info
        is_in_place_computing = self.__get_in_place_computing(request)
        left_op = request.left
        right_op = request.right
        rtn = self.__create_output_storage_locator(left_op, task_info, request.session, True)
        with MDBEnv(Processor.get_path(left_op), create_if_missing=True) as left_env, \
                MDBEnv(Processor.get_path(right_op), create_if_missing=True) as right_env, \
                MDBEnv(Processor.get_path(rtn), create_if_missing=True) as dst_env:
            with left_env.begin(db=Processor.get_default_db(left_env)) as left_txn, \
                    right_env.begin(db=Processor.get_default_db(right_env)) as right_txn, \
                    dst_env.begin(db=Processor.get_default_db(dst_env), write=True) as dst_txn:
                cursor = left_txn.cursor()
                for k_bytes, left_v_bytes in cursor:
                    right_v_bytes = right_txn.get(k_bytes)
                    if right_v_bytes is None:
                        if not is_in_place_computing:       # add to new table (not in-place)
                            dst_txn.put(k_bytes, left_v_bytes)
                    else:                                   # delete in existing table (in-place)
                        if is_in_place_computing:
                            dst_txn.delete(k_bytes)
                cursor.close()
        LOGGER.debug(gen_log(LogStage.END, request.session, 'subtractByKey', rtn))
        return rtn

    def filter(self, request, context):
        LOGGER.debug(gen_log(LogStage.START, request.session, 'filter', request))
        task_info = request.info
        is_in_place_computing = self.__get_in_place_computing(request)

        _filter, _serdes = self.get_function_and_serdes(task_info)
        op = request.operand

        rtn = self.__create_output_storage_locator(op, task_info, request.session, True)

        src_db_path = Processor.get_path(op)
        dst_db_path = Processor.get_path(rtn)
        with MDBEnv(dst_db_path, create_if_missing=True) as dst_env, \
                MDBEnv(src_db_path, create_if_missing=True) as src_env:
            with src_env.begin(db=Processor.get_default_db(src_env)) as src_txn, \
                    dst_env.begin(db=Processor.get_default_db(dst_env), write=True) as dst_txn:
                cursor = src_txn.cursor()
                for k_bytes, v_bytes in cursor:
                    k = _serdes.deserialize(k_bytes)
                    v = _serdes.deserialize(v_bytes)
                    if _filter(k, v):
                        if not is_in_place_computing:
                            dst_txn.put(k_bytes, v_bytes)
                    else:
                        if is_in_place_computing:
                            dst_txn.delete(k_bytes)
                cursor.close()
        LOGGER.debug(gen_log(LogStage.END, request.session, 'filter', rtn))
        return rtn

    def union(self, request, context):
        LOGGER.debug(gen_log(LogStage.START, request.session, 'union', request))
        task_info = request.info
        is_in_place_computing = self.__get_in_place_computing(request)
        _func, _serdes = self.get_function_and_serdes(task_info)
        left_op = request.left
        right_op = request.right

        rtn = self.__create_output_storage_locator(left_op, task_info, request.session, True)
        with MDBEnv(Processor.get_path(left_op), create_if_missing=True) as left_env, \
                MDBEnv(Processor.get_path(right_op), create_if_missing=True) as right_env, \
                MDBEnv(Processor.get_path(rtn), create_if_missing=True) as dst_env:
            with left_env.begin(db=Processor.get_default_db(left_env)) as left_txn, \
                    right_env.begin(db=Processor.get_default_db(right_env)) as right_txn, \
                    dst_env.begin(db=Processor.get_default_db(dst_env), write=True) as dst_txn:
                # process left op
                left_cursor = left_txn.cursor()
                for k_bytes, left_v_bytes in left_cursor:
                    right_v_bytes = right_txn.get(k_bytes)
                    if right_v_bytes is None:
                        if not is_in_place_computing:                           # add left-only to new table
                            dst_txn.put(k_bytes, left_v_bytes)
                    else:                                                       # update existing k-v
                        left_v = _serdes.deserialize(left_v_bytes)
                        right_v = _serdes.deserialize(right_v_bytes)
                        final_v = self._run_user_binary_logic(_func, left_v, right_v, False)
                        dst_txn.put(k_bytes, _serdes.serialize(final_v))
                left_cursor.close()

                # process right op
                right_cursor = right_txn.cursor()
                for k_bytes, right_v_bytes in right_cursor:
                    final_v_bytes = dst_txn.get(k_bytes)
                    if final_v_bytes is None:
                        dst_txn.put(k_bytes, right_v_bytes)
                right_cursor.close()
        LOGGER.debug(gen_log(LogStage.END, request.session, 'union', rtn))
        return rtn

    def flatMap(self, request, context):
        LOGGER.debug(gen_log(LogStage.START, request.session, 'flatMap', request))
        task_info = request.info

        _func, _serdes = self.get_function_and_serdes(task_info)
        op = request.operand
        rtn = self.__create_output_storage_locator(op, task_info, request.session, False)

        src_db_path = Processor.get_path(op)
        dst_db_path = Processor.get_path(rtn)
        with MDBEnv(dst_db_path, create_if_missing=True) as dst_env, \
                MDBEnv(src_db_path, create_if_missing=True) as src_env:
            with src_env.begin(db=Processor.get_default_db(src_env)) as src_txn, \
                    dst_env.begin(db=Processor.get_default_db(dst_env), write=True) as dst_txn:
                cursor = src_txn.cursor()
                for k_bytes, v_bytes in cursor:
                    k = _serdes.deserialize(k_bytes)
                    v = _serdes.deserialize(v_bytes)
                    map_result = _func(k, v)
                    for result_k, result_v in map_result:
                        dst_txn.put(_serdes.serialize(result_k), _serdes.serialize(result_v))
                cursor.close()
        LOGGER.debug(gen_log(LogStage.END, request.session, 'flatMap', rtn))
        return rtn

    def get_function_and_serdes(self, task_info: processor_pb2.TaskInfo):
        _function_bytes = task_info.function_bytes
        return self.get_function(_function_bytes), self._serdes

    """
    @staticmethod
    def get_environment(path, create_if_missing=True):
        if create_if_missing:
            os.makedirs(path, exist_ok=True)
        return lmdb.open(path, create=create_if_missing, max_dbs=1, sync=False, map_size=LMDB_MAP_SIZE)
    """

    @staticmethod
    def get_path(d_table: storage_basic_pb2.StorageLocator):
        return Processor._do_get_path(d_table.type, d_table.namespace, d_table.name, d_table.fragment)

    @staticmethod
    def _do_get_path(db_type, namespace, table, fragment):
        dir_prefix = ''
        if db_type == storage_basic_pb2.IN_MEMORY:
            dir_prefix = Processor.TEMP_DIR
        elif db_type == storage_basic_pb2.LMDB:
            dir_prefix = Processor.TEMP_DIR
        else:
            dir_prefix = Processor.LEVEL_DB_DIR

        path = os.sep.join([dir_prefix, namespace, table, str(fragment)])
        return path

    @staticmethod
    def get_default_db(env):
        return env.open_db(DEFAULT_DB)

    def _rearrage_binary_envs(self, left_env, right_env):
        """

        :param left_env:
        :param right_env:
        :return: small_env, big_env, is_swapped
        """
        with left_env.begin(db=Processor.get_default_db(left_env)) as left_txn, right_env.begin(db=Processor.get_default_db(right_env)) as right_txn:
            left_stat = left_txn.stat()
            right_stat = right_txn.stat()

            if left_stat['entries'] <= right_stat['entries']:
                return left_env, right_env, False
            else:
                return right_env, left_env, True

    def _run_user_binary_logic(self, func, left, right, is_swap):
        if is_swap:
            return func(right, left)
        else:
            return func(left, right)

    def __create_output_storage_locator(self, src_op, task_info, session, is_in_place_computing_effective):
        if is_in_place_computing_effective:
            if self.__get_in_place_computing_from_task_info(task_info):
                return src_op

        naming_policy = session.namingPolicy
        LOGGER.info('naming policy in processor: {}'.format(naming_policy))
        if naming_policy == 'ITER_AWARE':
            storage_name = DELIMETER.join([src_op.namespace, src_op.name, storage_basic_pb2.StorageType.Name(src_op.type)])
            name_ba = bytearray(storage_name.encode())
            name_ba.extend(DELIMETER_ENCODED)
            name_ba.extend(task_info.function_bytes)

            name = hashlib.md5(name_ba).hexdigest()
        else:
            name = task_info.function_id

        return storage_basic_pb2.StorageLocator(namespace=task_info.task_id, name=name,
                                                fragment=src_op.fragment,
                                                type=storage_basic_pb2.IN_MEMORY)

    def __get_in_place_computing(self, request):
        return self.__get_in_place_computing_from_task_info(request.info)

    def __get_in_place_computing_from_task_info(self, task_info):
        return task_info.isInPlaceComputing

class MDBEnv(object):
    env_lock = threading.Lock()
    env_dict = dict()
    count_dict = dict()

    def __init__(self, path, create_if_missing=True):
        with MDBEnv.env_lock:
            if path not in MDBEnv.env_dict:
                if create_if_missing:
                    os.makedirs(path, exist_ok=True)
                self.env = lmdb.open(path, create=create_if_missing, max_dbs=128, sync=False, map_size=LMDB_MAP_SIZE)

                MDBEnv.count_dict[path] = 0
                MDBEnv.env_dict[path] = self.env
            else:
                self.env = MDBEnv.env_dict[path]
            self.path = path
            MDBEnv.count_dict[path] = MDBEnv.count_dict[path] + 1

    def __enter__(self):
        return self.env

    def __exit__(self, exc_type, exc_val, exc_tb):
        with MDBEnv.env_lock:
            if self.env:
                count = MDBEnv.count_dict[self.path]
                if not count or count - 1 <= 0:
                    del MDBEnv.env_dict[self.path]
                    del MDBEnv.count_dict[self.path]
                else:
                    MDBEnv.count_dict[self.path] = count - 1
                self.env = None

    def __del__(self):
        with MDBEnv.env_lock:
            if self.env:
                count = MDBEnv.count_dict[self.path]
                if not count or count - 1 <= 0:
                    del MDBEnv.env_dict[self.path]
                    del MDBEnv.count_dict[self.path]
                else:
                    MDBEnv.count_dict[self.path] = count - 1
                self.env = None


def serve(args):
    port = args.port
    data_dir = args.data_dir
    node_manager = args.node_manager
    engine_addr = args.engine_addr
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1),
                         options=[(cygrpc.ChannelArgKey.max_send_message_length, -1),
                                  (cygrpc.ChannelArgKey.max_receive_message_length, -1)])
    LOGGER.info("server starting at {}, data_dir: {}".format(port, data_dir))
    processor = Processor(data_dir)
    processor_pb2_grpc.add_ProcessServiceServicer_to_server(
        processor, server)
    server.add_insecure_port("{}:{}".format(engine_addr, port))
    server.start()

    channel = grpc.insecure_channel(target=node_manager,
                                             options=[('grpc.max_send_message_length', -1),
                                                      ('grpc.max_receive_message_length', -1)])
    node_manager_stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)

    try:
        port = int(port)
        heartbeat_request = node_manager_pb2.HeartbeatRequest(endpoint=basic_meta_pb2.Endpoint(ip=engine_addr, port=port))
        while True:
            node_manager_stub.heartbeat(heartbeat_request)
            time.sleep(_ONE_MIN_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)
        sys.exit(0)


if __name__ == '__main__':
    from datetime import datetime
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--socket')
    parser.add_argument('-p', '--port', default=49999)
    parser.add_argument('-d', '--dir', default=os.path.dirname(os.path.realpath(__file__)))
    parser.add_argument('-m', '--node-manager', default="localhost:7888")
    parser.add_argument('-a', "--engine-addr", default="localhost")
    args = parser.parse_args()

    LOGGER.info("processor {}:{} started at {}, node-manager at {}".format(args.engine_addr, args.port, str(datetime.now()), args.node_manager))
    serve(args)
