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

import argparse
import sys
import time
from concurrent import futures
from eggroll.api.utils import log_utils, cloudpickle
import grpc
import lmdb
from cachetools import cached
from grpc._cython import cygrpc
from eggroll.api.utils import eggroll_serdes
from cachetools import LRUCache
from eggroll.api.proto import kv_pb2, kv_pb2_grpc, processor_pb2, processor_pb2_grpc, storage_basic_pb2, node_manager_pb2, node_manager_pb2_grpc
from eggroll.api.proto import basic_meta_pb2
import os
import numpy as np
import hashlib
import threading
import traceback
from multiprocessing import Process,Queue
from collections import Iterable
from eggroll.computing.storage_adapters import LmdbAdapter, RocksdbAdapter, SkvNetworkAdapter


_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_ONE_MIN_IN_SECONDS = 60

log_utils.setDirectory()
LOGGER = log_utils.getLogger()

PROCESS_RECV_FORMAT = "method {} receive task: {}"

PROCESS_DONE_FORMAT = "method {} done response: {}"


CHUNK_PARALLEL = 0

def _exception_logger(func):
    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except:
            msg ="\n====detail start====\n" +  traceback.format_exc() + "\n====detail end====\n"
            LOGGER.error(msg)
            raise RuntimeError(msg)
    return wrapper

def generator(serds, cursor):
    for k, v in cursor:
        yield serds.deserialize(k), serds.deserialize(v)

class RollPairProcessor(processor_pb2_grpc.ProcessServiceServicer):
    # options:
    #   eggroll.storage.data_dir
    #   eggroll.storage.port
    def __init__(self, options):
        self.options = options

    def _run_user_binary_logic(self, func, left, right, is_swap):
        if is_swap:
            return func(right, left)
        else:
            return func(left, right)

    def _get_in_place_computing(self, request):
        return request.info.isInPlaceComputing

    def _create_adapter(self, storage_loc: storage_basic_pb2.StorageLocator, create_if_missing=True, via_network=False):
        adapter = None
        adapter_opts = {"create_if_missing": create_if_missing}
        if via_network:
            # local only
            adapter_opts["host"] = "127.0.0.1"
            adapter_opts["port"] = self.options["eggroll.storage.port"]
            adapter_opts["store_type"] = storage_basic_pb2.StorageType.Name(storage_loc.type)
            adapter_opts["name"] = storage_loc.name
            adapter_opts["namespace"] = storage_loc.namespace
            adapter_opts["fragment"] = str(storage_loc.fragment)
            adapter = SkvNetworkAdapter(adapter_opts)
        elif storage_loc.type == storage_basic_pb2.LEVEL_DB:
            LOGGER.info("rocksdb adapter")
            sub_data_dir = 'in_memory' if storage_loc.type == storage_basic_pb2.IN_MEMORY else "level_db" 
            adapter_opts["path"] = os.sep.join([self.options["eggroll.storage.data_dir"], sub_data_dir,
                                                storage_loc.namespace, storage_loc.name, str(storage_loc.fragment)])
            LOGGER.info("rocksdb path:{}".format(adapter_opts["path"]))
            adapter = RocksdbAdapter(adapter_opts)
        elif storage_loc.type in (storage_basic_pb2.IN_MEMORY, storage_basic_pb2.LMDB):
            LOGGER.info("lmdb adapter")
            sub_data_dir = 'in_memory' if storage_loc.type == storage_basic_pb2.IN_MEMORY else "lmdb"
            adapter_opts["path"] = os.sep.join([self.options["eggroll.storage.data_dir"], sub_data_dir,
                                                storage_loc.namespace, storage_loc.name, str(storage_loc.fragment)])
            adapter = LmdbAdapter(adapter_opts)
        else:
            raise NotImplementedError()
        LOGGER.info("create adapter:" + str(adapter) + "  " + str(adapter_opts))
        return adapter

    def _create_serde(self):
        return eggroll_serdes.get_serdes()

    def _create_output_storage_locator(self, src_op, task_info, process_conf, support_inplace):
        if support_inplace and task_info.isInPlaceComputing:
            return src_op
        name = task_info.function_id
        return storage_basic_pb2.StorageLocator(namespace=task_info.task_id, name=name,
                                                fragment=src_op.fragment,
                                                type=storage_basic_pb2.IN_MEMORY)

    def _create_functor(self, task_info: processor_pb2.TaskInfo):
        if task_info.function_bytes == b'blank':
            return None
        try:
            return cloudpickle.loads(task_info.function_bytes)
        except:
            import pickle
            return pickle._loads(task_info.function_bytes)

    def _method_wrapper(self,action, func, req, ctx, ret_loc):
        def process_wrapper(req_type, func, result, req):
            try:
                req_pb =  processor_pb2.UnaryProcess() if req_type == "UnaryProcess" else processor_pb2.BinaryProcess()
                req_pb.ParseFromString(req)
                #TODO context serialize?
                func(req_pb, None)
                result.put("ok")
            except :
                err_str = traceback.format_exc()
                LOGGER.error(err_str)
                result.put("error:" + err_str)
        req_type = req.DESCRIPTOR.name
        detail_info = [self, " ", action  , ",request:[task_id:" , req.info.task_id , ",function_id:",
                       req.info.function_id, ",function_bytes:", req.info.function_bytes[:100],
                       ",conf:" + str(req.conf).replace("\n"," ")]
        if req_type == "UnaryProcess":
            detail_info.append(str(req.operand).replace("\n", " "))
        elif req_type == "BinaryProcess":
            detail_info.append(str(req.left).replace("\n", " "))
            detail_info.append(str(req.right).replace("\n", " "))
        else:
            raise ValueError("unsupported:" + str(type(req)))
        detail_info = " ".join(str(n) for n in detail_info)
        LOGGER.info("grpc start:" + detail_info)
        if CHUNK_PARALLEL == 0:
            LOGGER.info("method wrapper")
            func(req, ctx)
        elif CHUNK_PARALLEL == 1:
            result = Queue()
            #TODO: grpc ctx?
            proc = Process(target=process_wrapper, args=(req_type, func, result, req.SerializeToString()))
            proc.start()
            proc.join()
            status = result.get()
            if status != "ok":
                raise RuntimeError(status)
        else:
            raise NotImplementedError()
        LOGGER.info("grpc end:" + detail_info + " ret_loc:" + str(ret_loc).replace("\n", " "))

    def _run_unary_unwrapper(self, action, func, req, context, support_inplace):
        dst_loc = self._create_output_storage_locator(req.operand, req.info, req.conf, support_inplace)
        is_inplace = req.info.isInPlaceComputing
        src_adapter, dst_adapter, src_serde, dst_serde, functor = self._create_adapter(req.operand, via_network=False), \
                                                        self._create_adapter(dst_loc, via_network=False), \
                                                        self._create_serde(),self._create_serde(), \
                                                        self._create_functor(req.info)
        with src_adapter as src_db, dst_adapter as dst_db:
            with src_db.iteritems() as src_it, dst_db.new_batch() as dst_wb:
                return func(src_it, dst_wb,src_serde, dst_serde, functor,is_inplace)

    def _run_unary(self, action, func, req, context, support_inplace):
        def run_unary_wrapper(req, context):
            is_inplace = req.info.isInPlaceComputing
            src_adapter, dst_adapter, src_serde, dst_serde, functor = self._create_adapter(req.operand, via_network=False), \
                                                            self._create_adapter(dst_loc, via_network=False), \
                                                            self._create_serde(),self._create_serde(), \
                                                            self._create_functor(req.info)
            with src_adapter as src_db, dst_adapter as dst_db:
                with src_db.iteritems() as src_it, dst_db.new_batch() as dst_wb:
                    LOGGER.info("src:{}, dst:{}".format(src_adapter, dst_adapter))
                    func(src_it, dst_wb,src_serde, dst_serde, functor,is_inplace)
        dst_loc = self._create_output_storage_locator(req.operand, req.info, req.conf, support_inplace)
        dst_loc.type = req.operand.type
        self._method_wrapper(action, run_unary_wrapper, req, context, dst_loc)
        return dst_loc

    def _run_binary(self, action, func, req,context, support_inplace):
        def run_binary_wrapper(req, context):
            is_inplace = req.info.isInPlaceComputing
            left_adapter, right_adapter, dst_adapter, left_serde, right_serde, dst_serde = \
                self._create_adapter(req.left), \
                self._create_adapter(req.right), \
                self._create_adapter(dst_loc), \
                self._create_serde(), self._create_serde(),self._create_serde()
            functor = self._create_functor(req.info)
            with left_adapter as left_db, right_adapter as right_db, dst_adapter as dst_db:
                with left_db.iteritems() as left_it, right_db.iteritems() as right_it, \
                        dst_db.new_batch() as dst_wb:
                    func(left_it, right_it, dst_wb, left_serde, right_serde, dst_serde, functor, is_inplace)
        dst_loc = self._create_output_storage_locator(req.left, req.info, req.conf, support_inplace)
        dst_loc.type = req.right.type 
        self._method_wrapper(action, run_binary_wrapper, req, context, dst_loc)
        return dst_loc

    @_exception_logger
    def mapValues(self, request, context):
        def mapValues_wrapper(src_it, dst_wb,src_serde, dst_serde, functor, is_in_place_computing):
            if is_in_place_computing:
                raise NotImplementedError()
            from inspect import signature
            sig = signature(functor)
            n = len(sig.parameters)
            if n == 1:
                for k_bytes, v_bytes in src_it:
                    v = functor(src_serde.deserialize(v_bytes))
                    dst_wb.put(k_bytes, dst_serde.serialize(v))
            elif n == 2:
                for k_bytes, v_bytes in src_it:
                    v = functor(src_serde.deserialize(k_bytes), src_serde.deserialize(v_bytes))
                    dst_wb.put(k_bytes, dst_serde.serialize(v))
            else:
                raise ValueError
        return self._run_unary("mapValues", mapValues_wrapper, request,context, True)

    @_exception_logger
    def map(self, request, context):
        def map_wrapper(src_it, dst_wb,src_serde, dst_serde, functor, is_in_place_computing):
            if is_in_place_computing:
                raise NotImplementedError()
            for k_bytes, v_bytes in src_it:
                k = src_serde.deserialize(k_bytes)
                v = src_serde.deserialize(v_bytes)
                k1, v1 = functor(k, v)
                dst_wb.put(dst_serde.serialize(k1), dst_serde.serialize(v1))
        return self._run_unary("map", map_wrapper, request,context, False)

    @_exception_logger
    def mapPartitions(self, request, context):
        def mapPartitions_wrapper(src_it, dst_wb,src_serde, dst_serde, functor, is_in_place_computing):
            if is_in_place_computing:
                raise NotImplementedError()
            v = functor(generator(src_serde, src_it))
            if src_it.last():
                k_bytes = src_it.key()
                dst_wb.put(k_bytes, dst_serde.serialize(v))
        return self._run_unary("mapPartitions", mapPartitions_wrapper, request,context, False)

    @_exception_logger
    def mapPartitions2(self, request, context):
        def mapPartitions2_wrapper(src_it, dst_wb,src_serde, dst_serde, functor, is_in_place_computing):
            if is_in_place_computing:
                raise NotImplementedError()
            v = functor(generator(src_serde, src_it))
            if src_it.last():
                if isinstance(v, Iterable):
                    for k1, v1 in v:
                        dst_wb.put(dst_serde.serialize(k1), dst_serde.serialize(v1))
                else:
                    k_bytes = src_it.key()
                    dst_wb.put(k_bytes, dst_serde.serialize(v))
        return self._run_unary("mapPartitions2", mapPartitions2_wrapper, request,context, False)

    @_exception_logger
    def flatMap(self, request, context):
        def flatMap_wrapper(src_it, dst_wb, src_serde, dst_serde, functor, is_in_place_computing):
            if is_in_place_computing:
                raise NotImplementedError()
            for k_bytes, v_bytes in src_it:
                for k2, v2 in functor(src_serde.deserialize(k_bytes), src_serde.deserialize(v_bytes)):
                    dst_wb.put(dst_serde.serialize(k2), dst_serde.serialize(v2))
        return self._run_unary("flatMap", flatMap_wrapper, request, context, False)

    @_exception_logger
    def union(self, request, context):
        def union_wrapper(left_it, right_it, dst_wb, left_serde, right_serde, dst_serde, functor, is_in_place_computing):
            if is_in_place_computing:
                raise NotImplementedError()
            for k_bytes, left_v_bytes in left_it:
                right_v_bytes = right_it.adapter.get(k_bytes)
                if right_v_bytes is None:
                    if not is_in_place_computing:                           # add left-only to new table
                        dst_wb.put(k_bytes, left_v_bytes)
                else:                                                       # update existing k-v
                    left_v = left_serde.deserialize(left_v_bytes)
                    right_v = right_serde.deserialize(right_v_bytes)
                    final_v = functor(left_v, right_v)
                    dst_wb.put(k_bytes, dst_serde.serialize(final_v))

            for k_bytes, right_v_bytes in right_it:
                final_v_bytes = dst_wb.adapter.get(k_bytes)
                if final_v_bytes is None:
                    dst_wb.put(k_bytes, right_v_bytes)
        return self._run_binary("union", union_wrapper, request, context, True)

    @_exception_logger
    def join(self, request, context):
        def join_wrapper(left_it, right_it, dst_wb, left_serde, right_serde, dst_serde, functor, is_in_place_computing):
            for k_bytes, left_v_bytes in left_it:
                right_v_bytes = right_it.adapter.get(k_bytes)
                if right_v_bytes is None:
                    if not is_in_place_computing:
                        dst_wb.delete(k_bytes, left_v_bytes)
                    continue
                left_v = left_serde.deserialize(left_v_bytes)
                right_v = right_serde.deserialize(right_v_bytes)
                v = functor(left_v, right_v)
                dst_wb.put(k_bytes, dst_serde.serialize(v))
        return self._run_binary("join", join_wrapper, request, context, True)

    @_exception_logger
    def subtractByKey(self, request, context):
        def subtractByKey_wrapper(left_it, right_it, dst_wb, left_serde, right_serde, dst_serde, functor, is_in_place_computing):
            for k_bytes, left_v_bytes in left_it:
                right_v_bytes = right_it.adapter.get(k_bytes)
                if right_v_bytes is None:
                    if not is_in_place_computing:
                        dst_wb.put(k_bytes, left_v_bytes)
                else:
                    if is_in_place_computing:
                        dst_wb.delete(k_bytes)           
        return self._run_binary("subtractByKey", subtractByKey_wrapper, request, context, True)

    @_exception_logger
    def reduce(self, request, context):
        def reduce_wrapper(src_it, dst_wb,src_serde, dst_serde, functor, is_in_place_computing):
            if is_in_place_computing:
                raise NotImplementedError()
            value = None
            result_key_bytes = None
            for k_bytes, v_bytes in src_it:
                v = src_serde.deserialize(v_bytes)
                if value is None:
                    value = v
                else:
                    value = functor(value, v)
                result_key_bytes = k_bytes
            return kv_pb2.Operand(key=result_key_bytes, value=dst_serde.serialize(value))
        rtn = self._run_unary_unwrapper("reduce", reduce_wrapper, request, context, False)

        yield rtn

    @_exception_logger
    def glom(self, request, context):
        
        def glom_wrapper(src_it, dst_wb, src_serde, dst_serde, functor, is_in_place_computing):
            k_tmp = None
            v_list = []
            for k_bytes, v_bytes in src_it:
                v_list.append((src_serde.deserialize(k_bytes), src_serde.deserialize(v_bytes)))
                k_tmp = k_bytes
            if k_tmp is not None:
                dst_wb.put(k_tmp, dst_serde.serialize(v_list))
        return self._run_unary("glom", glom_wrapper, request, context, False)
    
    @_exception_logger 
    def sample(self, request, context):
        def sample_wrapper(src_it, dst_wb, src_serde, dst_serde, functor, is_in_place_computing):
            fraction, seed = functor()
            src_it.first()
            random_state = np.random.RandomState(seed)
            for k_bytes, v_bytes in src_it:
                if random_state.rand() < fraction:
                    dst_wb.put(k_bytes, v_bytes)
        return self._run_unary("sample", sample_wrapper, request, context, False)
    
    @_exception_logger
    def filter(self, request, context):
        def filter_wrapper(src_it, dst_wb, src_serde, dst_serde, functor, is_in_place_computing):
            for k_bytes, v_bytes in src_it:
                k = src_serde.deserialize(k_bytes)
                v = src_serde.deserialize(v_bytes)
                if functor(k, v):
                    if not is_in_place_computing:
                        dst_wb.put(k_bytes, v_bytes)
                else:
                    if is_in_place_computing:
                        dst_wb.delete(k_bytes)
        return self._run_unary("filter", filter_wrapper, request, context, False)


def serve(args):
    port = args.port
    data_dir = args.data_dir
    node_manager = args.node_manager
    engine_addr = args.engine_addr
    socket = args.socket

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1),
                         options=[(cygrpc.ChannelArgKey.max_send_message_length, -1),
                                  (cygrpc.ChannelArgKey.max_receive_message_length, -1)])
    opts = {"eggroll.storage.data_dir": data_dir, "eggroll.storage.port":"20106"}
    processor = RollPairProcessor(opts)
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
    parser.add_argument('-p', '--port', default=7888)
    parser.add_argument('-d', '--data-dir', default=os.path.dirname(os.path.realpath(__file__)))
    parser.add_argument('-m', '--node-manager', default="localhost:7888")
    parser.add_argument('-a', "--engine-addr", default="localhost")
    args = parser.parse_args()
    LOGGER.info("started at {}".format(str(datetime.now())))
    serve(args) 

