# -*- coding: utf-8 -*-
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
import configparser
import gc
import os
import platform
import signal
import threading
import time

import grpc

from eggroll.core.client import NodeManagerClient
from eggroll.core.command.command_router import CommandRouter
from eggroll.core.command.command_service import CommandServicer
from eggroll.core.conf_keys import (
    SessionConfKeys,
    RollPairConfKeys,
    CoreConfKeys,
    NodeManagerConfKeys,
)
from eggroll.core.constants import ProcessorTypes, ProcessorStatus
from eggroll.core.datastructure import create_executor_pool
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErProcessor, ErEndpoint
from eggroll.core.proto import command_pb2_grpc, transfer_pb2_grpc, deepspeed_download_pb2_grpc
from eggroll.core.transfer.transfer_service import GrpcTransferServicer, GrpcDsDownloadServicer
from eggroll.core.utils import add_runtime_storage
from eggroll.core.utils import set_static_er_conf, get_static_er_conf
from eggroll.utils.log_utils import get_logger
from eggroll.utils.profile import get_system_metric

L = get_logger()


def serve(args):
    prefix = "v1/eggs-pair"
    service_name = f"{prefix}/runTask"

    from eggroll.roll_pair.tasks import EggTaskHandler

    CommandRouter.get_instance().register_handler(
        service_name=service_name,
        route_to_method=EggTaskHandler.run_task,
        route_to_call_based_class_instance=EggTaskHandler(args.data_dir),
    )

    max_workers = int(RollPairConfKeys.EGGROLL_ROLLPAIR_EGGPAIR_SERVER_EXECUTOR_POOL_MAX_SIZE.get())
    executor_pool_type = CoreConfKeys.EGGROLL_CORE_DEFAULT_EXECUTOR_POOL.get()
    command_server = grpc.server(
        create_executor_pool(
            canonical_name=executor_pool_type, max_workers=max_workers, thread_name_prefix="eggpair-command-server"
        ),
        options=[
            (
                "grpc.max_metadata_size",
                int(CoreConfKeys.EGGROLL_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_METADATA_SIZE.get()),
            ),
            (
                "grpc.max_send_message_length",
                int(CoreConfKeys.EGGROLL_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE.get()),
            ),
            (
                "grpc.max_receive_message_length",
                int(CoreConfKeys.EGGROLL_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE.get()),
            ),
            ("grpc.keepalive_time_ms", int(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIME_SEC.get()) * 1000),
            (
                "grpc.keepalive_timeout_ms",
                int(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIMEOUT_SEC.get()) * 1000,
            ),
            (
                "grpc.keepalive_permit_without_calls",
                int(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED.get()),
            ),
            (
                "grpc.per_rpc_retry_buffer_size",
                int(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_RETRY_BUFFER_SIZE.get()),
            ),
            ("grpc.so_reuseport", False),
        ],
    )

    command_servicer = CommandServicer()
    command_pb2_grpc.add_CommandServiceServicer_to_server(command_servicer, command_server)

    transfer_servicer = GrpcTransferServicer()
    ds_download_servicer = GrpcDsDownloadServicer()

    port = args.port
    transfer_port = args.transfer_port

    port = command_server.add_insecure_port(f"[::]:{port}")

    if transfer_port == "-1":
        transfer_server = command_server
        transfer_port = port
        transfer_pb2_grpc.add_TransferServiceServicer_to_server(transfer_servicer, transfer_server)
        deepspeed_download_pb2_grpc.add_DsDownloadServiceServicer_to_server(ds_download_servicer, transfer_server)
    else:
        transfer_server_max_workers = int(
            RollPairConfKeys.EGGROLL_ROLLPAIR_EGGPAIR_DATA_SERVER_EXECUTOR_POOL_MAX_SIZE.get()
        )
        transfer_server = grpc.server(
            create_executor_pool(
                canonical_name=executor_pool_type,
                max_workers=transfer_server_max_workers,
                thread_name_prefix="transfer_server",
            ),
            options=[
                (
                    "grpc.max_metadata_size",
                    int(CoreConfKeys.EGGROLL_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_METADATA_SIZE.get()),
                ),
                (
                    "grpc.max_send_message_length",
                    int(CoreConfKeys.EGGROLL_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE.get()),
                ),
                (
                    "grpc.max_receive_message_length",
                    int(CoreConfKeys.EGGROLL_CORE_GRPC_SERVER_CHANNEL_MAX_INBOUND_MESSAGE_SIZE.get()),
                ),
                (
                    "grpc.keepalive_time_ms",
                    int(CoreConfKeys.CONFKEY_CORE_GRPC_CHANNEL_KEEPALIVE_TIME_SEC.get()) * 1000,
                ),
                (
                    "grpc.keepalive_timeout_ms",
                    int(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_TIMEOUT_SEC.get()) * 1000,
                ),
                (
                    "grpc.keepalive_permit_without_calls",
                    int(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_KEEPALIVE_WITHOUT_CALLS_ENABLED.get()),
                ),
                (
                    "grpc.per_rpc_retry_buffer_size",
                    int(CoreConfKeys.CONFKEY_CORE_GRPC_SERVER_CHANNEL_RETRY_BUFFER_SIZE.get()),
                ),
                ("grpc.so_reuseport", False),
            ],
        )
        transfer_port = transfer_server.add_insecure_port(f"[::]:{transfer_port}")
        transfer_pb2_grpc.add_TransferServiceServicer_to_server(transfer_servicer, transfer_server)

        deepspeed_download_pb2_grpc.add_DsDownloadServiceServicer_to_server(ds_download_servicer, transfer_server)
        transfer_server.start()
    pid = os.getpid()

    L.info(f"starting egg_pair service, port: {port}, transfer port: {transfer_port}, pid: {pid}")
    command_server.start()

    cluster_manager = args.cluster_manager
    node_manager = args.node_manager
    myself = None
    # cluster_manager_client = None
    node_manager_client = None
    if cluster_manager:
        session_id = args.session_id
        server_node_id = int(args.server_node_id)
        static_er_conf = get_static_er_conf()
        static_er_conf["server_node_id"] = server_node_id

        if not session_id:
            raise ValueError("session id is missing")
        options = {SessionConfKeys.CONFKEY_SESSION_ID: args.session_id}
        myself = ErProcessor(
            id=int(args.processor_id),
            server_node_id=server_node_id,
            processor_type=ProcessorTypes.EGG_PAIR,
            command_endpoint=ErEndpoint(host="localhost", port=port),
            transfer_endpoint=ErEndpoint(host="localhost", port=transfer_port),
            pid=pid,
            options=options,
            status=ProcessorStatus.RUNNING,
        )

        cluster_manager_host, cluster_manager_port = cluster_manager.strip().split(":")

        L.info(f"egg_pair cluster_manager: {cluster_manager}")
        # cluster_manager_client = ClusterManagerClient(options={
        #     ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST: cluster_manager_host,
        #     ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT: cluster_manager_port
        # })

        node_manager_client = NodeManagerClient(
            options={
                NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HOST: "localhost",
                NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT: node_manager,
            }
        )

        # cluster_manager_client.heartbeat(myself)
        # node_manager_client.heartbeat(myself)
        add_runtime_storage("er_session_id", session_id)

        if platform.system() == "Windows":
            t1 = threading.Thread(target=stop_processor, args=[node_manager_client, myself])
            t1.start()

    L.info(f"egg_pair started at port={port}, transfer_port={transfer_port}")

    run = True

    def exit_gracefully(signum, frame):
        nonlocal run
        run = False
        L.info(
            f"egg_pair {args.processor_id} at port={port}, transfer_port={transfer_port}, pid={pid} receives signum={signal.getsignal(signum)}, stopping gracefully."
        )

    signal.signal(signal.SIGTERM, exit_gracefully)
    signal.signal(signal.SIGINT, exit_gracefully)

    while run:
        send_heartbeat(node_manager_client, myself)
        time.sleep(int(RollPairConfKeys.EGGROLL_ROLLPAIR_EGGPAIR_SERVER_HEARTBEAT_INTERVAL.get()))

    L.info(f"sending exit heartbeat to cm")
    if cluster_manager:
        myself._status = ProcessorStatus.STOPPED
        send_heartbeat(node_manager_client, myself)

    GrpcChannelFactory.shutdown_all_now()

    # todo:1: move to RocksdbAdapter and provide a cleanup method
    from eggroll.core.pair_store.rocksdb import RocksdbAdapter

    RocksdbAdapter.release_db_resource()
    L.info(f"closed RocksDB open dbs")

    gc.collect()

    L.info(f"system metric at exit: {get_system_metric(1)}")
    L.info(f"egg_pair {args.processor_id} at port={port}, transfer_port={transfer_port}, pid={pid} stopped gracefully")


def send_heartbeat(node_manager_client: NodeManagerClient, myself: ErProcessor):
    try:
        node_manager_client.heartbeat(myself)
    except Exception as e:
        L.exception(f"eggpair send heartbeat to nodemanager error")


if __name__ == "__main__":
    L.info(f"system metric at start: {get_system_metric(0.1)}")
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-d", "--data-dir")
    args_parser.add_argument("-cm", "--cluster-manager")
    args_parser.add_argument("-nm", "--node-manager")
    args_parser.add_argument("-s", "--session-id")
    args_parser.add_argument("-p", "--port", default="0")
    args_parser.add_argument("-t", "--transfer-port", default="0")
    args_parser.add_argument("-sn", "--server-node-id")
    args_parser.add_argument("-prid", "--processor-id", default="0")
    args_parser.add_argument("-c", "--config")

    args = args_parser.parse_args()

    EGGROLL_HOME = os.environ["EGGROLL_HOME"]
    configs = configparser.ConfigParser()
    if args.config:
        conf_file = args.config
        L.info(f"reading config path: {conf_file}")
    else:
        conf_file = f"{EGGROLL_HOME}/conf/eggroll.properties"
        L.info(f"reading default config: {conf_file}")

    configs.read(conf_file)
    set_static_er_conf(configs["eggroll"])

    if configs:
        if not args.data_dir:
            args.data_dir = configs["eggroll"]["eggroll.data.dir"]
            if not os.path.isabs(args.data_dir):
                args.data_dir = os.path.join(EGGROLL_HOME, args.data_dir)
                args.data_dir = os.path.realpath(args.data_dir)

    L.info(args)
    serve(args)
