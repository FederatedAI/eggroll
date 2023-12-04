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
import gc
import logging
import os
import pathlib
import signal
import threading

import grpc

from eggroll.config import Config, ConfigKey
from eggroll.core.client import NodeManagerClient
from eggroll.core.command.command_router import CommandRouter
from eggroll.core.command.command_service import CommandServicer
from eggroll.core.constants import ProcessorTypes, ProcessorStatus
from eggroll.core.datastructure import create_executor_pool
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErProcessor, ErEndpoint
from eggroll.core.proto import (
    command_pb2_grpc,
    transfer_pb2_grpc,
    deepspeed_download_pb2_grpc,
)
from eggroll.core.transfer.transfer_service import (
    GrpcTransferServicer,
    GrpcDsDownloadServicer,
)
from eggroll.trace import get_system_metric

L = logging.getLogger(__name__)


def serve(
    config: Config,
    data_dir: str,
    port: int,
    transfer_port: int,
    cluster_manager,
    node_manager,
    session_id: str,
    server_node_id: int,
    processor_id: int,
):
    # register tasks
    from eggroll.computing import tasks

    env_options = tasks.EnvOptions(data_dir=data_dir, config=config)
    tasks.register(CommandRouter.get_instance(), env_options)

    # start command server
    max_workers = config.eggroll.rollpair.eggpair.server.executor.pool.max.size
    executor_pool_type = config.eggroll.core.default.executor.pool
    command_server = grpc.server(
        create_executor_pool(
            canonical_name=executor_pool_type,
            max_workers=max_workers,
            thread_name_prefix="eggpair-command-server",
        ),
        options=[
            (
                "grpc.max_metadata_size",
                config.eggroll.core.grpc.server.channel.max.inbound.metadata.size,
            ),
            (
                "grpc.max_send_message_length",
                config.eggroll.core.grpc.server.channel.max.inbound.message.size,
            ),
            (
                "grpc.max_receive_message_length",
                config.eggroll.core.grpc.server.channel.max.inbound.message.size,
            ),
            (
                "grpc.keepalive_time_ms",
                config.eggroll.core.grpc.channel.keepalive.time.sec * 1000,
            ),
            (
                "grpc.keepalive_timeout_ms",
                config.eggroll.core.grpc.channel.keepalive.timeout.sec * 1000,
            ),
            (
                "grpc.keepalive_permit_without_calls",
                int(
                    config.eggroll.core.grpc.channel.keepalive.permit.without.calls.enabled
                ),
            ),
            (
                "grpc.per_rpc_retry_buffer_size",
                config.eggroll.core.grpc.server.channel.retry.buffer.size,
            ),
            ("grpc.so_reuseport", False),
        ],
    )

    command_servicer = CommandServicer()
    command_pb2_grpc.add_CommandServiceServicer_to_server(
        command_servicer, command_server
    )

    transfer_servicer = GrpcTransferServicer()
    ds_download_servicer = GrpcDsDownloadServicer(config=config)

    port = command_server.add_insecure_port(f"[::]:{port}")

    if transfer_port == "-1":
        transfer_server = command_server
        transfer_port = port
        transfer_pb2_grpc.add_TransferServiceServicer_to_server(
            transfer_servicer, transfer_server
        )
        deepspeed_download_pb2_grpc.add_DsDownloadServiceServicer_to_server(
            ds_download_servicer, transfer_server
        )
    else:
        transfer_server_max_workers = (
            config.eggroll.rollpair.data.server.executor.pool.max.size
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
                    config.eggroll.core.grpc.server.channel.max.inbound.metadata.size,
                ),
                (
                    "grpc.max_send_message_length",
                    config.eggroll.core.grpc.server.channel.max.inbound.message.size,
                ),
                (
                    "grpc.max_receive_message_length",
                    config.eggroll.core.grpc.server.channel.max.inbound.message.size,
                ),
                (
                    "grpc.keepalive_time_ms",
                    config.eggroll.core.grpc.channel.keepalive.time.sec * 1000,
                ),
                (
                    "grpc.keepalive_timeout_ms",
                    config.eggroll.core.grpc.channel.keepalive.timeout.sec * 1000,
                ),
                (
                    "grpc.keepalive_permit_without_calls",
                    int(
                        config.eggroll.core.grpc.channel.keepalive.permit.without.calls.enabled
                    ),
                ),
                (
                    "grpc.per_rpc_retry_buffer_size",
                    config.eggroll.core.grpc.server.channel.retry.buffer.size,
                ),
                ("grpc.so_reuseport", False),
            ],
        )
        transfer_port = transfer_server.add_insecure_port(f"[::]:{transfer_port}")
        transfer_pb2_grpc.add_TransferServiceServicer_to_server(
            transfer_servicer, transfer_server
        )

        deepspeed_download_pb2_grpc.add_DsDownloadServiceServicer_to_server(
            ds_download_servicer, transfer_server
        )
        transfer_server.start()
    pid = os.getpid()

    L.info(
        f"starting egg_pair service, port: {port}, transfer port: {transfer_port}, pid: {pid}"
    )
    command_server.start()

    myself = None
    # cluster_manager_client = None
    node_manager_client = None
    if cluster_manager:
        env_options.server_node_id = server_node_id

        if not session_id:
            raise ValueError("session id is missing")
        options = {ConfigKey.eggroll.session.id: session_id}
        myself = ErProcessor(
            id=processor_id,
            server_node_id=server_node_id,
            processor_type=ProcessorTypes.EGG_PAIR,
            command_endpoint=ErEndpoint(host="localhost", port=port),
            transfer_endpoint=ErEndpoint(host="localhost", port=transfer_port),
            pid=pid,
            options=options,
            status=ProcessorStatus.RUNNING,
        )
        L.info(f"egg_pair cluster_manager: {cluster_manager}")
        node_manager_client = NodeManagerClient(
            config=config,
            host="localhost",
            port=node_manager,
        )

    L.info(f"egg_pair started at port={port}, transfer_port={transfer_port}")

    poison = threading.Event()

    def exit_gracefully(signum, frame):
        if cluster_manager:
            myself._status = ProcessorStatus.STOPPED
            send_heartbeat(node_manager_client, myself)
        L.info(
            f"egg_pair {processor_id} at port={port}, transfer_port={transfer_port}, pid={pid} receives signum={signal.getsignal(signum)}, stopping gracefully."
        )
        poison.set()

    signal.signal(signal.SIGTERM, exit_gracefully)
    signal.signal(signal.SIGINT, exit_gracefully)

    while not poison.is_set():
        send_heartbeat(node_manager_client, myself)
        # L.info(f"system metric: {get_system_metric(1)}")
        poison.wait(config.eggroll.rollpair.eggpair.server.heartbeat.interval)

    L.info(f"sending exit heartbeat to cm")
    if cluster_manager:
        myself._status = ProcessorStatus.STOPPED
        send_heartbeat(node_manager_client, myself)

    GrpcChannelFactory.shutdown_all_now()

    gc.collect()

    L.info(f"system metric at exit: {get_system_metric(1)}")
    L.info(
        f"egg_pair {processor_id} at port={port}, transfer_port={transfer_port}, pid={pid} stopped gracefully"
    )


def send_heartbeat(node_manager_client: NodeManagerClient, myself: ErProcessor):
    try:
        node_manager_client.heartbeat(myself)
    except Exception as e:
        L.exception(f"eggpair send heartbeat to nodemanager error")


def main():
    L.info(f"system metric at start: {get_system_metric(0.1)}")
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-d", "--data-dir")
    args_parser.add_argument("-cm", "--cluster-manager", required=True)
    args_parser.add_argument("-nm", "--node-manager", type=int, required=True)
    args_parser.add_argument("-s", "--session-id", type=str, required=True)
    args_parser.add_argument("-p", "--port", type=int, default=0)
    args_parser.add_argument("-t", "--transfer-port", type=int, default=0)
    args_parser.add_argument("-sn", "--server-node-id", type=int, required=True)
    args_parser.add_argument("-prid", "--processor-id", type=int, required=True)
    args_parser.add_argument("-c", "--config", type=pathlib.Path, required=True)

    args = args_parser.parse_args()
    # config
    config = Config()

    # load from default
    config.load_default()

    # args override config
    if args.data_dir:
        config.eggroll.data.dir = args.data_dir

    # load config from properties
    config_path = args.config
    config_path = os.path.realpath(config_path)
    if os.path.exists(config_path):
        config.load_properties(config_path)

    # load config from env
    config.load_env()

    # init loggers
    session_id = args.session_id
    processor_id = args.processor_id
    log_file = os.path.join(config.eggroll.logs.dir, session_id, f"egg_pair.log")
    logging.basicConfig(
        level=logging.DEBUG,
        format=f"[{processor_id}]%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename=log_file,
    )

    # data dir
    data_dir = config.eggroll.data.dir
    if not os.path.isabs(data_dir):
        raise ValueError(f"data dir {data_dir} is not absolute path")
    try:
        serve(
            config=config,
            data_dir=data_dir,
            port=args.port,
            transfer_port=args.transfer_port,
            cluster_manager=args.cluster_manager,
            node_manager=args.node_manager,
            session_id=session_id,
            server_node_id=args.server_node_id,
            processor_id=args.processor_id,
        )
    except Exception as e:
        L.exception(f"egg_pair server error: {e}")
        raise e


if __name__ == "__main__":
    main()
