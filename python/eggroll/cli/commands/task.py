#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
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
import datetime
import os
import tempfile

import click
import time

from eggroll.deepspeed.sdk_client import EggrollClient
from eggroll.deepspeed.submit.client import ContentType
from ..utils.cli_utils import prettify, unzip
from eggroll.config import Config


@click.group(short_help="Task Operations")
@click.pass_context
def task(ctx):
    """ """
    pass


@task.command("submit", short_help="Submit task")
@click.option("--num-gpus", type=click.INT, required=True, help="world size")
@click.option(
    "--timeout-seconds",
    type=click.INT,
    required=False,
    default=60000,
    help="queue timeout time(s)",
)
@click.option(
    "--script-path", type=click.Path(exists=True), required=True, help="script path"
)
@click.option(
    "--log-type", type=click.STRING, required=False, help="log type", default="stdout"
)
@click.option("--conf", multiple=True)
@click.pass_context
def submit(ctx, **kwargs):
    config = Config().load_default()
    world_size = kwargs.get("num_gpus")
    script_path = kwargs.get("script_path")
    timeout_seconds = kwargs.get("timeout_seconds")
    files = {os.path.basename(script_path): script_path}
    resource_options = {"timeout_seconds": timeout_seconds}
    options = {"eggroll.container.deepspeed.script.path": os.path.basename(script_path)}
    script_args = kwargs.get("conf")
    command_arguments = []
    for args in script_args:
        if "=" in args:
            _args = args.split("=")
            command_arguments.append(f"--{_args[0]}")
            command_arguments.append(_args[-1])

    client: EggrollClient = ctx.obj["client"]
    session_id = (
        f"deepspeed_session_{datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')}"
    )
    print(f"session_id:{session_id}")
    client._session_id = session_id
    client.submit(
        world_size=world_size,
        files=files,
        resource_options=resource_options,
        options=options,
        command_arguments=command_arguments,
    )

    while True:
        response = client.query_status()
        print(f'task session_id:{session_id} status:{response["status"]}')
        time.sleep(1)
        if response["status"] != "NEW":
            break
    log_type = kwargs.get("log_type") if not kwargs.get("log_type") else "stdout"
    response = client.get_log(sessionId=session_id, logType=log_type)
    if response["status"] != 'failed':
        response = client.query_status()
    prettify(response)


@task.command("query", short_help="Query task status")
@click.option("--session-id", type=click.STRING, required=True, help="session id")
@click.pass_context
def query(ctx, **kwargs):
    client: EggrollClient = ctx.obj["client"]
    client._session_id = kwargs.get("session_id")
    response = client.query_status()
    prettify(response)


@task.command("kill", short_help="Kill job")
@click.option("--session-id", type=click.STRING, required=True, help="session id")
@click.pass_context
def kill(ctx, **kwargs):
    client: EggrollClient = ctx.obj["client"]
    client._session_id = kwargs.get("session_id")
    response = client.kill()
    prettify(response)


@task.command("stop", short_help="Stop job")
@click.option("--session-id", type=click.STRING, required=True, help="session id")
@click.pass_context
def stop(ctx, **kwargs):
    client: EggrollClient = ctx.obj["client"]
    client._session_id = kwargs.get("session_id")
    response = client.stop()
    prettify(response)


@task.command("download", short_help="Download task output")
@click.option("--session-id", type=click.STRING, required=True, help="session id")
@click.option(
    "--content-type",
    type=click.INT,
    default=0,
    required=False,
    help="ALL:0, MODELS: 1, LOGS: 2, RESULT: 3",
)
@click.option("--download-dir", type=click.STRING, required=True, help="download dir")
@click.option("--ranks", type=click.STRING, required=False, help="0,1,2..")
@click.pass_context
def download(ctx, **kwargs):
    client: EggrollClient = ctx.obj["client"]
    download_dir = kwargs.get("download_dir")
    client._session_id = kwargs.get("session_id")
    response = client.query_status()
    if response.get("code", None):
        return prettify(response)

    os.makedirs(download_dir, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp_dir:
        rank_to_path = lambda rank: f"{temp_dir}/{rank}.zip"
        client.download_job_to(
            rank_to_path=rank_to_path,
            content_type=ContentType(kwargs.get("content_type")),
            ranks=kwargs.get("ranks", None),
        )
        for file in os.listdir(temp_dir):
            if file.endswith(".zip"):
                rank_dir = os.path.join(download_dir, file.split(".zip")[0])
                os.makedirs(rank_dir, exist_ok=True)
                unzip(os.path.join(temp_dir, file), extra_dir=rank_dir)
    response_json = {"download_dir": f"download success: {download_dir}"}
    prettify(response_json)


@task.command("get-log", short_help="Get task log")
@click.option("--session-id", type=click.STRING, required=True, help="session id")
@click.option("--rank", type=click.STRING, required=False, help="0,1,2..", default="0")
@click.option("--path", type=click.STRING, required=False, help="path")
@click.option(
    "--tail", type=click.INT, required=False, help="log tail line", default=100
)
@click.option(
    "--log-type",
    type=click.Choice(["stdout", "stderr"]),
    required=False,
    help="log type",
    default="stdout",
)
@click.pass_context
def get_log(ctx, **kwargs):
    client: EggrollClient = ctx.obj["client"]
    client._session_id = kwargs.get("session_id")
    response = client.get_log(
        sessionId=kwargs.get("session_id"),
        rank=kwargs.get("rank"),
        path=kwargs.get("path"),
        startLine=kwargs.get("tail"),
        logType=kwargs.get("log_type"),
    )
    prettify(response)
