import datetime
import os
import time
import typing
from contextlib import ExitStack
from typing import Dict, List, Optional

from eggroll.core.conf_keys import SessionConfKeys
from eggroll.core.constants import SessionStatus
from eggroll.core.proto import deepspeed_pb2

from ..client import BaseClient
from .commands import JobCommands


class DeepspeedJob:
    def __init__(self, session_id: Optional[str] = None):
        if session_id is None:
            session_id = f"deepspeed_session_{datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')}"
        self._session_id = session_id
        self._rank_to_processor = {}

    def submit(
            self,
            name="",
            world_size=1,
            command_arguments: Optional[List[str]] = None,
            environment_variables: Optional[Dict[str, str]] = None,
            files: Optional[Dict[str, str]] = None,
            zipped_files: Optional[Dict[str, str]] = None,
            resource_options: Optional[Dict] = None,
            options: Optional[Dict] = None,
    ):
        if resource_options is None:
            resource_options = {}
        if options is None:
            options = {}
        if not name:
            name = f"session_{self._session_id}"
        options = options.copy()
        options[SessionConfKeys.CONFKEY_SESSION_ID] = self._session_id
        environment_variables = {} if environment_variables is None else environment_variables

        files = {} if files is None else files
        zipped_files = {} if zipped_files is None else zipped_files

        with ExitStack() as stack:
            files = {name: stack.enter_context(open(path, "rb")).read() for name, path in files.items()}
            zipped_files = {name: stack.enter_context(open(path, "rb")).read() for name, path in zipped_files.items()}

        submit_request = deepspeed_pb2.SubmitJobRequest(
            session_id=self._session_id,
            name=name,
            job_type="deepspeed",
            world_size=world_size,
            command_arguments=command_arguments,
            environment_variables={str(k): str(v) for k, v in environment_variables.items()},
            files=files,
            zipped_files=zipped_files,
            resource_options=deepspeed_pb2.ResourceOptions(
                timeout_seconds=int(resource_options.get("timeout_seconds", 300)),
                resource_exhausted_strategy=resource_options.get("resource_exhausted_strategy", "waiting")
            ),
            options=options
        )

        submit_response = BaseClient().do_sync_request(
            submit_request, output_type=deepspeed_pb2.SubmitJobResponse, command_uri=JobCommands.SUBMIT_JOB
        )
        for processor in submit_response.processors:
            rank = int(processor.options.get("globalRank"))
            self._rank_to_processor[rank] = processor
        return submit_response

    def query_status(self):
        query_job_status_request = deepspeed_pb2.QueryJobStatusRequest(session_id=self._session_id)
        return BaseClient().do_sync_request(
            query_job_status_request,
            output_type=deepspeed_pb2.QueryJobStatusResponse,
            command_uri=JobCommands.QUERY_JOB_STATUS,
        )

    def query_session(self):
        query_job_request = deepspeed_pb2.QueryJobRequest(session_id=self._session_id)
        query_response = BaseClient().do_sync_request(
            query_job_request, output_type=deepspeed_pb2.QueryJobResponse, command_uri=JobCommands.QUERY_JOB
        )
        return query_response

    def kill(self):
        kill_job_request = deepspeed_pb2.KillJobRequest(session_id=self._session_id)
        kill_response = BaseClient().do_sync_request(
            kill_job_request, output_type=deepspeed_pb2.KillJobResponse, command_uri=JobCommands.KILL_JOB
        )
        return kill_response

    def await_finished(self, timeout: int = 0, poll_interval: int = 1):
        deadline = time.time() + timeout
        query_response = self.query_status()
        while timeout <= 0 or time.time() < deadline:
            if query_response.status not in {SessionStatus.NEW, SessionStatus.ACTIVE}:
                break
            query_response = self.query_status()
            time.sleep(poll_interval)
        return query_response.status

    def download_job(self, ranks: Optional[List[int]] = None):
        if ranks is None:
            ranks = []
        download_job_request = deepspeed_pb2.DownloadJobRequest(
            session_id=self._session_id,
            ranks=ranks,
            compress_method="zip",
        )
        download_job_response = BaseClient().do_sync_request(
            download_job_request, output_type=deepspeed_pb2.DownloadJobResponse, command_uri=JobCommands.DOWNLOAD_JOB
        )
        return download_job_response

    def download_job_to(
            self,
            ranks: Optional[List[int]] = None,
            rank_to_path: typing.Callable[[int], str] = lambda rank: f"rank_{rank}.zip",
    ):
        download_job_response = self.download_job(ranks)
        if ranks is None:
            ranks = range(len(download_job_response.container_content))
        for rank, content in zip(ranks, download_job_response.container_content):
            path = rank_to_path(rank)
            print(os.getcwd())
            with open(path, "wb") as f:
                f.write(content.content)
