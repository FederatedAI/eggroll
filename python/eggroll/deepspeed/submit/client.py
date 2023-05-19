import time
from contextlib import ExitStack
from typing import Dict, List, Optional

from eggroll.core.conf_keys import SessionConfKeys
from eggroll.core.constants import SessionStatus
from eggroll.core.proto import deepspeed_pb2

from ..client import BaseClient
from .commands import JobCommands


class DeepspeedJob:
    def __init__(self, session_id):
        self._session_id = session_id

    def submit(
            self,
            name="",
            world_size=1,
            command_arguments: Optional[List[str]] = None,
            environment_variables: Optional[Dict[str, str]] = None,
            files: Optional[Dict[str, str]] = None,
            zipped_files: Optional[Dict[str, str]] = None,
            options: Optional[Dict] = None,
    ):
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
            options=options,
            status=SessionStatus.NEW,
        )

        submit_response = BaseClient().do_sync_request(
            submit_request, output_type=deepspeed_pb2.SubmitJobResponse, command_uri=JobCommands.SUBMIT_JOB
        )
        return submit_response

    def query_status(self):
        query_job_status_request = deepspeed_pb2.QueryJobStatusRequest(session_id=self._session_id)
        return BaseClient().do_sync_request(
            query_job_status_request, output_type=deepspeed_pb2.QueryJobStatusResponse,
            command_uri=JobCommands.QUERY_JOB_STATUS
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
