import datetime
import enum
import json
import threading
import time
import typing
from contextlib import ExitStack
from multiprocessing.dummy import Pool as ThreadPool
from typing import Dict, List, Optional

from eggroll.config import load_config, ConfigKey, ConfigUtils
from eggroll.core.command.command_status import SessionStatus
from eggroll.core.command.command_uri import SessionCommands, JobCommands
from eggroll.core.proto import (
    containers_pb2,
    deepspeed_pb2,
    deepspeed_download_pb2,
    meta_pb2,
    extend_pb2_grpc,
    extend_pb2,
)

from ..client import BaseClient
from ..store.client import destroy


class ContentType(enum.Enum):
    ALL = 0
    MODELS = 1
    LOGS = 2
    RESULT = 3

    def to_proto(self):
        if self == ContentType.ALL:
            return containers_pb2.ALL
        if self == ContentType.MODELS:
            return containers_pb2.MODELS
        if self == ContentType.LOGS:
            return containers_pb2.LOGS
        if self == ContentType.RESULT:
            return containers_pb2.RESULT
        raise NotImplementedError(f"{self}")


class DeepspeedJob:
    def __init__(
        self,
        session_id: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        config=None,
    ):
        if config is None:
            config = load_config(None)
        if session_id is None:
            session_id = f"deepspeed_session_{datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')}"
        self._config = config
        self._session_id = session_id
        self._host = host
        self._port = port

    @property
    def session_id(self):
        return self._session_id

    def _get_client(self):
        return BaseClient(self._config, self._host, self._port)

    def _get_clientv2(self, host, port):
        return BaseClient(self._config, host, port)

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
        options[ConfigKey.eggroll.session.id] = self._session_id
        environment_variables = (
            {} if environment_variables is None else environment_variables
        )

        files = {} if files is None else files
        zipped_files = {} if zipped_files is None else zipped_files

        with ExitStack() as stack:
            files = {
                name: stack.enter_context(open(path, "rb")).read()
                for name, path in files.items()
            }
            zipped_files = {
                name: stack.enter_context(open(path, "rb")).read()
                for name, path in zipped_files.items()
            }

        submit_request = deepspeed_pb2.SubmitJobRequest(
            session_id=self._session_id,
            name=name,
            job_type="deepspeed",
            world_size=world_size,
            command_arguments=command_arguments,
            environment_variables={
                str(k): str(v) for k, v in environment_variables.items()
            },
            files=files,
            zipped_files=zipped_files,
            resource_options=deepspeed_pb2.ResourceOptions(
                timeout_seconds=int(resource_options.get("timeout_seconds", 300)),
                resource_exhausted_strategy=resource_options.get(
                    "resource_exhausted_strategy", "waiting"
                ),
            ),
            options=options,
        )

        submit_response = self._get_client().do_sync_request(
            submit_request,
            output_type=deepspeed_pb2.SubmitJobResponse,
            command_uri=JobCommands.SUBMIT_JOB,
        )
        return submit_response

    def query_status(self):
        query_job_status_request = deepspeed_pb2.QueryJobStatusRequest(
            session_id=self._session_id
        )
        return self._get_client().do_sync_request(
            query_job_status_request,
            output_type=deepspeed_pb2.QueryJobStatusResponse,
            command_uri=JobCommands.QUERY_JOB_STATUS,
        )

    def query_session(self):
        query_job_request = deepspeed_pb2.QueryJobRequest(session_id=self._session_id)
        query_response = self._get_client().do_sync_request(
            query_job_request,
            output_type=deepspeed_pb2.QueryJobResponse,
            command_uri=JobCommands.QUERY_JOB,
        )
        return query_response

    def kill(self):
        kill_job_request = deepspeed_pb2.KillJobRequest(session_id=self._session_id)
        kill_response = self._get_client().do_sync_request(
            kill_job_request,
            output_type=deepspeed_pb2.KillJobResponse,
            command_uri=JobCommands.KILL_JOB,
        )
        return kill_response

    def stop(self):
        status = self.query_status()
        if not status.get("status"):
            return status
        stop_job_request = deepspeed_pb2.StopJobRequest(session_id=self._session_id)
        response = self._get_client().do_sync_request(
            stop_job_request,
            output_type=deepspeed_pb2.StopJobResponse,
            command_uri=JobCommands.STOP_JOB,
        )
        status = self.query_status()
        response = {"session_id": response.session_id, "status": status.get("status")}
        return response

    def await_finished(self, timeout: int = 0, poll_interval: int = 1):
        deadline = time.time() + timeout
        query_response = self.query_status()
        while timeout <= 0 or time.time() < deadline:
            if query_response.status not in {SessionStatus.NEW, SessionStatus.ACTIVE}:
                break
            query_response = self.query_status()
            time.sleep(poll_interval)
        return query_response.status

    def download_jobv2(
        self,
        ranks: Optional[List[int]] = None,
        content_type: ContentType = ContentType.ALL,
        compress_method: str = "zip",
        compress_level: int = 1,
    ):
        if compress_level < 0 or compress_level > 9:
            raise ValueError(f"compress_level must be in [0, 9], got {compress_level}")
        if compress_method not in {"zip"}:
            raise ValueError(
                f"compress_method must be in ['zip'], got {compress_method}"
            )

        if ranks is None:
            ranks = []
        download_job_request = deepspeed_download_pb2.PrepareDownloadRequest(
            session_id=self._session_id,
            ranks=ranks,
            compress_method=compress_method,
            compress_level=compress_level,
            content_type=content_type.to_proto(),
        )

        prepare_download_job_response = self._get_client().do_sync_request(
            download_job_request,
            output_type=deepspeed_download_pb2.PrepareDownloadResponse,
            command_uri=JobCommands.PREPARE_DOWNLOAD_JOB,
        )

        download_session_id = prepare_download_job_response.session_id
        download_meta: dict = json.loads(prepare_download_job_response.content)
        # zipped_container_content = []
        zipped_container_content_map = {}
        pool = ThreadPool()
        # pool.map(process, items)
        lock = threading.Lock()

        # print( download_meta)
        try:
            # for address in download_meta :

            # element_data = download_meta[address]
            def inner_handle_download(address):
                element_data = download_meta[address]
                ranks = list(map(lambda d: d[2], element_data))
                indexes = list(map(lambda d: d[4], element_data))
                ipport = address.split(":")
                eggpair_client = self._get_clientv2(ipport[0], int(ipport[1]))
                request = deepspeed_download_pb2.DsDownloadRequest(
                    compress_level=compress_level,
                    compress_method=compress_method,
                    ranks=ranks,
                    content_type=content_type.to_proto(),
                    session_id=self.session_id,
                )
                response = eggpair_client.do_download_stream(request)
                zipped_container_content_map.update(response)
                # if(response != None):
                #     temp_ziped = list(zip(indexes,response.container_content))
                #     try:
                #         lock.acquire()
                #         zipped_container_content_map.(temp_ziped)
                #     finally:
                #         lock.release()

                # else:
                #     raise RuntimeError(f"download return None from {address}")

            pool.map(inner_handle_download, download_meta)
            pool.close()
            pool.join()

            # zipped_container_content.sort(key=lambda  x:x[0])
            # final_content= list(map(lambda d:d[1],zipped_container_content))
            # print("zipped_container_content_map",zipped_container_content_map)
            return zipped_container_content_map
            # return deepspeed_pb2.DownloadJobResponse(session_id=self._session_id,container_content=final_content)

        finally:
            try:
                self.close_session(session_id=download_session_id)
                print("over")
            except Exception as e:
                self.kill_session(session_id=download_session_id)

    def close_session(self, session_id):
        if session_id is not None:
            session = meta_pb2.SessionMeta(id=session_id)
            download_job_response = self._get_client().do_sync_request(
                session,
                output_type=meta_pb2.SessionMeta,
                command_uri=SessionCommands.STOP_SESSION,
            )
        print("close")

    def kill_session(self, session_id):
        if session_id is not None:
            session = meta_pb2.SessionMeta(id=session_id)
            download_job_response = self._get_client().do_sync_request(
                session,
                output_type=meta_pb2.SessionMeta,
                command_uri=SessionCommands.KILL_SESSION,
            )
        print("close")

    def download_job(
        self,
        ranks: Optional[List[int]] = None,
        content_type: ContentType = ContentType.ALL,
        compress_method: str = "zip",
        compress_level: int = 1,
    ):
        if compress_level < 0 or compress_level > 9:
            raise ValueError(f"compress_level must be in [0, 9], got {compress_level}")
        if compress_method not in {"zip"}:
            raise ValueError(
                f"compress_method must be in ['zip'], got {compress_method}"
            )

        if ranks is None:
            ranks = []
        download_job_request = deepspeed_pb2.DownloadJobRequest(
            session_id=self._session_id,
            ranks=ranks,
            compress_method=compress_method,
            compress_level=compress_level,
            content_type=content_type.to_proto(),
        )
        download_job_response = self._get_client().do_sync_request(
            download_job_request,
            output_type=deepspeed_pb2.DownloadJobResponse,
            command_uri=JobCommands.DOWNLOAD_JOB,
        )
        return download_job_response

    def download_job_to(
        self,
        ranks: Optional[List[int]] = None,
        content_type: ContentType = ContentType.ALL,
        rank_to_path: typing.Callable[[int], str] = lambda rank: f"rank_{rank}.zip",
        compress_method: str = "zip",
        compress_level: int = 1,
    ):
        # download_job_response = self.download_job(ranks, content_type, compress_method, compress_level)
        download_job_response = self.download_jobv2(
            ranks, content_type, compress_method, compress_level
        )

        # if ranks is None:
        #     ranks = range(len(download_job_response.container_content))
        # for rank, content in zip(ranks, download_job_response.container_content):
        #     path = rank_to_path(rank)
        #     with open(path, "wb") as f:
        #         f.write(content.content)

        for key, value in download_job_response.items():
            path = rank_to_path(key)
            with open(path, "wb") as f:
                f.write(value)

        # for content in download_job_response.container_content:
        #     path = rank_to_path(content.rank)
        #     with open(path, "wb") as f:
        #         f.write(content.content)

    def cleanup(self):
        try:
            destroy(self._get_client(), self._session_id)
        except Exception as e:
            pass

    @staticmethod
    def generator_yields(build, flag):
        while len(flag):
            yield build
            time.sleep(10)

    @staticmethod
    def writer(stream, logging=None):
        if not logging:
            logging = print
        try:
            for res in stream:
                if str(res.code) == "0":
                    for info in res.datas:
                        logging(info)
                else:
                    # logging(f"get log return code {res.code}")
                    pass
        except Exception as e:
            pass

    def cancel_stream(self, stream, flag):
        self.await_finished()
        time.sleep(5)
        stream.cancel()
        try:
            # control stream end
            flag.pop()
        except:
            pass

    def write_logs_to(
        self,
        rank: str = "0",
        start_line: int = 0,
        log_type: str = "INFO",
        path: str = None,
        logging: object = None,
    ):
        flag = [0]
        builds = self.generator_yields(
            extend_pb2.GetLogRequest(
                sessionId=self.session_id,
                rank=rank,
                path=path,
                startLine=start_line,
                logType=log_type,
            ),
            flag,
        )
        channel = self._get_client().channel_factory
        stub = extend_pb2_grpc.ExtendTransferServerStub(
            channel.create_channel(
                config=self._config, endpoint=self._get_client().endpoint
            )
        )
        stream = stub.getLog(builds)
        _writer = threading.Thread(target=self.writer, args=(stream, logging))
        _cancel = threading.Thread(target=self.cancel_stream, args=(stream, flag))
        _writer.start()
        _cancel.start()
        return _writer, _cancel


if __name__ == "__main__":
    deepspeedJob = DeepspeedJob(
        session_id="deepspeed_session_20230705-175508-766715",
        host="localhost",
        port=4670,
    )
    deepspeedJob.download_job_to()
    # print(DsDownloadResponse(session_id="xxxxxxx").SerializeToString())
    # DsDownloadResponse.MergeFromString(b'\n\x07xxxxxxx')

    # print(CommandRequest(uri="xxxx").SerializeToString())
    # print(CommandRequest.MergeFrom(CommandRequest(uri="xxxx").SerializeToString()))
