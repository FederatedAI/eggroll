import time
import threading
import queue
import json
import typing
from multiprocessing.pool import ThreadPool
from eggroll.config import Config
from eggroll.deepspeed.submit.client import DeepspeedJob
from eggroll.deepspeed.utils.params_utils import filter_invalid_params
from eggroll.core.proto import extend_pb2_grpc,extend_pb2,deepspeed_download_pb2
from eggroll.core.command.command_status import SessionStatus
from typing import Dict, List, Optional
from eggroll.core.command.command_uri import JobCommands


class EggrollClient(DeepspeedJob):

    def __init__(self, config: Config, host="127.0.0.1", port=4670):

        super().__init__(config=config, host=host, port=port)

    @staticmethod
    def check_status(func):
        def wrapper(self, *args, **kwargs):
            result = self.query_status()
            if not result.get("status"):
                return {"code": 0, "message": f"session_id:{self._session_id} is not exists"}
            return func(self, *args, **kwargs)
        return wrapper

    def query_status(self):
        # response = super(EggrollClient, self).query_status()
        result = super().query_status()
        if not result.status:
            return {"code": 0, "message": f"session_id:{self._session_id} is not exists"}
        return {"status": result.status}

    @check_status
    def kill(self):
        # result = self.query_status()
        # if not result.get("status"):
        #     return result
        super().kill()
        result = self.query_status()
        return {"session_id": self._session_id, "status": result.get("status")}

    @check_status
    def stop(self):
        # result = self.query_status()
        # if not result.get("status"):
        #     return result
        super().stop()
        result = self.query_status()
        return {"session_id": self._session_id, "status": result.get("status")}

    def await_finished(self, timeout: int = 0, poll_interval: int = 1):
        deadline = time.time() + timeout
        query_response = self.query_status()
        while timeout <= 0 or time.time() < deadline:
            if query_response.get("status", "") not in {SessionStatus.NEW, SessionStatus.ACTIVE}:
                break
            query_response = self.query_status()
            time.sleep(poll_interval)
        return query_response["status"]

    def writers(self, stream, result_queue):
        try:
            for res in stream:
                if str(res.code) == "0":
                    for log_info in res.datas:
                        print(log_info)
                elif str(res.code) == "110":
                    ret = {"code": res.code, "message": f"file is not exists sessionId: {self._session_id}"}
                    result_queue.put(ret)
                else:
                    ret = {"code": res.code, "message": f"info error"}
                    result_queue.put(ret)
        except Exception as e:
            ret = self.query_status()
            ret = {"status": ret["status"]}
            result_queue.put(ret)

    def cancel_stream(self, stream, flag):
        self.await_finished()
        time.sleep(5)
        stream.cancel()
        try:
            flag.pop()
        except:
            pass

    def get_log(self, sessionId: str, rank: str = None, path: str = None, startLine: int = None,
                logType: str = None):
        kwargs = locals()
        params = filter_invalid_params(**kwargs)
        build = extend_pb2.GetLogRequest(**params)
        flag = [0]
        channel = self._get_client().channel_factory
        stub = extend_pb2_grpc.ExtendTransferServerStub(
            channel.create_channel(config=None, endpoint=self._get_client().endpoint)
        )
        builds = self.generator_yields(build, flag)
        stream = stub.getLog(builds)

        result_queue = queue.Queue()
        channel_1 = threading.Thread(target=self.writers, args=(stream, result_queue))
        channel_2 = threading.Thread(target=self.cancel_stream, args=(stream, flag))

        channel_1.start()
        channel_2.start()
        channel_1.join()
        channel_2.join()

        return result_queue.get()

    def download_job_v2(
            self,
            session_id,
            ranks: Optional[List[int]] = None,
            content_type: int = 0,
            compress_method: str = "zip",
            compress_level: int = 1,
    ):
        if compress_level < 0 or compress_level > 9:
            raise ValueError(f"compress_level must be in [0, 9], got {compress_level}")
        if compress_method not in {"zip"}:
            raise ValueError(f"compress_method must be in ['zip'], got {compress_method}")

        if ranks is None:
            ranks = []
        download_job_request = deepspeed_download_pb2.PrepareDownloadRequest(
            session_id=session_id,
            ranks=ranks,
            compress_method=compress_method,
            compress_level=compress_level,
            content_type=content_type,
        )
        prepare_download_job_response = self._get_client().do_sync_request(
            download_job_request, output_type=deepspeed_download_pb2.PrepareDownloadResponse, command_uri=JobCommands.PREPARE_DOWNLOAD_JOB
        )
        download_session_id = prepare_download_job_response.session_id
        download_meta: dict = json.loads(prepare_download_job_response.content)
        zipped_container_content_map = {}
        pool = ThreadPool()
        try:
            def inner_handle_download(address):
                element_data = download_meta[address]
                ranks = list(map(lambda d: d[2], element_data))
                indexes = list(map(lambda d: d[4], element_data))
                ip_port = address.split(":")
                eggpair_client = self._get_client(ip_port[0], int(ip_port[1]))
                request = deepspeed_download_pb2.DsDownloadRequest(
                    compress_level=compress_level,
                    compress_method=compress_method,
                    ranks=ranks,
                    content_type=content_type,
                    session_id=session_id
                )
                response = eggpair_client.do_download_stream(request)
                zipped_container_content_map.update(response)
            pool.map(inner_handle_download, download_meta)
            pool.close()
            pool.join()
            return zipped_container_content_map
        finally:
            try:
                self.close_session(session_id=download_session_id)
                print("over")
            except Exception as e:
                self.kill_session(session_id=download_session_id)

    def download_job_to(
            self,
            ranks: Optional[List[int]] = None,
            content_type: int = 0,
            rank_to_path: typing.Callable[[int], str] = lambda rank: f"rank_{rank}.zip",
            compress_method: str = "zip",
            compress_level: int = 1,
    ):
        response = self.query_status()
        if not response.get("status"):
            raise ValueError(f'not found session_id:{self._session_id}')
        download_job_response = self.download_job_v2(self._session_id, ranks, content_type, compress_method, compress_level)
        for key, value in download_job_response.items():
            path = rank_to_path(key)
            with open(path, "wb") as f:
                f.write(value)