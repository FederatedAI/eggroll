import time
import threading
import queue
from eggroll.config import Config
from eggroll.deepspeed.submit.client import DeepspeedJob
from eggroll.deepspeed.utils.params_utils import filter_invalid_params
from eggroll.core.proto import extend_pb2_grpc, extend_pb2
from eggroll.core.command.command_status import SessionStatus


class EggrollClient(DeepspeedJob):

    def __init__(self, config: Config, host="127.0.0.1", port=4670):

        super().__init__(config=config, host=host, port=port)

    def query_status(self):
        result = super().query_status()
        if not result.status:
            return {"code": 110, "message": f"session_id:{self._session_id} is not exists"}
        return {"status": result.status}

    def execute_action(self, action):
        result = self.query_status()
        if result.get("code"):
            return result
        getattr(super(), action)()
        result = self.query_status()
        return {"session_id": self._session_id, "status": result.get("status")}

    def kill(self):
        return self.execute_action("kill")

    def stop(self):
        return self.execute_action("stop")

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
                    ret = {"code": res.code, "status": "failed", "message": f"file is not exists sessionId: {self._session_id}"}
                    result_queue.put(ret)
                else:
                    ret = {"code": res.code, "status": "failed", "message": f"info error"}
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
        response = self.query_status()
        if response.get("code"):
            return response
        params = filter_invalid_params(**kwargs)
        build = extend_pb2.GetLogRequest(**params)
        flag = [0]
        channel = self._get_client().channel_factory
        stub = extend_pb2_grpc.ExtendTransferServerStub(
            channel.create_channel(config=self._config, endpoint=self._get_client().endpoint)
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