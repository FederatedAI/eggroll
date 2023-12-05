import logging
import typing
from typing import Callable
from typing import TypeVar

from eggroll.computing.tasks import consts, store
from eggroll.computing.tasks.submit_utils import _process_key_request
from eggroll.core.meta_model import (
    ErJob,
    ErTask,
    GetRequest,
    GetResponse,
    DeleteRequest,
    DeleteResponse,
    PutRequest,
    PutResponse,
)
from ._task import Task, EnvOptions

if typing.TYPE_CHECKING:
    from eggroll.computing.roll_pair import RollPair

L = logging.getLogger(__name__)

T = TypeVar("T")


class Get(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        request = GetRequest.from_proto_string(job.first_functor.body)
        with store.get_adapter(task.first_input, env_options.data_dir) as input_adapter:
            value = input_adapter.get(request.key)
            if value is None:
                return GetResponse(key=request.key, value=b"", exists=False)
            else:
                return GetResponse(key=request.key, value=value, exists=True)

    @classmethod
    def submit(cls, rp: "RollPair", k: bytes, partitioner: Callable[[bytes, int], int]):
        response = _process_key_request(
            rp,
            k,
            partitioner,
            job_name=consts.GET,
            request=GetRequest(key=k),
            response_type=GetResponse,
        )
        return response.value if response.exists else None


class Delete(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        request = job.first_functor.deserialized_as(DeleteRequest)
        with store.get_adapter(task.first_input, env_options.data_dir) as input_adapter:
            success = input_adapter.delete(request.key)
            if success:
                L.debug("delete k success")
            return DeleteResponse(key=request.key, success=success)

    @classmethod
    def submit(cls, rp: "RollPair", k: bytes, partitioner: Callable[[bytes, int], int]):
        response = _process_key_request(
            rp,
            k,
            partitioner,
            job_name=consts.DELETE,
            request=GetRequest(key=k),
            response_type=DeleteResponse,
        )
        return response.success


class Put(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        request = job.first_functor.deserialized_as(PutRequest)
        with store.get_adapter(task.first_input, env_options.data_dir) as input_adapter:
            success = input_adapter.put(request.key, request.value)
            return PutResponse(key=request.key, value=request.value, success=success)

    @classmethod
    def submit(
        cls,
        rp: "RollPair",
        k: bytes,
        v: bytes,
        partitioner: Callable[[bytes, int], int],
    ):
        response = _process_key_request(
            rp,
            k,
            partitioner,
            job_name=consts.PUT,
            request=PutRequest(key=k, value=v),
            response_type=PutResponse,
        )
        return response.success
