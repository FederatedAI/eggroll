import logging

from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.model.task import (
    PutRequest,
    PutResponse,
)
from ._task import Task, EnvOptions

L = logging.getLogger(__name__)


class _Put(Task):
    @classmethod
    def run(cls,
            env_options: EnvOptions
            , job: ErJob, task: ErTask):
        request = job.first_functor.deserialized_as(PutRequest)
        with task.first_input.get_adapter(env_options.data_dir) as input_adapter:
            success = input_adapter.put(request.key, request.value)
            return PutResponse(key=request.key, value=request.value, success=success)
