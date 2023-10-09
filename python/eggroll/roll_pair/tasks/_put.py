from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.model.task import (
    PutRequest,
    PutResponse,
)
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _Put(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        request = job.first_functor.deserialized_as(PutRequest)
        with task.first_input.get_adapter(data_dir) as input_adapter:
            success = input_adapter.put(request.key, request.value)
            return PutResponse(key=request.key, value=request.value, success=success)
