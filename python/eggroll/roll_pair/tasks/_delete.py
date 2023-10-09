from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.model.task import (
    DeleteRequest,
    DeleteResponse,
)
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _Delete(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        request = job.first_functor.deserialized_as(DeleteRequest)
        with task.first_input.get_adapter(data_dir) as input_adapter:
            success = input_adapter.delete(request.key)
            if success:
                L.trace("delete k success")
            return DeleteResponse(key=request.key, success=success)
