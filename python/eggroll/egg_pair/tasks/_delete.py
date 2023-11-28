import logging

from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.model.task import (
    DeleteRequest,
    DeleteResponse,
)
from ._task import Task, EnvOptions

L = logging.getLogger(__name__)


class _Delete(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        request = job.first_functor.deserialized_as(DeleteRequest)
        with task.first_input.get_adapter(env_options.data_dir) as input_adapter:
            success = input_adapter.delete(request.key)
            if success:
                L.trace("delete k success")
            return DeleteResponse(key=request.key, success=success)
