import logging

from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.model.task import (
    GetRequest,
    GetResponse,
)
from ._task import Task, EnvOptions

L = logging.getLogger(__name__)


class _Get(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        request = GetRequest.from_proto_string(job.first_functor.body)
        with task.first_input.get_adapter(env_options.data_dir) as input_adapter:
            value = input_adapter.get(request.key)
            if value is None:
                return GetResponse(key=request.key, value=b"", exists=False)
            else:
                return GetResponse(key=request.key, value=value, exists=True)
