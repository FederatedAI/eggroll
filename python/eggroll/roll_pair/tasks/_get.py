from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.model.task import (
    GetRequest,
    GetResponse,
)
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _Get(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        request = GetRequest.from_proto_string(job.first_functor.body)
        with task.first_input.get_adapter(data_dir) as input_adapter:
            value = input_adapter.get(request.key)
            if value is None:
                return GetResponse(key=request.key, value=b"", exists=False)
            else:
                return GetResponse(key=request.key, value=value, exists=True)
