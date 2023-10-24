from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.model.task import (
    CountResponse,
)
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _Count(object):
    @classmethod
    def run(cls, data_dir: str, _job: ErJob, task: ErTask):
        with task.first_input.get_adapter(data_dir) as input_adapter:
            return CountResponse(value=input_adapter.count())
