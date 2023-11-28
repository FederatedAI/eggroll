import logging

from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.model.task import (
    CountResponse,
)
from ._task import Task, EnvOptions

L = logging.getLogger(__name__)


class _Count(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, _job: ErJob, task: ErTask):
        with task.first_input.get_adapter(env_options.data_dir) as input_adapter:
            return CountResponse(value=input_adapter.count())
