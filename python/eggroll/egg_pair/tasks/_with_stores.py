import logging

from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.model.task import WithStoresResponse
from ._task import Task, EnvOptions

L = logging.getLogger(__name__)


class _WithStores(Task):
    @classmethod
    def run(cls,
            env_options: EnvOptions,
            job: ErJob, task: ErTask):
        f = job.first_functor.func
        value = f(env_options.data_dir, task)
        return WithStoresResponse(id=task.first_input.id, value=value)
