from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.model.task import WithStoresResponse
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _WithStores(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        f = job.first_functor.func
        value = f(data_dir, task)
        return WithStoresResponse(id=task.first_input.id, value=value)
