import logging

from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.roll_pair.transfer_pair import TransferPair
from ._task import EnvOptions, Task

L = logging.getLogger(__name__)


class _PutAll(Task):
    @classmethod
    def run(cls,
            env_options: EnvOptions,
            job: ErJob, task: ErTask):
        TransferPair(task.id).store_broker(env_options.data_dir, task.first_output, False).result()
