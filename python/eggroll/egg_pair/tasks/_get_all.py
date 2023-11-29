import logging

from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.model.task import (
    GetAllRequest,
)
from eggroll.core.transfer.transfer_service import TransferService
from eggroll.roll_pair.transfer_pair import TransferPair
from ._task import EnvOptions, Task

L = logging.getLogger(__name__)


class _GetAll(Task):
    @classmethod
    def run(cls,
            env_options: EnvOptions,
            job: ErJob, task: ErTask):
        tag = task.id
        request = job.first_functor.deserialized_as(GetAllRequest)

        def generate_broker():
            with task.first_input.get_adapter(env_options.data_dir) as db, db.iteritems() as rb:
                limit = None if request.limit < 0 else request.limit
                try:
                    yield from TransferPair.pair_to_bin_batch(config=env_options.config, input_iter=rb, limit=limit)
                finally:
                    TransferService.remove_broker(tag)

        TransferService.set_broker(tag, generate_broker())
