from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.model.task import (
    GetAllRequest,
)
from eggroll.core.transfer.transfer_service import TransferService
from eggroll.roll_pair.transfer_pair import TransferPair
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _GetAll(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        tag = task.id
        request = job.first_functor.deserialized_as(GetAllRequest)

        def generate_broker():
            with task.first_input.get_adapter(data_dir) as db, db.iteritems() as rb:
                limit = None if request.limit < 0 else request.limit
                try:
                    yield from TransferPair.pair_to_bin_batch(rb, limit=limit)
                finally:
                    TransferService.remove_broker(tag)

        TransferService.set_broker(tag, generate_broker())
