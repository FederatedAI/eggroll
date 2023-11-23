from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.roll_pair.transfer_pair import TransferPair
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _PutAll(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        TransferPair(task.id).store_broker(data_dir, task.first_output, False).result()
