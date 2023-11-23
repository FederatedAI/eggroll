from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.roll_pair._batch_stream_status import PutBatchTask
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _PutBatch(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        PutBatchTask(str(task.id), task.first_output).run(data_dir)
