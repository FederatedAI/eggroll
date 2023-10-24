from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.transfer_model import ErRollSitePullClearStatusRequest, ErRollSitePullClearStatusResponse
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _PullClearStatus(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        from eggroll.roll_pair._batch_stream_status import PutBatchTask

        request = job.first_functor.deserialized_as(ErRollSitePullClearStatusRequest)

        put_batch_task = PutBatchTask(tag=f"{request.tag}{task.first_input.id}", partition=None)
        status = put_batch_task.clear_status()
        return ErRollSitePullClearStatusResponse()
