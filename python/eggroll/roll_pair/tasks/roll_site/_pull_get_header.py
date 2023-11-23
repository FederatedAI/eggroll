from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _PullGetHeader(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        from eggroll.core.transfer_model import ErRollSitePullGetHeaderRequest, ErRollSitePullGetHeaderResponse
        from eggroll.roll_pair._batch_stream_status import PutBatchTask

        request = job.first_functor.deserialized_as(ErRollSitePullGetHeaderRequest)
        header = PutBatchTask(tag=request.tag).get_header(timeout=request.timeout)
        return ErRollSitePullGetHeaderResponse(header=header)
