from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.core.transfer_model import (
    ErRollSitePullGetPartitionStatusRequest,
    ErRollSitePullGetPartitionStatusResponse,
)
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _PullGetPartitionStatus(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        from eggroll.roll_pair._batch_stream_status import PutBatchTask

        request = job.first_functor.deserialized_as(ErRollSitePullGetPartitionStatusRequest)

        put_batch_task = PutBatchTask(tag=f"{request.tag}{task.first_input.id}", partition=None)
        status = put_batch_task.get_status(request.timeout)
        return ErRollSitePullGetPartitionStatusResponse(
            partition_id=task.first_input.id,
            tag=status.tag,
            is_finished=status.is_finished,
            total_batches=status.total_batches,
            batch_seq_to_pair_counter=status.batch_seq_to_pair_counter,
            total_streams=status.total_streams,
            stream_seq_to_pair_counter=status.stream_seq_to_pair_counter,
            stream_seq_to_batch_seq=status.stream_seq_to_batch_seq,
            total_pairs=status.total_pairs,
            data_type=status.data_type,
        )
