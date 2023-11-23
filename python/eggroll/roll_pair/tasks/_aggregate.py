import contextlib

from eggroll.core.meta_model import ErJob, ErTask
from eggroll.core.model.task import AggregateRequest, AggregateResponse


class _Aggregate(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        zero_value = job.first_functor.deserialized_as(AggregateRequest).zero_value
        seq_op = job.second_functor.func
        seq_op_result = zero_value
        with contextlib.ExitStack() as stack:
            input_adapter = stack.enter_context(task.first_input.get_adapter(data_dir))
            input_iter = stack.enter_context(input_adapter.iteritems())
            for k_bytes, v_bytes in input_iter:
                seq_op_result = seq_op(seq_op_result, v_bytes)
        return AggregateResponse(id=task.first_input.id, value=seq_op_result)
