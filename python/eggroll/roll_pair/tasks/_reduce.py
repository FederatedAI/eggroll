import contextlib

from eggroll.core.meta_model import ErJob, ErTask
from eggroll.core.model.task import ReduceResponse


class _Reduce(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        seq_op = job.first_functor.func
        first = True
        seq_op_result = None
        with contextlib.ExitStack() as stack:
            input_adapter = stack.enter_context(task.first_input.get_adapter(data_dir))
            input_iter = stack.enter_context(input_adapter.iteritems())
            for k_bytes, v_bytes in input_iter:
                if first:
                    seq_op_result = v_bytes
                    first = False
                else:
                    seq_op_result = seq_op(seq_op_result, v_bytes)
        return ReduceResponse(id=task.first_input.id, value=seq_op_result)
