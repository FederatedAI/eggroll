import contextlib
import logging

from eggroll.computing.tasks import store
from eggroll.core.meta_model import ErJob, ErTask, AggregateRequest, AggregateResponse
from ._task import Task, EnvOptions

L = logging.getLogger(__name__)


class Aggregate(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        zero_value = job.first_functor.deserialized_as(AggregateRequest).zero_value
        seq_op = job.second_functor.func
        seq_op_result = zero_value
        with contextlib.ExitStack() as stack:
            input_adapter = stack.enter_context(
                store.get_adapter(task.first_input, env_options.data_dir)
            )
            input_iter = stack.enter_context(input_adapter.iteritems())
            for k_bytes, v_bytes in input_iter:
                seq_op_result = seq_op(seq_op_result, v_bytes)
        return AggregateResponse(id=task.first_input.id, value=seq_op_result)
