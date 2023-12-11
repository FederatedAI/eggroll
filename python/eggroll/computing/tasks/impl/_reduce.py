import contextlib
import functools
import logging
import typing

from eggroll.computing.tasks import consts, store, job_util
from eggroll.computing.tasks.submit_utils import block_submit_unary_unit_job
from eggroll.core.meta_model import (
    ErJob,
    ErFunctor,
    ErTask,
    ErJobIO,
    ReduceResponse,
)

if typing.TYPE_CHECKING:
    pass

from ._task import Task, EnvOptions

L = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from eggroll.computing.roll_pair import RollPair


class Reduce(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        seq_op = job.first_functor.func
        first = True
        seq_op_result = None
        with contextlib.ExitStack() as stack:
            input_adapter = stack.enter_context(
                store.get_adapter(task.first_input, env_options.data_dir)
            )
            input_iter = stack.enter_context(input_adapter.iteritems())
            for k_bytes, v_bytes in input_iter:
                if first:
                    seq_op_result = v_bytes
                    first = False
                else:
                    seq_op_result = seq_op(seq_op_result, v_bytes)
        return ReduceResponse(id=task.first_input.id, value=seq_op_result)

    @classmethod
    def reduce(cls, rp: "RollPair", func):
        job_id = job_util.generate_job_id(session_id=rp.session_id, tag=consts.REDUCE)
        reduce_op = ErFunctor.from_func(name=consts.REDUCE, func=func)
        results = block_submit_unary_unit_job(
            command_client=rp.command_client,
            job=ErJob(
                id=job_id,
                name=consts.REDUCE,
                inputs=[ErJobIO(rp.get_store())],
                functors=[reduce_op],
            ),
            output_types=[ReduceResponse],
        )
        results = [r[0].value for r in results if r[0].value]
        if len(results) == 0:
            return None
        return functools.reduce(func, results)
