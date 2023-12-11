import contextlib
import logging
import typing
from typing import Callable, Iterable

from eggroll.computing.tasks import consts, store, job_util
from eggroll.computing.tasks.submit_utils import block_submit_unary_unit_job
from eggroll.core.meta_model import (
    ErJob,
    ErFunctor,
    ErTask,
    ErJobIO,
)
from ._task import Task, EnvOptions

if typing.TYPE_CHECKING:
    from eggroll.computing.roll_pair import RollPair
L = logging.getLogger(__name__)


class BinarySortedMapPartitionsWithIndex(Task):
    @classmethod
    def run(
        cls,
        env_options: EnvOptions,
        job: ErJob,
        task: ErTask,
    ):
        with contextlib.ExitStack() as stack:
            left_input_read_iterator = stack.enter_context(
                stack.enter_context(
                    store.get_adapter(task.first_input, env_options.data_dir)
                ).iteritems()
            )
            assert left_input_read_iterator.is_sorted(), "left input must be sorted"
            right_input_read_iterator = stack.enter_context(
                stack.enter_context(
                    store.get_adapter(task.second_input, env_options.data_dir)
                ).iteritems()
            )
            assert right_input_read_iterator.is_sorted(), "right input must be sorted"
            output_write_batch = stack.enter_context(
                stack.enter_context(
                    store.get_adapter(task.first_output, env_options.data_dir)
                ).new_batch()
            )
            f = job.first_functor.load_with_cloudpickle()
            for k, v in f(
                task.id, iter(left_input_read_iterator), iter(right_input_read_iterator)
            ):
                output_write_batch.put(k, v)

    @classmethod
    def submit(
        cls,
        left: "RollPair",
        right: "RollPair",
        func: Callable[[int, Iterable, Iterable], Iterable],
    ):
        from eggroll.computing.roll_pair import RollPair

        output_store = left.get_store().fork()
        output_store = left.ctx.session.get_or_create_store(output_store)
        functor = ErFunctor.from_func(
            name=consts.BINARY_SORTED_MAP_PARTITIONS_WITH_INDEX,
            func=func,
        )
        job = ErJob(
            id=job_util.generate_job_id(
                session_id=left.session_id,
                tag=consts.BINARY_SORTED_MAP_PARTITIONS_WITH_INDEX,
            ),
            name=consts.BINARY_SORTED_MAP_PARTITIONS_WITH_INDEX,
            inputs=[ErJobIO(left.get_store()), ErJobIO(right.get_store())],
            outputs=[ErJobIO(output_store)],
            functors=[functor],
        )
        block_submit_unary_unit_job(
            command_client=left.command_client, job=job, output_types=[ErTask]
        )
        return RollPair(output_store, left.ctx)
