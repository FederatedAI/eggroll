import contextlib
import logging

from eggroll.core.meta_model import ErTask, ErJob
from ._task import Task, EnvOptions

L = logging.getLogger(__name__)


class _BinarySortedMapPartitionsWithIndex(Task):
    @classmethod
    def run(
            cls,
            env_options: EnvOptions,
            job: ErJob,
            task: ErTask,
    ):
        with contextlib.ExitStack() as stack:
            left_input_read_iterator = stack.enter_context(
                stack.enter_context(task.first_input.get_adapter(env_options.data_dir)).iteritems()
            )
            assert left_input_read_iterator.is_sorted(), "left input must be sorted"
            right_input_read_iterator = stack.enter_context(
                stack.enter_context(task.second_input.get_adapter(env_options.data_dir)).iteritems()
            )
            assert right_input_read_iterator.is_sorted(), "right input must be sorted"
            output_write_batch = stack.enter_context(
                stack.enter_context(task.first_output.get_adapter(env_options.data_dir)).new_batch()
            )
            f = job.first_functor.load_with_cloudpickle()
            for k, v in f(task.id, iter(left_input_read_iterator), iter(right_input_read_iterator)):
                output_write_batch.put(k, v)
