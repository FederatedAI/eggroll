import contextlib

from eggroll.core.meta_model import ErTask, ErJob


class _BinarySortedMapPartitionsWithIndex(object):
    @classmethod
    def run(
        cls,
        data_dir: str,
        job: ErJob,
        task: ErTask,
    ):
        with contextlib.ExitStack() as stack:
            left_input_read_iterator = stack.enter_context(
                stack.enter_context(task.first_input.get_adapter(data_dir)).iteritems()
            )
            assert left_input_read_iterator.is_sorted(), "left input must be sorted"
            right_input_read_iterator = stack.enter_context(
                stack.enter_context(task.second_input.get_adapter(data_dir)).iteritems()
            )
            assert right_input_read_iterator.is_sorted(), "right input must be sorted"
            output_write_batch = stack.enter_context(
                stack.enter_context(task.first_output.get_adapter(data_dir)).new_batch()
            )
            f = job.first_functor.load_with_cloudpickle()
            for k, v in f(task.id, iter(left_input_read_iterator), iter(right_input_read_iterator)):
                output_write_batch.put(k, v)
