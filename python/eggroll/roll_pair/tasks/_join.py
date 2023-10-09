import contextlib

from eggroll.core.meta_model import ErTask, ErJob


class _MergeJoin(object):
    @classmethod
    def run(
        cls,
        data_dir: str,
        job: ErJob,
        task: ErTask,
    ):
        with contextlib.ExitStack() as stack:
            left_iterator = stack.enter_context(
                stack.enter_context(task.first_input.get_adapter(data_dir)).iteritems()
            )
            right_iterator = stack.enter_context(
                stack.enter_context(task.second_input.get_adapter(data_dir)).iteritems()
            )
            output_write_batch = stack.enter_context(
                stack.enter_context(task.first_output.get_adapter(data_dir)).new_batch()
            )
            try:
                assert left_iterator.is_sorted(), "left input must be sorted"
                assert right_iterator.is_sorted(), "right input must be sorted"

                f = job.first_functor.func

                l_iter = iter(left_iterator)
                r_iter = iter(right_iterator)

                try:
                    left_key_bytes, left_value_bytes = next(l_iter)
                    right_key_bytes, right_value_bytes = next(r_iter)
                    while True:
                        while right_key_bytes < left_key_bytes:
                            right_key_bytes, right_value_bytes = next(r_iter)

                        while left_key_bytes < right_key_bytes:
                            left_key_bytes, left_value_bytes = next(l_iter)

                        if left_key_bytes == right_key_bytes:
                            output_bytes = f(left_value_bytes, right_value_bytes)
                            output_write_batch.put(left_key_bytes, output_bytes)
                            left_key_bytes, left_value_bytes = next(l_iter)
                except StopIteration as e:
                    return
            except Exception as e:
                raise EnvironmentError("exec task:{} error".format(task), e) from e


class _HashJoin:
    @classmethod
    def run(
        cls,
        data_dir: str,
        job: ErJob,
        task: ErTask,
    ):
        with contextlib.ExitStack() as stack:
            left_iterator = stack.enter_context(
                stack.enter_context(task.first_input.get_adapter(data_dir)).iteritems()
            )
            right_iterator = stack.enter_context(
                stack.enter_context(task.second_input.get_adapter(data_dir)).iteritems()
            )
            output_write_batch = stack.enter_context(
                stack.enter_context(task.first_output.get_adapter(data_dir)).new_batch()
            )
            try:
                f = job.first_functor.func
                left_iter = iter(left_iterator)
                for k_left, l_v_bytes in left_iter:
                    r_v_bytes = right_iterator.adapter.get(k_left)
                    if r_v_bytes:
                        output_write_batch.put(k_left, f(l_v_bytes, r_v_bytes)),
            except Exception as e:
                raise EnvironmentError("exec task:{} error".format(task), e) from e
