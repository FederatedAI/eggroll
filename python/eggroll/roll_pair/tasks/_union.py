import contextlib

from eggroll.core.meta_model import ErTask, ErJob


class _MergeUnion(object):
    @classmethod
    def run(
        cls,
        data_dir: str,
        task: ErTask,
        job: ErJob,
    ):
        with contextlib.ExitStack() as stack:
            left_input_read_iterator = stack.enter_context(
                stack.enter_context(task.first_input.get_adapter(data_dir)).iteritems()
            )
            right_input_read_iterator = stack.enter_context(
                stack.enter_context(task.second_input.get_adapter(data_dir)).iteritems()
            )
            output_write_batch = stack.enter_context(
                stack.enter_context(task.first_output.get_adapter(data_dir)).new_batch()
            )
            assert left_input_read_iterator.is_sorted(), "left input must be sorted"
            assert right_input_read_iterator.is_sorted(), "right input must be sorted"
            f = job.first_functor.func

            l_iter = iter(left_input_read_iterator)
            r_iter = iter(right_input_read_iterator)
            k_left = None
            v_left_bytes = None
            k_right = None
            v_right_bytes = None
            none_none = (None, None)

            is_left_stopped = False
            is_equal = False
            try:
                k_left, v_left_bytes = next(l_iter, none_none)
                k_right, v_right_bytes = next(r_iter, none_none)
                while True:
                    while k_right < k_left:
                        output_write_batch.put(k_right, v_right_bytes)
                        k_right, v_right_bytes = next(r_iter)

                    while k_left < k_right:
                        output_write_batch.put(k_left, v_left_bytes)
                        k_left, v_left_bytes = next(l_iter)

                    if k_left == k_right:
                        output_write_batch.put(k_left, f(v_left_bytes, v_right_bytes))
                        k_left, v_left_bytes = next(l_iter)
                        k_right, v_right_bytes = next(r_iter)
            except StopIteration as e:
                pass

            if not is_left_stopped:
                try:
                    output_write_batch.put(k_left, v_left_bytes)
                    while True:
                        k_left, v_left_bytes = next(l_iter)
                        output_write_batch.put(k_left, v_left_bytes)
                except StopIteration as e:
                    pass
            else:
                try:
                    if not is_equal:
                        if is_same_serdes:
                            output_writebatch.put(k_right_raw, v_right_bytes)
                        else:
                            output_writebatch.put(
                                left_key_serdes.serialize(right_key_serdes.deserialize(k_right_raw)),
                                left_value_serdes.serialize(right_value_serdes.deserialize(v_right_bytes)),
                            )
                    while True:
                        k_right_raw, v_right_bytes = next(r_iter)
                        if is_same_serdes:
                            output_writebatch.put(k_right_raw, v_right_bytes)
                        else:
                            output_writebatch.put(
                                left_key_serdes.serialize(right_key_serdes.deserialize(k_right_raw)),
                                left_value_serdes.serialize(right_value_serdes.deserialize(v_right_bytes)),
                            )
                except StopIteration as e:
                    pass

                # end of merge union wrapper
                return


class _HashUnion(object):
    @classmethod
    def _run_binary(
        cls,
        task: ErTask,
        job: ErJob,
        left_input_read_iterator,
        right_input_read_iterator,
        output_write_batch_broker,
        left_key_serdes,
        left_value_serdes,
        right_key_serdes,
        right_value_serdes,
        output_key_serdes,
        output_value_serdes,
    ):
        union_type = task._job._options.get("union_type", "merge")

        raise NotImplementedError(f"{cls.__name__}._run_binary")

    def hash_union_wrapper(
        left_iterator,
        left_key_serdes,
        left_value_serdes,
        right_iterator,
        right_key_serdes,
        right_value_serdes,
        output_writebatch,
    ):
        f = create_functor(functors[0]._body)

        is_diff_serdes = type(left_key_serdes) != type(right_key_serdes)
        for k_left, v_left in left_iterator:
            if is_diff_serdes:
                k_left = right_key_serdes.serialize(left_key_serdes.deserialize(k_left))
            v_right = right_iterator.adapter.get(k_left)
            if v_right is None:
                output_writebatch.put(k_left, v_left)
            else:
                v_final = f(left_value_serdes.deserialize(v_left), right_value_serdes.deserialize(v_right))
                output_writebatch.put(k_left, left_value_serdes.serialize(v_final))

        right_iterator.first()
        for k_right, v_right in right_iterator:
            if is_diff_serdes:
                final_v_bytes = output_writebatch.get(left_key_serdes.serialize(right_key_serdes.deserialize(k_right)))
            else:
                final_v_bytes = output_writebatch.get(k_right)

            if final_v_bytes is None:
                output_writebatch.put(k_right, v_right)
