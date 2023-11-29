import contextlib
import logging
import time
import typing
from concurrent.futures import Future

from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.meta_model import ErTask, ErJob
from eggroll.core.model.task import MapPartitionsWithIndexRequest
from eggroll.roll_pair.transfer_pair import BatchBroker
from eggroll.roll_pair.transfer_pair import TransferPair
from ._task import Task, EnvOptions

L = logging.getLogger(__name__)


class _MapReducePartitionsWithIndex(Task):
    @classmethod
    def run(cls,
            env_options: EnvOptions,
            job: ErJob, task: ErTask):
        shuffle = job.second_functor.deserialized_as(MapPartitionsWithIndexRequest).shuffle
        map_op = job.first_functor.func
        reduce_op = job.third_functor.func
        if shuffle:
            cls._run_shuffle(env_options, job, task, map_op, reduce_op)
        else:
            cls._run_non_shuffle(env_options, job, task, map_op)

    @classmethod
    def _run_shuffle(cls, env_options: EnvOptions, job: ErJob, task: ErTask, map_op, reduce_op):
        """
        for shuffle, we need to:
        1. shuffle write(input_iterator -> shuffle_write_broker): write input to temp broker for shuffler to read
        2. shuffle gather(shuffle_write_broker -> task output store): read and reduce from shuffler's gather broker
        3. shuffle scatter(shuffle_write_broker -> target partition broker): write temp broker to target partition broker
        """
        task_has_input = task.has_input and (
                env_options.server_node_id is None or task.first_input.is_on_node(env_options.server_node_id)
        )
        task_has_output = task.has_output and (
                env_options.server_node_id is None or task.first_output.is_on_node(env_options.server_node_id)
        )

        features = []
        shuffler = TransferPair(transfer_id=job.id)
        if task_has_output:
            task_output = task.first_output
            # shuffle gather: source partition broker -> task output store
            features.append(
                shuffler.store_broker(
                    data_dir=env_options.data_dir,
                    store_partition=task_output,
                    is_shuffle=True,
                    total_writers=job.first_input.num_partitions,
                    reduce_op=reduce_op,
                )
            )
        if task_has_input:
            with contextlib.ExitStack() as stack:
                shuffle_write_broker = stack.enter_context(FifoBroker())

                # shuffle scatter: shuffle_write_broker -> target partition broker
                output_partitioner = job.first_output.partitioner.load_with_cloudpickle()
                features.append(
                    shuffler.scatter(
                        config=env_options.config,
                        input_broker=shuffle_write_broker,
                        partitioner=output_partitioner,
                        output_store=job.first_output.store,
                    )
                )

                # shuffle write: input_iterator -> shuffle_write_broker
                task_input_iterator = stack.enter_context(
                    stack.enter_context(task.first_input.get_adapter(env_options.data_dir)).iteritems()
                )
                task_shuffle_write_batch_broker = stack.enter_context(BatchBroker(shuffle_write_broker))
                partition_id = task.first_input.id
                value = map_op(partition_id, task_input_iterator)

                if isinstance(value, typing.Iterable):
                    for k1, v1 in value:
                        task_shuffle_write_batch_broker.put((k1, v1))
                else:
                    key = task_input_iterator.key()
                    task_shuffle_write_batch_broker.put((key, value))

        # TODO: cycle through features and check for exceptions
        while True:
            should_break = True
            for feature in features:
                feature: Future
                if feature.done():
                    feature.result()
                else:
                    should_break = False
            if should_break:
                break
            time.sleep(0.1)

    @classmethod
    def _run_non_shuffle(cls, env_options: EnvOptions, job: ErJob, task: ErTask, map_op):
        with contextlib.ExitStack() as stack:
            input_adapter = stack.enter_context(task.first_input.get_adapter(env_options.data_dir))
            input_iterator = stack.enter_context(input_adapter.iteritems())
            output_adapter = stack.enter_context(task.first_output.get_adapter(env_options.data_dir))
            output_write_batch = stack.enter_context(output_adapter.new_batch())
            partition_id = task.id
            value = map_op(partition_id, input_iterator)
            if isinstance(value, typing.Iterable):
                for k1, v1 in value:
                    output_write_batch.put(k1, v1)
            else:
                raise ValueError("mapPartitionsWithIndex must return an iterable")
