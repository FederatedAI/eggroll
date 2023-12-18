import contextlib
import logging
import typing
import uuid

from eggroll.computing.tasks import consts, store, job_util, transfer
from eggroll.computing.tasks.submit_utils import block_submit_unary_unit_job
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.meta_model import (
    ErJob,
    ErFunctor,
    ErTask,
    ErPartitioner,
    ErJobIO,
    ErSerdes,
    MapPartitionsWithIndexRequest,
)
from ._task import Task, EnvOptions

if typing.TYPE_CHECKING:
    from eggroll.computing.roll_pair import RollPair
L = logging.getLogger(__name__)


class MapReducePartitionsWithIndex(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        shuffle = job.second_functor.deserialized_as(
            MapPartitionsWithIndexRequest
        ).shuffle
        map_op = job.first_functor.func
        reduce_op = job.third_functor.func
        if shuffle:
            cls._run_shuffle(env_options, job, task, map_op, reduce_op)
        else:
            cls._run_non_shuffle(env_options, job, task, map_op)

    @classmethod
    def submit(
        cls,
        rp: "RollPair",
        map_partition_op,
        reduce_partition_op,
        shuffle,
        input_key_serdes,
        input_key_serdes_type,
        input_value_serdes,
        input_value_serdes_type,
        input_partitioner,
        input_partitioner_type,
        output_key_serdes,
        output_key_serdes_type,
        output_value_serdes,
        output_value_serdes_type,
        output_partitioner,
        output_partitioner_type,
        output_num_partitions,
        output_namespace=None,
        output_name=None,
        output_store_type=None,
    ):
        from eggroll.computing.roll_pair import RollPair

        if output_namespace is None:
            output_namespace = rp.get_namespace()
        if output_name is None:
            output_name = str(uuid.uuid1())
        if output_store_type is None:
            output_store_type = rp.get_store_type()
        if not shuffle:
            if input_key_serdes_type != output_key_serdes_type:
                raise ValueError("key serdes type changed without shuffle")
            if rp.num_partitions != output_num_partitions:
                raise ValueError("partition num changed without shuffle")
            if input_partitioner_type != output_partitioner_type:
                raise ValueError("partitioner type changed without shuffle")

        map_functor = ErFunctor.from_func(
            name=consts.MAP_REDUCE_PARTITIONS_WITH_INDEX, func=map_partition_op
        )
        reduce_functor = ErFunctor.from_func(
            name=consts.MAP_REDUCE_PARTITIONS_WITH_INDEX, func=reduce_partition_op
        )
        shuffle = ErFunctor(
            name=f"{consts.MAP_REDUCE_PARTITIONS_WITH_INDEX}_{shuffle}",
            body=MapPartitionsWithIndexRequest(shuffle=shuffle).to_proto_string(),
        )
        job_id = job_util.generate_job_id(
            session_id=rp.session_id, tag=consts.MAP_REDUCE_PARTITIONS_WITH_INDEX
        )
        output_store = rp.ctx.create_store(
            id=rp.get_store().store_locator.id,
            name=output_name,
            namespace=output_namespace,
            total_partitions=output_num_partitions,
            store_type=output_store_type,
            key_serdes_type=output_key_serdes_type,
            value_serdes_type=output_value_serdes_type,
            partitioner_type=output_partitioner_type,
            options={},
        )
        output_store = rp.ctx.session.get_or_create_store(output_store)
        job = ErJob(
            id=job_id,
            name=consts.MAP_REDUCE_PARTITIONS_WITH_INDEX,
            inputs=[
                ErJobIO(
                    rp.get_store(),
                    key_serdes=ErSerdes.from_func(
                        input_key_serdes_type, input_key_serdes
                    ),
                    value_serdes=ErSerdes.from_func(
                        input_value_serdes_type, input_value_serdes
                    ),
                    partitioner=ErPartitioner.from_func(
                        input_partitioner_type, input_partitioner
                    ),
                )
            ],
            outputs=[
                ErJobIO(
                    output_store,
                    key_serdes=ErSerdes.from_func(
                        output_key_serdes_type, output_key_serdes
                    ),
                    value_serdes=ErSerdes.from_func(
                        output_value_serdes_type, output_value_serdes
                    ),
                    partitioner=ErPartitioner.from_func(
                        output_partitioner_type, output_partitioner
                    ),
                )
            ],
            functors=[map_functor, shuffle, reduce_functor],
        )

        block_submit_unary_unit_job(
            command_client=rp.command_client, job=job, output_types=[ErTask]
        )
        return RollPair(output_store, rp.ctx)

    @classmethod
    def _run_shuffle(
        cls, env_options: EnvOptions, job: ErJob, task: ErTask, map_op, reduce_op
    ):
        """
        for shuffle, we need to:
        1. shuffle write(input_iterator -> shuffle_write_broker): write input to temp broker for shuffler to read
        2. shuffle gather(shuffle_write_broker -> task output store): read and reduce from shuffler's gather broker
        3. shuffle scatter(shuffle_write_broker -> target partition broker): write temp broker to target partition broker
        """
        task_has_input = task.has_input and (
            env_options.server_node_id is None
            or task.first_input.is_on_node(env_options.server_node_id)
        )
        task_has_output = task.has_output and (
            env_options.server_node_id is None
            or task.first_output.is_on_node(env_options.server_node_id)
        )

        shuffler = transfer.TransferPair(config=env_options.config, transfer_id=job.id)
        store_broker_future = None
        if task_has_output:
            task_output = task.first_output
            # shuffle gather: source partition broker -> task output store
            store_broker_future = shuffler.store_broker(
                config=env_options.config,
                data_dir=env_options.data_dir,
                store_partition=task_output,
                is_shuffle=True,
                total_writers=job.first_input.num_partitions,
                reduce_op=reduce_op,
            )
        if task_has_input:
            with contextlib.ExitStack() as stack:
                shuffle_write_broker = stack.enter_context(
                    FifoBroker(config=env_options.config)
                )

                # shuffle scatter: shuffle_write_broker -> target partition broker
                output_partitioner = (
                    job.first_output.partitioner.load_with_cloudpickle()
                )
                scatter_future = shuffler.scatter(
                    config=env_options.config,
                    input_broker=shuffle_write_broker,
                    partitioner=output_partitioner,
                    output_store=job.first_output.store,
                )

                # shuffle write: input_iterator -> shuffle_write_broker
                task_input_iterator = stack.enter_context(
                    stack.enter_context(
                        store.get_adapter(task.first_input, env_options.data_dir)
                    ).iteritems()
                )
                task_shuffle_write_batch_broker = stack.enter_context(
                    transfer.BatchBroker(
                        config=env_options.config, broker=shuffle_write_broker
                    )
                )
                partition_id = task.first_input.id
                value = map_op(partition_id, task_input_iterator)

                if isinstance(value, typing.Iterable):
                    for k1, v1 in value:
                        task_shuffle_write_batch_broker.put((k1, v1))
                else:
                    key = task_input_iterator.key()
                    task_shuffle_write_batch_broker.put((key, value))

            scatter_future.result()
        if store_broker_future is not None:
            store_broker_future.result()
        # TODO: sp3: check exceptions
        # # TODO: cycle through features and check for exceptions
        # while True:
        #     should_break = True
        #     for feature in features:
        #         feature: Future
        #         if feature.done():
        #             feature.result()
        #         else:
        #             should_break = False
        #     if should_break:
        #         break
        #     time.sleep(0.001)

    @classmethod
    def _run_non_shuffle(
        cls, env_options: EnvOptions, job: ErJob, task: ErTask, map_op
    ):
        with contextlib.ExitStack() as stack:
            input_adapter = stack.enter_context(
                store.get_adapter(task.first_input, env_options.data_dir)
            )
            input_iterator = stack.enter_context(input_adapter.iteritems())
            output_adapter = stack.enter_context(
                store.get_adapter(task.first_output, env_options.data_dir)
            )
            output_write_batch = stack.enter_context(output_adapter.new_batch())
            partition_id = task.first_input.id
            value = map_op(partition_id, input_iterator)
            if isinstance(value, typing.Iterable):
                for k1, v1 in value:
                    output_write_batch.put(k1, v1)
            else:
                raise ValueError("mapPartitionsWithIndex must return an iterable")
