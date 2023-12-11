import logging
import queue
import threading
import typing
from collections import defaultdict, namedtuple

from eggroll.computing.tasks import consts, store, job_util, transfer
from eggroll.computing.tasks.submit_utils import block_submit_unary_unit_job
from eggroll.config import Config
from eggroll.core.datastructure.broker import BrokerClosed
from eggroll.core.meta_model import (
    ErJob,
    ErPartition,
    ErTask,
    ErJobIO,
    ErFunctor,
    ErRollSiteHeader,
    ErRollSitePullGetHeaderRequest,
    ErRollSitePullGetHeaderResponse,
    ErRollSitePullGetPartitionStatusRequest,
    ErRollSitePullGetPartitionStatusResponse,
    ErRollSitePullClearStatusRequest,
    ErRollSitePullClearStatusResponse,
)
from ._task import Task, EnvOptions

if typing.TYPE_CHECKING:
    from eggroll.computing.roll_pair import RollPair

L = logging.getLogger(__name__)


class PullGetHeader(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        request = job.first_functor.deserialized_as(ErRollSitePullGetHeaderRequest)
        header = PutBatchTask(tag=request.tag).get_header(timeout=request.timeout)
        return ErRollSitePullGetHeaderResponse(header=header)

    @classmethod
    def submit(cls, rp: "RollPair", tag: str, timeout: float, description: str = None):
        stores = [rp.get_store()]
        job_id = job_util.generate_job_id(
            session_id=rp.session_id,
            tag=consts.PULL_GET_HEADER,
            description=description,
        )
        job = ErJob(
            id=job_id,
            name=consts.PULL_GET_HEADER,
            inputs=[ErJobIO(store) for store in stores],
            functors=[
                ErFunctor(
                    name=consts.PULL_GET_HEADER,
                    body=ErRollSitePullGetHeaderRequest(
                        tag=tag, timeout=timeout
                    ).to_proto_string(),
                )
            ],
            options={},
        )
        responses = block_submit_unary_unit_job(
            rp.command_client, job, [ErRollSitePullGetHeaderResponse]
        )
        results = []
        for response in responses:
            response = response[0]
            results.append(response.header)
        return results


class PullGetPartitionStatus(object):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        request = job.first_functor.deserialized_as(
            ErRollSitePullGetPartitionStatusRequest
        )

        put_batch_task = PutBatchTask(
            tag=f"{request.tag}{task.first_input.id}", partition=None
        )
        status = put_batch_task.get_status(request.timeout)
        return ErRollSitePullGetPartitionStatusResponse(
            partition_id=task.first_input.id,
            tag=status.tag,
            is_finished=status.is_finished,
            total_batches=status.total_batches,
            batch_seq_to_pair_counter=status.batch_seq_to_pair_counter,
            total_streams=status.total_streams,
            stream_seq_to_pair_counter=status.stream_seq_to_pair_counter,
            stream_seq_to_batch_seq=status.stream_seq_to_batch_seq,
            total_pairs=status.total_pairs,
            data_type=status.data_type,
        )

    @classmethod
    def submit(cls, rp: "RollPair", tag: str, timeout: float, description: str = None):
        stores = [rp.get_store()]
        job_id = job_util.generate_job_id(
            session_id=rp.session_id,
            tag=consts.PULL_GET_PARTITION_STATUS,
            description=description,
        )
        job = ErJob(
            id=job_id,
            name=consts.PULL_GET_PARTITION_STATUS,
            inputs=[ErJobIO(store) for store in stores],
            functors=[
                ErFunctor(
                    name=consts.PULL_GET_PARTITION_STATUS,
                    body=ErRollSitePullGetPartitionStatusRequest(
                        tag=tag, timeout=timeout
                    ).to_proto_string(),
                )
            ],
            options={},
        )
        responses = block_submit_unary_unit_job(
            rp.command_client, job, [ErRollSitePullGetPartitionStatusResponse]
        )
        all_finished = True
        pull_status = {}
        total_batches = 0
        total_pairs = 0
        for task_response in responses:
            response = task_response[0]
            if not response.is_finished:
                all_finished = False
            pull_status[response.partition_id] = response
            total_batches += response.total_batches
            total_pairs += response.total_pairs
        return pull_status, all_finished, total_batches, total_pairs


class PullClearStatus(object):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        request = job.first_functor.deserialized_as(ErRollSitePullClearStatusRequest)

        put_batch_task = PutBatchTask(
            tag=f"{request.tag}{task.first_input.id}", partition=None
        )
        status = put_batch_task.clear_status()
        return ErRollSitePullClearStatusResponse()

    @classmethod
    def submit(cls, rp: "RollPair", tag: str, description: str = None):
        stores = [rp.get_store()]
        job_id = job_util.generate_job_id(
            session_id=rp.session_id,
            tag=consts.PULL_CLEAR_STATUS,
            description=description,
        )
        job = ErJob(
            id=job_id,
            name=consts.PULL_CLEAR_STATUS,
            inputs=[ErJobIO(store) for store in stores],
            functors=[
                ErFunctor(
                    name=consts.PULL_CLEAR_STATUS,
                    body=ErRollSitePullClearStatusRequest(tag=tag).to_proto_string(),
                )
            ],
            options={},
        )

        block_submit_unary_unit_job(
            rp.command_client, job, [ErRollSitePullClearStatusResponse]
        )


class PutBatch(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        PutBatchTask(str(task.id), task.first_output).run(
            config=env_options.config, data_dir=env_options.data_dir
        )


class PutBatchTask:

    """
    transfer a total roll_pair by several batch streams
    """

    # tag -> seq -> count

    _class_lock = threading.Lock()
    _partition_lock = defaultdict(threading.Lock)

    def __init__(self, tag, partition: ErPartition = None):
        self.partition = partition
        self.tag = tag

    def run(self, config: Config, data_dir):
        # batch stream must be executed serially, and reinit.
        # TODO:0:  remove lock to bss
        rs_header = None
        with PutBatchTask._partition_lock[self.tag]:
            # tag includes partition info in tag generation
            L.debug(
                f"do_store start for tag={self.tag}, partition_id={self.partition.id}"
            )
            bss = _BatchStreamStatus.get_or_create(self.tag)
            try:
                broker = transfer.TransferService.get_or_create_broker(
                    config=config, key=self.tag, write_signals=1
                )

                iter_wait = 0
                iter_timeout = config.eggroll.core.fifobroker.iter.timeout.sec
                batch_get_interval = 0.1
                with store.get_adapter(
                    self.partition, data_dir
                ) as db, db.new_batch() as wb:
                    # for batch in broker:
                    while not broker.is_closable():
                        try:
                            batch = broker.get(block=True, timeout=batch_get_interval)
                        except queue.Empty as e:
                            iter_wait += batch_get_interval
                            if iter_wait > iter_timeout:
                                raise TimeoutError(
                                    f"timeout in PutBatchTask.run. tag={self.tag}, iter_timeout={iter_timeout}, iter_wait={iter_wait}"
                                )
                            else:
                                continue
                        except BrokerClosed as e:
                            continue

                        iter_wait = 0
                        rs_header = ErRollSiteHeader.from_proto_string(batch.header.ext)
                        batch_pairs = 0
                        if batch.data:
                            bin_data = store.ArrayByteBuffer(batch.data)
                            reader = store.PairBinReader(
                                pair_buffer=bin_data, data=batch.data
                            )
                            for k_bytes, v_bytes in reader.read_all():
                                wb.put(k_bytes, v_bytes)
                                batch_pairs += 1
                        bss.count_batch(rs_header, batch_pairs)
                        # TODO:0
                        bss._data_type = rs_header._data_type
                        if rs_header._stage == consts.FINISH_STATUS:
                            bss.set_done(rs_header)  # starting from 0

                bss.check_finish()
                # TransferService.remove_broker(tag) will be called in get_status phrase finished or exception got
            except Exception as e:
                L.exception(
                    f"_run_put_batch error, tag={self.tag}, "
                    f"rs_key={rs_header.get_rs_key() if rs_header is not None else None}, rs_header={rs_header}"
                )
                raise e
            finally:
                transfer.TransferService.remove_broker(self.tag)

    def get_status(self, timeout):
        return _BatchStreamStatus.wait_finish(self.tag, timeout)

    def get_header(self, timeout):
        return _BatchStreamStatus.wait_header(self.tag, timeout)

    def clear_status(self):
        return _BatchStreamStatus.clear_status(self.tag)


class _BatchStreamStatus:
    _recorder = {}

    _recorder_lock = threading.Lock()

    # Initialisation of this class MUST BE wrapped in lock
    def __init__(self, tag):
        self._tag = tag
        self._stage = "doing"
        self._total_batches = -1
        self._total_streams = -1
        self._data_type = None
        self._batch_seq_to_pair_counter = defaultdict(int)
        self._stream_seq_to_pair_counter = defaultdict(int)
        self._stream_seq_to_batch_seq = defaultdict(int)
        self._stream_finish_event = threading.Event()
        self._header_arrive_event = threading.Event()
        self._rs_header = None
        self._rs_key = None
        # removes lock. otherwise it deadlocks
        self._recorder[self._tag] = self
        self._is_in_order = True
        # self._last_updated_at = None

    def _debug_string(self):
        return (
            f"BatchStreams end normally, tag={self._tag} "
            f"total_batches={self._total_batches}:total_elems={sum(self._batch_seq_to_pair_counter.values())}"
        )

    def set_done(self, rs_header):
        L.debug(f"set done. rs_key={rs_header.get_rs_key()}, rs_header={rs_header}")
        self._total_batches = rs_header.total_batches
        self._total_streams = rs_header.total_streams
        self._stage = "done"
        if self._total_batches != len(self._batch_seq_to_pair_counter):
            self._is_in_order = False
            L.debug(
                f"MarkEnd BatchStream ahead of all BatchStreams received, {self._debug_string()}, rs_key={rs_header.get_rs_key()}, rs_header={rs_header}"
            )

    def count_batch(self, rs_header: ErRollSiteHeader, batch_pairs):
        L.debug(
            f"count batch. rs_key={rs_header.get_rs_key()}, rs_header={rs_header}, batch_pairs={batch_pairs}"
        )
        batch_seq_id = rs_header.batch_seq
        stream_seq_id = rs_header.stream_seq
        if self._rs_header is None:
            self._rs_header = rs_header
            self._rs_key = rs_header.get_rs_key()
            L.debug(
                f"header arrived. rs_key={rs_header.get_rs_key()}, rs_header={rs_header}"
            )
            self._header_arrive_event.set()
        self._batch_seq_to_pair_counter[batch_seq_id] = batch_pairs
        self._stream_seq_to_pair_counter[stream_seq_id] += batch_pairs
        self._stream_seq_to_batch_seq[stream_seq_id] = batch_seq_id

    def check_finish(self):
        if L.isEnabledFor(logging.DEBUG):
            L.debug(
                f"checking finish. rs_key={self._rs_key}, rs_header={self._rs_header}, stage={self._stage}, total_batches={self._total_batches}, len={len(self._batch_seq_to_pair_counter)}"
            )
        if self._stage == "done" and self._total_batches == len(
            self._batch_seq_to_pair_counter
        ):
            L.debug(
                f"All BatchStreams finished, {self._debug_string()}. is_in_order={self._is_in_order}, rs_key={self._rs_key}, rs_header={self._rs_header}"
            )
            self._stream_finish_event.set()
            return True
        else:
            return False

    @classmethod
    def get_or_create(cls, tag):
        with cls._recorder_lock:
            if tag not in cls._recorder:
                bss = _BatchStreamStatus(tag)
            else:
                bss = cls._recorder[tag]
        return bss

    @classmethod
    def wait_finish(cls, tag, timeout) -> "BSS":
        bss = cls.get_or_create(tag)
        finished = bss._stream_finish_event.wait(timeout)
        if finished:
            transfer.TransferService.remove_broker(tag)
            # del cls._recorder[tag]

        return BSS(
            tag=bss._tag,
            is_finished=finished,
            total_batches=bss._total_batches,
            batch_seq_to_pair_counter=bss._batch_seq_to_pair_counter,
            total_streams=bss._total_streams,
            stream_seq_to_pair_counter=bss._stream_seq_to_pair_counter,
            stream_seq_to_batch_seq=bss._stream_seq_to_batch_seq,
            total_pairs=sum(bss._batch_seq_to_pair_counter.values()),
            data_type=bss._data_type,
        )
        # return finished, bss._total_batches, bss._counter, sum(bss._counter.values()), bss._data_type

    @classmethod
    def clear_status(cls, tag):
        with cls._recorder_lock:
            if tag in cls._recorder:
                bss = cls._recorder[tag]
                del cls._recorder[tag]
                return bss._to_tuple()

    @classmethod
    def wait_header(cls, tag, timeout):
        bss = cls.get_or_create(tag)
        bss._header_arrive_event.wait(timeout)
        return bss._rs_header

    def __repr__(self):
        return (
            f"<_BatchStreamStatus(tag={self._tag}, "
            f"stage={self._stage}, "
            f"total_batches={self._total_batches}, "
            f"data_type={self._data_type}, "
            f"batch_seq_to_pair_counter={self._batch_seq_to_pair_counter}, "
            f"stream_seq_to_pair_counter={self._stream_seq_to_pair_counter}, "
            f"stream_seq_to_batch_seq={self._stream_seq_to_batch_seq}, "
            f"stream_finish_event={self._stream_finish_event.is_set()}, "
            f"header_arrive_event={self._header_arrive_event.is_set()}, "
            f"rs_header={self._rs_header}) at {hex(id(self))}>"
        )

    def _to_tuple(self):
        return BSS(
            tag=self._tag,
            is_finished=self._stream_finish_event.is_set(),
            total_batches=self._total_batches,
            batch_seq_to_pair_counter=self._batch_seq_to_pair_counter,
            total_streams=self._total_streams,
            stream_seq_to_pair_counter=self._stream_seq_to_pair_counter,
            stream_seq_to_batch_seq=self._stream_seq_to_batch_seq,
            total_pairs=sum(self._batch_seq_to_pair_counter.values()),
            data_type=self._data_type,
        )


BSS = namedtuple(
    "BSS",
    [
        "tag",
        "is_finished",
        "total_batches",
        "batch_seq_to_pair_counter",
        "total_streams",
        "stream_seq_to_pair_counter",
        "stream_seq_to_batch_seq",
        "total_pairs",
        "data_type",
    ],
)
