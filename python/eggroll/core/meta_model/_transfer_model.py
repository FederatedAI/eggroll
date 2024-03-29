#  Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


from eggroll.core.proto import transfer_pb2
from ._base_model import RpcMessage
from ._utils import _stringify_dict, _repr_bytes


class ErTransferHeader(RpcMessage):
    def __init__(self, id: int, tag: str = "", total_size=-1, status="", ext=b""):
        self._id = id
        self._tag = tag
        self._total_size = total_size
        self._status = status
        self._ext = ext

    def to_proto(self):
        return transfer_pb2.TransferHeader(
            id=self._id,
            tag=self._tag,
            totalSize=self._total_size,
            status=self._status,
            ext=self._ext,
        )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ErTransferHeader(
            id=pb_message.id,
            tag=pb_message.tag,
            total_size=pb_message.totalSize,
            status=pb_message.status,
            ext=pb_message.ext,
        )

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return (
            f"<ErTransferHeader("
            f"id={repr(self._id)}, "
            f"tag={repr(self._tag)}, "
            f"size={repr(self._total_size)}, "
            f"status={repr(self._status)},"
            f"ext={repr(self._ext)[:300]}) "
            f"at {hex(id(self))}>"
        )


class ErTransferBatch(RpcMessage):
    def __init__(self, header: ErTransferHeader, batch_size=-1, data=None):
        self._header = header
        self._batch_size = batch_size
        self._data = data

    def to_proto(self):
        return transfer_pb2.TransferBatch(
            header=self._header.to_proto(), batchSize=self._batch_size, data=self._data
        )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ErTransferBatch(
            header=ErTransferHeader.from_proto(pb_message.header),
            batch_size=pb_message.batchSize,
            data=pb_message.data,
        )

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = transfer_pb2.TransferBatch()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErTransferBatch.from_proto(pb_message)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return (
            f"<ErTransferBatch("
            f"header={repr(self._header)}, "
            f"batch_size={repr(self._batch_size)}, "
            f"data={_repr_bytes(self._data)}) "
            f"at {hex(id(self))}>"
        )


class ErRollSiteHeader(RpcMessage):
    RS_KEY_DELIM = "#"
    RS_KEY_PREFIX = "__rsk"

    def __init__(
        self,
        roll_site_session_id: str,
        name: str,
        tag: str,
        src_role: str,
        src_party_id: str,
        dst_role: str,
        dst_party_id: str,
        data_type: str = "",
        options: dict = None,
        total_partitions: int = -1,
        partition_id: int = -1,
        total_streams: int = -1,
        total_batches: int = -1,
        stream_seq: int = -1,
        batch_seq: int = -1,
        stage: str = "",
    ):
        if options is None:
            options = {}

        self._roll_site_session_id = roll_site_session_id
        self._name = name
        self._tag = tag
        self._src_role = src_role
        self._src_party_id = src_party_id
        self._dst_role = dst_role
        self._dst_party_id = dst_party_id
        self._data_type = data_type
        self._options = options.copy()
        self._total_partitions = total_partitions
        self._partition_id = partition_id
        self._total_streams = total_streams
        self._total_batches = total_batches
        self._stream_seq = stream_seq
        self._batch_seq = batch_seq
        self._stage = stage

    @property
    def batch_seq(self):
        return self._batch_seq

    @property
    def stream_seq(self):
        return self._stream_seq

    @property
    def total_batches(self):
        return self._total_batches

    @property
    def total_streams(self):
        return self._total_streams

    @property
    def partition_id(self):
        return self._partition_id

    def to_proto(self):
        return transfer_pb2.RollSiteHeader(
            rollSiteSessionId=self._roll_site_session_id,
            name=self._name,
            tag=self._tag,
            srcRole=self._src_role,
            srcPartyId=self._src_party_id,
            dstRole=self._dst_role,
            dstPartyId=self._dst_party_id,
            dataType=self._data_type,
            options=_stringify_dict(self._options),
            totalPartitions=self._total_partitions,
            partitionId=self._partition_id,
            totalStreams=self._total_streams,
            totalBatches=self._total_batches,
            streamSeq=self._stream_seq,
            batchSeq=self._batch_seq,
            stage=self._stage,
        )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message: transfer_pb2.RollSiteHeader):
        return ErRollSiteHeader(
            roll_site_session_id=pb_message.rollSiteSessionId,
            name=pb_message.name,
            tag=pb_message.tag,
            src_role=pb_message.srcRole,
            src_party_id=pb_message.srcPartyId,
            dst_role=pb_message.dstRole,
            dst_party_id=pb_message.dstPartyId,
            data_type=pb_message.dataType,
            options=dict(pb_message.options),
            total_partitions=pb_message.totalPartitions,
            partition_id=pb_message.partitionId,
            total_streams=pb_message.totalStreams,
            total_batches=pb_message.totalBatches,
            stream_seq=pb_message.streamSeq,
            batch_seq=pb_message.batchSeq,
            stage=pb_message.stage,
        )

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = transfer_pb2.RollSiteHeader()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErRollSiteHeader.from_proto(pb_message)

    def __repr__(self):
        return (
            f"<ErRollSiteHeader("
            f"roll_site_session_id={repr(self._roll_site_session_id)}, "
            f"name={repr(self._name)}, "
            f"tag={repr(self._tag)}, "
            f"src_role={repr(self._src_role)}, "
            f"src_party_id={repr(self._src_party_id)}, "
            f"dst_role={repr(self._dst_role)}, "
            f"dst_party_id={repr(self._dst_party_id)}, "
            f"data_type={repr(self._data_type)}, "
            f"options=[{repr(self._options)}], "
            f"total_partitions={repr(self._total_partitions)}, "
            f"partition_id={repr(self._partition_id)}, "
            f"total_streams={repr(self._total_streams)}, "
            f"total_batches={self._total_batches}, "
            f"stream_seq={self._stream_seq}, "
            f"batch_seq={self._batch_seq}, "
            f"stage={self._stage}) "
            f"at {hex(id(self))}>"
        )

    def get_rs_key(self):
        return ErRollSiteHeader.RS_KEY_DELIM.join(
            [
                ErRollSiteHeader.RS_KEY_PREFIX,
                self._roll_site_session_id,
                self._name,
                self._tag,
                self._src_role,
                self._src_party_id,
                self._dst_role,
                self._dst_party_id,
            ]
        )


class ErRollSitePullGetHeaderRequest(RpcMessage):
    def __init__(self, tag: str, timeout: float):
        self._tag = tag
        self._timeout = timeout

    @property
    def tag(self):
        return self._tag

    @property
    def timeout(self):
        return self._timeout

    def to_proto(self):
        return transfer_pb2.RollSitePullGetHeaderRequest(
            tag=self._tag, timeout=self._timeout
        )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ErRollSitePullGetHeaderRequest(
            tag=pb_message.tag, timeout=pb_message.timeout
        )

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = transfer_pb2.RollSitePullGetHeaderRequest()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErRollSitePullGetHeaderRequest.from_proto(pb_message)


class ErRollSitePullGetHeaderResponse(RpcMessage):
    def __init__(self, header: ErRollSiteHeader):
        self._header = header

    @property
    def header(self):
        return self._header

    def to_proto(self):
        return transfer_pb2.RollSitePullGetHeaderResponse(
            header=self._header.to_proto()
        )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ErRollSitePullGetHeaderResponse(
            header=ErRollSiteHeader.from_proto(pb_message.header)
        )

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = transfer_pb2.RollSitePullGetHeaderResponse()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErRollSitePullGetHeaderResponse.from_proto(pb_message)


class ErRollSitePullGetPartitionStatusRequest(RpcMessage):
    def __init__(self, tag: str, timeout: float):
        self._tag = tag
        self._timeout = timeout

    @property
    def tag(self):
        return self._tag

    @property
    def timeout(self):
        return self._timeout

    def to_proto(self):
        return transfer_pb2.RollSitePullGetPartitionStatusRequest(
            tag=self._tag, timeout=self._timeout
        )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ErRollSitePullGetPartitionStatusRequest(
            tag=pb_message.tag, timeout=pb_message.timeout
        )

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = transfer_pb2.RollSitePullGetPartitionStatusRequest()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErRollSitePullGetPartitionStatusRequest.from_proto(pb_message)


class ErRollSitePullGetPartitionStatusResponse(RpcMessage):
    def __init__(
        self,
        partition_id: int,
        tag: str,
        is_finished: bool,
        total_batches: int,
        batch_seq_to_pair_counter: dict,
        total_streams: int,
        stream_seq_to_pair_counter: dict,
        stream_seq_to_batch_seq: dict,
        total_pairs: int,
        data_type: str,
    ):
        self._partition_id = partition_id
        self._tag = tag
        self._is_finished = is_finished
        self._total_batches = total_batches
        self._batch_seq_to_pair_counter = batch_seq_to_pair_counter
        self._total_streams = total_streams
        self._stream_seq_to_pair_counter = stream_seq_to_pair_counter
        self._stream_seq_to_batch_seq = stream_seq_to_batch_seq
        self._total_pairs = total_pairs
        self._data_type = data_type

    @property
    def partition_id(self):
        return self._partition_id

    @property
    def tag(self):
        return self._tag

    @property
    def is_finished(self):
        return self._is_finished

    @property
    def total_batches(self):
        return self._total_batches

    @property
    def batch_seq_to_pair_counter(self):
        return self._batch_seq_to_pair_counter

    @property
    def total_streams(self):
        return self._total_streams

    @property
    def stream_seq_to_pair_counter(self):
        return self._stream_seq_to_pair_counter

    @property
    def stream_seq_to_batch_seq(self):
        return self._stream_seq_to_batch_seq

    @property
    def total_pairs(self):
        return self._total_pairs

    @property
    def data_type(self):
        return self._data_type

    def to_proto(self):
        proto = transfer_pb2.RollSitePullGetPartitionStatusResponse(
            partition_id=self._partition_id,
            status=transfer_pb2.RollSitePullGetPartitionStatusResponse.RollSitePullGetPartitionStatusResponseStatus(
                tag=self._tag,
                is_finished=self._is_finished,
                total_batches=self._total_batches,
                total_streams=self._total_streams,
                total_pairs=self._total_pairs,
                data_type=self._data_type,
            ),
        )
        for key, value in self._batch_seq_to_pair_counter.items():
            proto.status.batch_seq_to_pair_counter.add(key=key, value=value)
        for key, value in self._stream_seq_to_pair_counter.items():
            proto.status.stream_seq_to_pair_counter.add(key=key, value=value)
        for key, value in self._stream_seq_to_batch_seq.items():
            proto.status.stream_seq_to_batch_seq.add(key=key, value=value)
        return proto

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message: transfer_pb2.RollSitePullGetPartitionStatusResponse):
        return ErRollSitePullGetPartitionStatusResponse(
            partition_id=pb_message.partition_id,
            tag=pb_message.status.tag,
            is_finished=pb_message.status.is_finished,
            total_batches=pb_message.status.total_batches,
            batch_seq_to_pair_counter=pb_message.status.batch_seq_to_pair_counter,
            total_streams=pb_message.status.total_streams,
            stream_seq_to_pair_counter=pb_message.status.stream_seq_to_pair_counter,
            stream_seq_to_batch_seq=pb_message.status.stream_seq_to_batch_seq,
            total_pairs=pb_message.status.total_pairs,
            data_type=pb_message.status.data_type,
        )

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = transfer_pb2.RollSitePullGetPartitionStatusResponse()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErRollSitePullGetPartitionStatusResponse.from_proto(pb_message)


class ErRollSitePullClearStatusRequest(RpcMessage):
    def __init__(self, tag: str):
        self._tag = tag

    @property
    def tag(self):
        return self._tag

    def to_proto(self):
        return transfer_pb2.RollSitePullClearStatusRequest(tag=self._tag)

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ErRollSitePullClearStatusRequest(tag=pb_message.tag)

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = transfer_pb2.RollSitePullGetPartitionStatusRequest()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErRollSitePullGetPartitionStatusRequest.from_proto(pb_message)


class ErRollSitePullClearStatusResponse(RpcMessage):
    def __init__(self):
        pass

    def to_proto(self):
        return transfer_pb2.RollSitePullClearStatusResponse()

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return ErRollSitePullClearStatusResponse()

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = transfer_pb2.RollSitePullClearStatusResponse()
        msg_len = pb_message.ParseFromString(pb_string)
        return ErRollSitePullClearStatusResponse.from_proto(pb_message)
