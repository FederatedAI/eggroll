# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: transfer.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0etransfer.proto\x12 com.webank.eggroll.core.transfer\"Y\n\x0eTransferHeader\x12\n\n\x02id\x18\x01 \x01(\x05\x12\x0b\n\x03tag\x18\x02 \x01(\t\x12\x11\n\ttotalSize\x18\x03 \x01(\x03\x12\x0e\n\x06status\x18\x04 \x01(\t\x12\x0b\n\x03\x65xt\x18\x05 \x01(\x0c\"r\n\rTransferBatch\x12@\n\x06header\x18\x01 \x01(\x0b\x32\x30.com.webank.eggroll.core.transfer.TransferHeader\x12\x11\n\tbatchSize\x18\x02 \x01(\x03\x12\x0c\n\x04\x64\x61ta\x18\x03 \x01(\x0c\"\xb0\x03\n\x0eRollSiteHeader\x12\x19\n\x11rollSiteSessionId\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x0b\n\x03tag\x18\x03 \x01(\t\x12\x0f\n\x07srcRole\x18\x04 \x01(\t\x12\x12\n\nsrcPartyId\x18\x05 \x01(\t\x12\x0f\n\x07\x64stRole\x18\x06 \x01(\t\x12\x12\n\ndstPartyId\x18\x07 \x01(\t\x12\x10\n\x08\x64\x61taType\x18\x08 \x01(\t\x12N\n\x07options\x18\n \x03(\x0b\x32=.com.webank.eggroll.core.transfer.RollSiteHeader.OptionsEntry\x12\x17\n\x0ftotalPartitions\x18\x0f \x01(\x05\x12\x13\n\x0bpartitionId\x18\x10 \x01(\x05\x12\x14\n\x0ctotalStreams\x18\x11 \x01(\x03\x12\x14\n\x0ctotalBatches\x18\x12 \x01(\x03\x12\x11\n\tstreamSeq\x18\x14 \x01(\x03\x12\x10\n\x08\x62\x61tchSeq\x18\x15 \x01(\x03\x12\r\n\x05stage\x18\x1e \x01(\t\x1a.\n\x0cOptionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"<\n\x1cRollSitePullGetHeaderRequest\x12\x0b\n\x03tag\x18\x01 \x01(\t\x12\x0f\n\x07timeout\x18\x02 \x01(\x01\"a\n\x1dRollSitePullGetHeaderResponse\x12@\n\x06header\x18\x01 \x01(\x0b\x32\x30.com.webank.eggroll.core.transfer.RollSiteHeader\"E\n%RollSitePullGetPartitionStatusRequest\x12\x0b\n\x03tag\x18\x01 \x01(\t\x12\x0f\n\x07timeout\x18\x02 \x01(\x01\"\xa0\x06\n&RollSitePullGetPartitionStatusResponse\x12\x14\n\x0cpartition_id\x18\x01 \x01(\x03\x12\x85\x01\n\x06status\x18\x02 \x01(\x0b\x32u.com.webank.eggroll.core.transfer.RollSitePullGetPartitionStatusResponse.RollSitePullGetPartitionStatusResponseStatus\x1a\x30\n\x12IntKeyIntValuePair\x12\x0b\n\x03key\x18\x01 \x01(\x03\x12\r\n\x05value\x18\x02 \x01(\x03\x1a\xa5\x04\n,RollSitePullGetPartitionStatusResponseStatus\x12\x0b\n\x03tag\x18\x01 \x01(\t\x12\x13\n\x0bis_finished\x18\x02 \x01(\x08\x12\x15\n\rtotal_batches\x18\x03 \x01(\x03\x12~\n\x19\x62\x61tch_seq_to_pair_counter\x18\x04 \x03(\x0b\x32[.com.webank.eggroll.core.transfer.RollSitePullGetPartitionStatusResponse.IntKeyIntValuePair\x12\x15\n\rtotal_streams\x18\x05 \x01(\x03\x12\x7f\n\x1astream_seq_to_pair_counter\x18\x06 \x03(\x0b\x32[.com.webank.eggroll.core.transfer.RollSitePullGetPartitionStatusResponse.IntKeyIntValuePair\x12|\n\x17stream_seq_to_batch_seq\x18\x07 \x03(\x0b\x32[.com.webank.eggroll.core.transfer.RollSitePullGetPartitionStatusResponse.IntKeyIntValuePair\x12\x13\n\x0btotal_pairs\x18\x08 \x01(\x03\x12\x11\n\tdata_type\x18\t \x01(\t\"-\n\x1eRollSitePullClearStatusRequest\x12\x0b\n\x03tag\x18\x01 \x01(\t\"!\n\x1fRollSitePullClearStatusResponse2\xdb\x02\n\x0fTransferService\x12j\n\x04send\x12/.com.webank.eggroll.core.transfer.TransferBatch\x1a/.com.webank.eggroll.core.transfer.TransferBatch(\x01\x12j\n\x04recv\x12/.com.webank.eggroll.core.transfer.TransferBatch\x1a/.com.webank.eggroll.core.transfer.TransferBatch0\x01\x12p\n\x08sendRecv\x12/.com.webank.eggroll.core.transfer.TransferBatch\x1a/.com.webank.eggroll.core.transfer.TransferBatch(\x01\x30\x01\x62\x06proto3')



_TRANSFERHEADER = DESCRIPTOR.message_types_by_name['TransferHeader']
_TRANSFERBATCH = DESCRIPTOR.message_types_by_name['TransferBatch']
_ROLLSITEHEADER = DESCRIPTOR.message_types_by_name['RollSiteHeader']
_ROLLSITEHEADER_OPTIONSENTRY = _ROLLSITEHEADER.nested_types_by_name['OptionsEntry']
_ROLLSITEPULLGETHEADERREQUEST = DESCRIPTOR.message_types_by_name['RollSitePullGetHeaderRequest']
_ROLLSITEPULLGETHEADERRESPONSE = DESCRIPTOR.message_types_by_name['RollSitePullGetHeaderResponse']
_ROLLSITEPULLGETPARTITIONSTATUSREQUEST = DESCRIPTOR.message_types_by_name['RollSitePullGetPartitionStatusRequest']
_ROLLSITEPULLGETPARTITIONSTATUSRESPONSE = DESCRIPTOR.message_types_by_name['RollSitePullGetPartitionStatusResponse']
_ROLLSITEPULLGETPARTITIONSTATUSRESPONSE_INTKEYINTVALUEPAIR = _ROLLSITEPULLGETPARTITIONSTATUSRESPONSE.nested_types_by_name['IntKeyIntValuePair']
_ROLLSITEPULLGETPARTITIONSTATUSRESPONSE_ROLLSITEPULLGETPARTITIONSTATUSRESPONSESTATUS = _ROLLSITEPULLGETPARTITIONSTATUSRESPONSE.nested_types_by_name['RollSitePullGetPartitionStatusResponseStatus']
_ROLLSITEPULLCLEARSTATUSREQUEST = DESCRIPTOR.message_types_by_name['RollSitePullClearStatusRequest']
_ROLLSITEPULLCLEARSTATUSRESPONSE = DESCRIPTOR.message_types_by_name['RollSitePullClearStatusResponse']
TransferHeader = _reflection.GeneratedProtocolMessageType('TransferHeader', (_message.Message,), {
  'DESCRIPTOR' : _TRANSFERHEADER,
  '__module__' : 'transfer_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.transfer.TransferHeader)
  })
_sym_db.RegisterMessage(TransferHeader)

TransferBatch = _reflection.GeneratedProtocolMessageType('TransferBatch', (_message.Message,), {
  'DESCRIPTOR' : _TRANSFERBATCH,
  '__module__' : 'transfer_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.transfer.TransferBatch)
  })
_sym_db.RegisterMessage(TransferBatch)

RollSiteHeader = _reflection.GeneratedProtocolMessageType('RollSiteHeader', (_message.Message,), {

  'OptionsEntry' : _reflection.GeneratedProtocolMessageType('OptionsEntry', (_message.Message,), {
    'DESCRIPTOR' : _ROLLSITEHEADER_OPTIONSENTRY,
    '__module__' : 'transfer_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.transfer.RollSiteHeader.OptionsEntry)
    })
  ,
  'DESCRIPTOR' : _ROLLSITEHEADER,
  '__module__' : 'transfer_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.transfer.RollSiteHeader)
  })
_sym_db.RegisterMessage(RollSiteHeader)
_sym_db.RegisterMessage(RollSiteHeader.OptionsEntry)

RollSitePullGetHeaderRequest = _reflection.GeneratedProtocolMessageType('RollSitePullGetHeaderRequest', (_message.Message,), {
  'DESCRIPTOR' : _ROLLSITEPULLGETHEADERREQUEST,
  '__module__' : 'transfer_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.transfer.RollSitePullGetHeaderRequest)
  })
_sym_db.RegisterMessage(RollSitePullGetHeaderRequest)

RollSitePullGetHeaderResponse = _reflection.GeneratedProtocolMessageType('RollSitePullGetHeaderResponse', (_message.Message,), {
  'DESCRIPTOR' : _ROLLSITEPULLGETHEADERRESPONSE,
  '__module__' : 'transfer_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.transfer.RollSitePullGetHeaderResponse)
  })
_sym_db.RegisterMessage(RollSitePullGetHeaderResponse)

RollSitePullGetPartitionStatusRequest = _reflection.GeneratedProtocolMessageType('RollSitePullGetPartitionStatusRequest', (_message.Message,), {
  'DESCRIPTOR' : _ROLLSITEPULLGETPARTITIONSTATUSREQUEST,
  '__module__' : 'transfer_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.transfer.RollSitePullGetPartitionStatusRequest)
  })
_sym_db.RegisterMessage(RollSitePullGetPartitionStatusRequest)

RollSitePullGetPartitionStatusResponse = _reflection.GeneratedProtocolMessageType('RollSitePullGetPartitionStatusResponse', (_message.Message,), {

  'IntKeyIntValuePair' : _reflection.GeneratedProtocolMessageType('IntKeyIntValuePair', (_message.Message,), {
    'DESCRIPTOR' : _ROLLSITEPULLGETPARTITIONSTATUSRESPONSE_INTKEYINTVALUEPAIR,
    '__module__' : 'transfer_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.transfer.RollSitePullGetPartitionStatusResponse.IntKeyIntValuePair)
    })
  ,

  'RollSitePullGetPartitionStatusResponseStatus' : _reflection.GeneratedProtocolMessageType('RollSitePullGetPartitionStatusResponseStatus', (_message.Message,), {
    'DESCRIPTOR' : _ROLLSITEPULLGETPARTITIONSTATUSRESPONSE_ROLLSITEPULLGETPARTITIONSTATUSRESPONSESTATUS,
    '__module__' : 'transfer_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.transfer.RollSitePullGetPartitionStatusResponse.RollSitePullGetPartitionStatusResponseStatus)
    })
  ,
  'DESCRIPTOR' : _ROLLSITEPULLGETPARTITIONSTATUSRESPONSE,
  '__module__' : 'transfer_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.transfer.RollSitePullGetPartitionStatusResponse)
  })
_sym_db.RegisterMessage(RollSitePullGetPartitionStatusResponse)
_sym_db.RegisterMessage(RollSitePullGetPartitionStatusResponse.IntKeyIntValuePair)
_sym_db.RegisterMessage(RollSitePullGetPartitionStatusResponse.RollSitePullGetPartitionStatusResponseStatus)

RollSitePullClearStatusRequest = _reflection.GeneratedProtocolMessageType('RollSitePullClearStatusRequest', (_message.Message,), {
  'DESCRIPTOR' : _ROLLSITEPULLCLEARSTATUSREQUEST,
  '__module__' : 'transfer_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.transfer.RollSitePullClearStatusRequest)
  })
_sym_db.RegisterMessage(RollSitePullClearStatusRequest)

RollSitePullClearStatusResponse = _reflection.GeneratedProtocolMessageType('RollSitePullClearStatusResponse', (_message.Message,), {
  'DESCRIPTOR' : _ROLLSITEPULLCLEARSTATUSRESPONSE,
  '__module__' : 'transfer_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.transfer.RollSitePullClearStatusResponse)
  })
_sym_db.RegisterMessage(RollSitePullClearStatusResponse)

_TRANSFERSERVICE = DESCRIPTOR.services_by_name['TransferService']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _ROLLSITEHEADER_OPTIONSENTRY._options = None
  _ROLLSITEHEADER_OPTIONSENTRY._serialized_options = b'8\001'
  _TRANSFERHEADER._serialized_start=52
  _TRANSFERHEADER._serialized_end=141
  _TRANSFERBATCH._serialized_start=143
  _TRANSFERBATCH._serialized_end=257
  _ROLLSITEHEADER._serialized_start=260
  _ROLLSITEHEADER._serialized_end=692
  _ROLLSITEHEADER_OPTIONSENTRY._serialized_start=646
  _ROLLSITEHEADER_OPTIONSENTRY._serialized_end=692
  _ROLLSITEPULLGETHEADERREQUEST._serialized_start=694
  _ROLLSITEPULLGETHEADERREQUEST._serialized_end=754
  _ROLLSITEPULLGETHEADERRESPONSE._serialized_start=756
  _ROLLSITEPULLGETHEADERRESPONSE._serialized_end=853
  _ROLLSITEPULLGETPARTITIONSTATUSREQUEST._serialized_start=855
  _ROLLSITEPULLGETPARTITIONSTATUSREQUEST._serialized_end=924
  _ROLLSITEPULLGETPARTITIONSTATUSRESPONSE._serialized_start=927
  _ROLLSITEPULLGETPARTITIONSTATUSRESPONSE._serialized_end=1727
  _ROLLSITEPULLGETPARTITIONSTATUSRESPONSE_INTKEYINTVALUEPAIR._serialized_start=1127
  _ROLLSITEPULLGETPARTITIONSTATUSRESPONSE_INTKEYINTVALUEPAIR._serialized_end=1175
  _ROLLSITEPULLGETPARTITIONSTATUSRESPONSE_ROLLSITEPULLGETPARTITIONSTATUSRESPONSESTATUS._serialized_start=1178
  _ROLLSITEPULLGETPARTITIONSTATUSRESPONSE_ROLLSITEPULLGETPARTITIONSTATUSRESPONSESTATUS._serialized_end=1727
  _ROLLSITEPULLCLEARSTATUSREQUEST._serialized_start=1729
  _ROLLSITEPULLCLEARSTATUSREQUEST._serialized_end=1774
  _ROLLSITEPULLCLEARSTATUSRESPONSE._serialized_start=1776
  _ROLLSITEPULLCLEARSTATUSRESPONSE._serialized_end=1809
  _TRANSFERSERVICE._serialized_start=1812
  _TRANSFERSERVICE._serialized_end=2159
# @@protoc_insertion_point(module_scope)