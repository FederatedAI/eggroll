# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: deepspeed_download.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
import meta_pb2 as meta__pb2
import containers_pb2 as containers__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x18\x64\x65\x65pspeed_download.proto\x12\x1c\x63om.webank.eggroll.core.meta\x1a\x1egoogle/protobuf/duration.proto\x1a\nmeta.proto\x1a\x10\x63ontainers.proto\"\xad\x01\n\x16PrepareDownloadRequest\x12\x12\n\nsession_id\x18\x01 \x01(\t\x12\r\n\x05ranks\x18\x02 \x03(\x05\x12\x17\n\x0f\x63ompress_method\x18\x03 \x01(\t\x12\x16\n\x0e\x63ompress_level\x18\x04 \x01(\x05\x12?\n\x0c\x63ontent_type\x18\x05 \x01(\x0e\x32).com.webank.eggroll.core.meta.ContentType\">\n\x17PrepareDownloadResponse\x12\x12\n\nsession_id\x18\x01 \x01(\t\x12\x0f\n\x07\x63ontent\x18\x02 \x01(\t\"\xa8\x01\n\x11\x44sDownloadRequest\x12\x12\n\nsession_id\x18\x01 \x01(\t\x12\r\n\x05ranks\x18\x02 \x03(\x05\x12\x17\n\x0f\x63ompress_method\x18\x03 \x01(\t\x12\x16\n\x0e\x63ompress_level\x18\x04 \x01(\x05\x12?\n\x0c\x63ontent_type\x18\x05 \x01(\x0e\x32).com.webank.eggroll.core.meta.ContentType\"s\n\x12\x44sDownloadResponse\x12\x12\n\nsession_id\x18\x01 \x01(\t\x12I\n\x11\x63ontainer_content\x18\x02 \x03(\x0b\x32..com.webank.eggroll.core.meta.ContainerContent\"5\n\x17\x44sDownloadSplitResponse\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\x0c\x12\x0c\n\x04rank\x18\x02 \x01(\x05\x32\x85\x02\n\x11\x44sDownloadService\x12o\n\x08\x64ownload\x12/.com.webank.eggroll.core.meta.DsDownloadRequest\x1a\x30.com.webank.eggroll.core.meta.DsDownloadResponse\"\x00\x12\x7f\n\x11\x64ownload_by_split\x12/.com.webank.eggroll.core.meta.DsDownloadRequest\x1a\x35.com.webank.eggroll.core.meta.DsDownloadSplitResponse\"\x00\x30\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'deepspeed_download_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_PREPAREDOWNLOADREQUEST']._serialized_start=121
  _globals['_PREPAREDOWNLOADREQUEST']._serialized_end=294
  _globals['_PREPAREDOWNLOADRESPONSE']._serialized_start=296
  _globals['_PREPAREDOWNLOADRESPONSE']._serialized_end=358
  _globals['_DSDOWNLOADREQUEST']._serialized_start=361
  _globals['_DSDOWNLOADREQUEST']._serialized_end=529
  _globals['_DSDOWNLOADRESPONSE']._serialized_start=531
  _globals['_DSDOWNLOADRESPONSE']._serialized_end=646
  _globals['_DSDOWNLOADSPLITRESPONSE']._serialized_start=648
  _globals['_DSDOWNLOADSPLITRESPONSE']._serialized_end=701
  _globals['_DSDOWNLOADSERVICE']._serialized_start=704
  _globals['_DSDOWNLOADSERVICE']._serialized_end=965
# @@protoc_insertion_point(module_scope)
