# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: containers.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x10\x63ontainers.proto\x12\x1c\x63om.webank.eggroll.core.meta\"\xca\x06\n\x16StartContainersRequest\x12\x12\n\nsession_id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x10\n\x08job_type\x18\x03 \x01(\t\x12\x19\n\x11\x63ommand_arguments\x18\x04 \x03(\t\x12m\n\x15\x65nvironment_variables\x18\x05 \x03(\x0b\x32N.com.webank.eggroll.core.meta.StartContainersRequest.EnvironmentVariablesEntry\x12N\n\x05\x66iles\x18\x06 \x03(\x0b\x32?.com.webank.eggroll.core.meta.StartContainersRequest.FilesEntry\x12[\n\x0czipped_files\x18\x07 \x03(\x0b\x32\x45.com.webank.eggroll.core.meta.StartContainersRequest.ZippedFilesEntry\x12h\n\x13typed_extra_configs\x18\x08 \x03(\x0b\x32K.com.webank.eggroll.core.meta.StartContainersRequest.TypedExtraConfigsEntry\x12R\n\x07options\x18\t \x03(\x0b\x32\x41.com.webank.eggroll.core.meta.StartContainersRequest.OptionsEntry\x1a;\n\x19\x45nvironmentVariablesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a,\n\nFilesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x0c:\x02\x38\x01\x1a\x32\n\x10ZippedFilesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x0c:\x02\x38\x01\x1a\x38\n\x16TypedExtraConfigsEntry\x12\x0b\n\x03key\x18\x01 \x01(\x04\x12\r\n\x05value\x18\x02 \x01(\x0c:\x02\x38\x01\x1a.\n\x0cOptionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\xf9\x01\n\x18\x44\x65\x65pspeedContainerConfig\x12\x1c\n\x14\x63uda_visible_devices\x18\x02 \x03(\r\x12\x12\n\nworld_size\x18\x03 \x01(\r\x12\x12\n\ncross_rank\x18\x04 \x01(\r\x12\x12\n\ncross_size\x18\x05 \x01(\r\x12\x12\n\nlocal_size\x18\x06 \x01(\r\x12\x12\n\nlocal_rank\x18\x07 \x01(\r\x12\x0c\n\x04rank\x18\x08 \x01(\r\x12\x12\n\nstore_host\x18\t \x01(\t\x12\x12\n\nstore_port\x18\n \x01(\x05\x12\x14\n\x0cstore_prefix\x18\x0b \x01(\t\x12\x0f\n\x07\x62\x61\x63kend\x18\x0c \x01(\t\"-\n\x17StartContainersResponse\x12\x12\n\nsession_id\x18\x01 \x01(\t\"B\n\x15StopContainersRequest\x12\x12\n\nsession_id\x18\x01 \x01(\t\x12\x15\n\rcontainer_ids\x18\x02 \x03(\x03\",\n\x16StopContainersResponse\x12\x12\n\nsession_id\x18\x01 \x01(\t\"B\n\x15KillContainersRequest\x12\x12\n\nsession_id\x18\x01 \x01(\t\x12\x15\n\rcontainer_ids\x18\x02 \x03(\x03\",\n\x16KillContainersResponse\x12\x12\n\nsession_id\x18\x01 \x01(\t\"\xb8\x01\n\x19\x44ownloadContainersRequest\x12\x12\n\nsession_id\x18\x01 \x01(\t\x12\x15\n\rcontainer_ids\x18\x02 \x03(\x03\x12\x17\n\x0f\x63ompress_method\x18\x03 \x01(\t\x12\x16\n\x0e\x63ompress_level\x18\x04 \x01(\x05\x12?\n\x0c\x63ontent_type\x18\x05 \x01(\x0e\x32).com.webank.eggroll.core.meta.ContentType\"{\n\x1a\x44ownloadContainersResponse\x12\x12\n\nsession_id\x18\x01 \x01(\t\x12I\n\x11\x63ontainer_content\x18\x02 \x03(\x0b\x32..com.webank.eggroll.core.meta.ContainerContent\"R\n\x10\x43ontainerContent\x12\x14\n\x0c\x63ontainer_id\x18\x01 \x01(\x03\x12\x0f\n\x07\x63ontent\x18\x02 \x01(\x0c\x12\x17\n\x0f\x63ompress_method\x18\x03 \x01(\t*8\n\x0b\x43ontentType\x12\x07\n\x03\x41LL\x10\x00\x12\n\n\x06MODELS\x10\x01\x12\x08\n\x04LOGS\x10\x02\x12\n\n\x06RESULT\x10\x03\x62\x06proto3')

_CONTENTTYPE = DESCRIPTOR.enum_types_by_name['ContentType']
ContentType = enum_type_wrapper.EnumTypeWrapper(_CONTENTTYPE)
ALL = 0
MODELS = 1
LOGS = 2
RESULT = 3


_STARTCONTAINERSREQUEST = DESCRIPTOR.message_types_by_name['StartContainersRequest']
_STARTCONTAINERSREQUEST_ENVIRONMENTVARIABLESENTRY = _STARTCONTAINERSREQUEST.nested_types_by_name['EnvironmentVariablesEntry']
_STARTCONTAINERSREQUEST_FILESENTRY = _STARTCONTAINERSREQUEST.nested_types_by_name['FilesEntry']
_STARTCONTAINERSREQUEST_ZIPPEDFILESENTRY = _STARTCONTAINERSREQUEST.nested_types_by_name['ZippedFilesEntry']
_STARTCONTAINERSREQUEST_TYPEDEXTRACONFIGSENTRY = _STARTCONTAINERSREQUEST.nested_types_by_name['TypedExtraConfigsEntry']
_STARTCONTAINERSREQUEST_OPTIONSENTRY = _STARTCONTAINERSREQUEST.nested_types_by_name['OptionsEntry']
_DEEPSPEEDCONTAINERCONFIG = DESCRIPTOR.message_types_by_name['DeepspeedContainerConfig']
_STARTCONTAINERSRESPONSE = DESCRIPTOR.message_types_by_name['StartContainersResponse']
_STOPCONTAINERSREQUEST = DESCRIPTOR.message_types_by_name['StopContainersRequest']
_STOPCONTAINERSRESPONSE = DESCRIPTOR.message_types_by_name['StopContainersResponse']
_KILLCONTAINERSREQUEST = DESCRIPTOR.message_types_by_name['KillContainersRequest']
_KILLCONTAINERSRESPONSE = DESCRIPTOR.message_types_by_name['KillContainersResponse']
_DOWNLOADCONTAINERSREQUEST = DESCRIPTOR.message_types_by_name['DownloadContainersRequest']
_DOWNLOADCONTAINERSRESPONSE = DESCRIPTOR.message_types_by_name['DownloadContainersResponse']
_CONTAINERCONTENT = DESCRIPTOR.message_types_by_name['ContainerContent']
StartContainersRequest = _reflection.GeneratedProtocolMessageType('StartContainersRequest', (_message.Message,), {

  'EnvironmentVariablesEntry' : _reflection.GeneratedProtocolMessageType('EnvironmentVariablesEntry', (_message.Message,), {
    'DESCRIPTOR' : _STARTCONTAINERSREQUEST_ENVIRONMENTVARIABLESENTRY,
    '__module__' : 'containers_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.StartContainersRequest.EnvironmentVariablesEntry)
    })
  ,

  'FilesEntry' : _reflection.GeneratedProtocolMessageType('FilesEntry', (_message.Message,), {
    'DESCRIPTOR' : _STARTCONTAINERSREQUEST_FILESENTRY,
    '__module__' : 'containers_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.StartContainersRequest.FilesEntry)
    })
  ,

  'ZippedFilesEntry' : _reflection.GeneratedProtocolMessageType('ZippedFilesEntry', (_message.Message,), {
    'DESCRIPTOR' : _STARTCONTAINERSREQUEST_ZIPPEDFILESENTRY,
    '__module__' : 'containers_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.StartContainersRequest.ZippedFilesEntry)
    })
  ,

  'TypedExtraConfigsEntry' : _reflection.GeneratedProtocolMessageType('TypedExtraConfigsEntry', (_message.Message,), {
    'DESCRIPTOR' : _STARTCONTAINERSREQUEST_TYPEDEXTRACONFIGSENTRY,
    '__module__' : 'containers_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.StartContainersRequest.TypedExtraConfigsEntry)
    })
  ,

  'OptionsEntry' : _reflection.GeneratedProtocolMessageType('OptionsEntry', (_message.Message,), {
    'DESCRIPTOR' : _STARTCONTAINERSREQUEST_OPTIONSENTRY,
    '__module__' : 'containers_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.StartContainersRequest.OptionsEntry)
    })
  ,
  'DESCRIPTOR' : _STARTCONTAINERSREQUEST,
  '__module__' : 'containers_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.StartContainersRequest)
  })
_sym_db.RegisterMessage(StartContainersRequest)
_sym_db.RegisterMessage(StartContainersRequest.EnvironmentVariablesEntry)
_sym_db.RegisterMessage(StartContainersRequest.FilesEntry)
_sym_db.RegisterMessage(StartContainersRequest.ZippedFilesEntry)
_sym_db.RegisterMessage(StartContainersRequest.TypedExtraConfigsEntry)
_sym_db.RegisterMessage(StartContainersRequest.OptionsEntry)

DeepspeedContainerConfig = _reflection.GeneratedProtocolMessageType('DeepspeedContainerConfig', (_message.Message,), {
  'DESCRIPTOR' : _DEEPSPEEDCONTAINERCONFIG,
  '__module__' : 'containers_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.DeepspeedContainerConfig)
  })
_sym_db.RegisterMessage(DeepspeedContainerConfig)

StartContainersResponse = _reflection.GeneratedProtocolMessageType('StartContainersResponse', (_message.Message,), {
  'DESCRIPTOR' : _STARTCONTAINERSRESPONSE,
  '__module__' : 'containers_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.StartContainersResponse)
  })
_sym_db.RegisterMessage(StartContainersResponse)

StopContainersRequest = _reflection.GeneratedProtocolMessageType('StopContainersRequest', (_message.Message,), {
  'DESCRIPTOR' : _STOPCONTAINERSREQUEST,
  '__module__' : 'containers_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.StopContainersRequest)
  })
_sym_db.RegisterMessage(StopContainersRequest)

StopContainersResponse = _reflection.GeneratedProtocolMessageType('StopContainersResponse', (_message.Message,), {
  'DESCRIPTOR' : _STOPCONTAINERSRESPONSE,
  '__module__' : 'containers_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.StopContainersResponse)
  })
_sym_db.RegisterMessage(StopContainersResponse)

KillContainersRequest = _reflection.GeneratedProtocolMessageType('KillContainersRequest', (_message.Message,), {
  'DESCRIPTOR' : _KILLCONTAINERSREQUEST,
  '__module__' : 'containers_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.KillContainersRequest)
  })
_sym_db.RegisterMessage(KillContainersRequest)

KillContainersResponse = _reflection.GeneratedProtocolMessageType('KillContainersResponse', (_message.Message,), {
  'DESCRIPTOR' : _KILLCONTAINERSRESPONSE,
  '__module__' : 'containers_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.KillContainersResponse)
  })
_sym_db.RegisterMessage(KillContainersResponse)

DownloadContainersRequest = _reflection.GeneratedProtocolMessageType('DownloadContainersRequest', (_message.Message,), {
  'DESCRIPTOR' : _DOWNLOADCONTAINERSREQUEST,
  '__module__' : 'containers_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.DownloadContainersRequest)
  })
_sym_db.RegisterMessage(DownloadContainersRequest)

DownloadContainersResponse = _reflection.GeneratedProtocolMessageType('DownloadContainersResponse', (_message.Message,), {
  'DESCRIPTOR' : _DOWNLOADCONTAINERSRESPONSE,
  '__module__' : 'containers_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.DownloadContainersResponse)
  })
_sym_db.RegisterMessage(DownloadContainersResponse)

ContainerContent = _reflection.GeneratedProtocolMessageType('ContainerContent', (_message.Message,), {
  'DESCRIPTOR' : _CONTAINERCONTENT,
  '__module__' : 'containers_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.ContainerContent)
  })
_sym_db.RegisterMessage(ContainerContent)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _STARTCONTAINERSREQUEST_ENVIRONMENTVARIABLESENTRY._options = None
  _STARTCONTAINERSREQUEST_ENVIRONMENTVARIABLESENTRY._serialized_options = b'8\001'
  _STARTCONTAINERSREQUEST_FILESENTRY._options = None
  _STARTCONTAINERSREQUEST_FILESENTRY._serialized_options = b'8\001'
  _STARTCONTAINERSREQUEST_ZIPPEDFILESENTRY._options = None
  _STARTCONTAINERSREQUEST_ZIPPEDFILESENTRY._serialized_options = b'8\001'
  _STARTCONTAINERSREQUEST_TYPEDEXTRACONFIGSENTRY._options = None
  _STARTCONTAINERSREQUEST_TYPEDEXTRACONFIGSENTRY._serialized_options = b'8\001'
  _STARTCONTAINERSREQUEST_OPTIONSENTRY._options = None
  _STARTCONTAINERSREQUEST_OPTIONSENTRY._serialized_options = b'8\001'
  _CONTENTTYPE._serialized_start=1818
  _CONTENTTYPE._serialized_end=1874
  _STARTCONTAINERSREQUEST._serialized_start=51
  _STARTCONTAINERSREQUEST._serialized_end=893
  _STARTCONTAINERSREQUEST_ENVIRONMENTVARIABLESENTRY._serialized_start=630
  _STARTCONTAINERSREQUEST_ENVIRONMENTVARIABLESENTRY._serialized_end=689
  _STARTCONTAINERSREQUEST_FILESENTRY._serialized_start=691
  _STARTCONTAINERSREQUEST_FILESENTRY._serialized_end=735
  _STARTCONTAINERSREQUEST_ZIPPEDFILESENTRY._serialized_start=737
  _STARTCONTAINERSREQUEST_ZIPPEDFILESENTRY._serialized_end=787
  _STARTCONTAINERSREQUEST_TYPEDEXTRACONFIGSENTRY._serialized_start=789
  _STARTCONTAINERSREQUEST_TYPEDEXTRACONFIGSENTRY._serialized_end=845
  _STARTCONTAINERSREQUEST_OPTIONSENTRY._serialized_start=847
  _STARTCONTAINERSREQUEST_OPTIONSENTRY._serialized_end=893
  _DEEPSPEEDCONTAINERCONFIG._serialized_start=896
  _DEEPSPEEDCONTAINERCONFIG._serialized_end=1145
  _STARTCONTAINERSRESPONSE._serialized_start=1147
  _STARTCONTAINERSRESPONSE._serialized_end=1192
  _STOPCONTAINERSREQUEST._serialized_start=1194
  _STOPCONTAINERSREQUEST._serialized_end=1260
  _STOPCONTAINERSRESPONSE._serialized_start=1262
  _STOPCONTAINERSRESPONSE._serialized_end=1306
  _KILLCONTAINERSREQUEST._serialized_start=1308
  _KILLCONTAINERSREQUEST._serialized_end=1374
  _KILLCONTAINERSRESPONSE._serialized_start=1376
  _KILLCONTAINERSRESPONSE._serialized_end=1420
  _DOWNLOADCONTAINERSREQUEST._serialized_start=1423
  _DOWNLOADCONTAINERSREQUEST._serialized_end=1607
  _DOWNLOADCONTAINERSRESPONSE._serialized_start=1609
  _DOWNLOADCONTAINERSRESPONSE._serialized_end=1732
  _CONTAINERCONTENT._serialized_start=1734
  _CONTAINERCONTENT._serialized_end=1816
# @@protoc_insertion_point(module_scope)
