# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: meta.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\nmeta.proto\x12\x1c\x63om.webank.eggroll.core.meta\"&\n\x08\x45ndpoint\x12\x0c\n\x04host\x18\x01 \x01(\t\x12\x0c\n\x04port\x18\x02 \x01(\x05\"\xd0\x01\n\nServerNode\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x11\n\tclusterId\x18\x03 \x01(\x03\x12\x38\n\x08\x65ndpoint\x18\x04 \x01(\x0b\x32&.com.webank.eggroll.core.meta.Endpoint\x12\x10\n\x08nodeType\x18\x05 \x01(\t\x12\x0e\n\x06status\x18\x06 \x01(\t\x12\x39\n\tresources\x18\x07 \x03(\x0b\x32&.com.webank.eggroll.core.meta.Resource\"u\n\rServerCluster\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x0c\n\x04name\x18\x02 \x01(\t\x12=\n\x0bserverNodes\x18\x03 \x03(\x0b\x32(.com.webank.eggroll.core.meta.ServerNode\x12\x0b\n\x03tag\x18\x04 \x01(\t\"\xf6\x02\n\tProcessor\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x14\n\x0cserverNodeId\x18\x02 \x01(\x03\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\x15\n\rprocessorType\x18\x04 \x01(\t\x12\x0e\n\x06status\x18\x05 \x01(\t\x12?\n\x0f\x63ommandEndpoint\x18\x06 \x01(\x0b\x32&.com.webank.eggroll.core.meta.Endpoint\x12@\n\x10transferEndpoint\x18\x07 \x01(\x0b\x32&.com.webank.eggroll.core.meta.Endpoint\x12\x0b\n\x03pid\x18\t \x01(\x05\x12\x45\n\x07options\x18\x08 \x03(\x0b\x32\x34.com.webank.eggroll.core.meta.Processor.OptionsEntry\x12\x0b\n\x03tag\x18\n \x01(\t\x1a.\n\x0cOptionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"t\n\x0eProcessorBatch\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x0c\n\x04name\x18\x02 \x01(\t\x12;\n\nprocessors\x18\x03 \x03(\x0b\x32\'.com.webank.eggroll.core.meta.Processor\x12\x0b\n\x03tag\x18\x04 \x01(\t\"\xaa\x01\n\x07\x46unctor\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0e\n\x06serdes\x18\x02 \x01(\t\x12\x0c\n\x04\x62ody\x18\x03 \x01(\x0c\x12\x43\n\x07options\x18\x04 \x03(\x0b\x32\x32.com.webank.eggroll.core.meta.Functor.OptionsEntry\x1a.\n\x0cOptionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\")\n\x0bPartitioner\x12\x0c\n\x04type\x18\x01 \x01(\x05\x12\x0c\n\x04\x62ody\x18\x02 \x01(\x0c\"$\n\x06Serdes\x12\x0c\n\x04type\x18\x01 \x01(\x05\x12\x0c\n\x04\x62ody\x18\x02 \x01(\x0c\"\"\n\x04Pair\x12\x0b\n\x03key\x18\x01 \x01(\x0c\x12\r\n\x05value\x18\x02 \x01(\x0c\">\n\tPairBatch\x12\x31\n\x05pairs\x18\x01 \x03(\x0b\x32\".com.webank.eggroll.core.meta.Pair\"\xc5\x01\n\x0cStoreLocator\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x12\n\nstore_type\x18\x02 \x01(\t\x12\x11\n\tnamespace\x18\x03 \x01(\t\x12\x0c\n\x04name\x18\x04 \x01(\t\x12\x0c\n\x04path\x18\x05 \x01(\t\x12\x18\n\x10total_partitions\x18\x06 \x01(\x05\x12\x17\n\x0fkey_serdes_type\x18\x07 \x01(\x05\x12\x19\n\x11value_serdes_type\x18\x08 \x01(\x05\x12\x18\n\x10partitioner_type\x18\t \x01(\x05\"\xf9\x01\n\x05Store\x12@\n\x0cstoreLocator\x18\x01 \x01(\x0b\x32*.com.webank.eggroll.core.meta.StoreLocator\x12;\n\npartitions\x18\x02 \x03(\x0b\x32\'.com.webank.eggroll.core.meta.Partition\x12\x41\n\x07options\x18\x03 \x03(\x0b\x32\x30.com.webank.eggroll.core.meta.Store.OptionsEntry\x1a.\n\x0cOptionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"@\n\tStoreList\x12\x33\n\x06stores\x18\x01 \x03(\x0b\x32#.com.webank.eggroll.core.meta.Store\"\xbf\x01\n\tPartition\x12\n\n\x02id\x18\x01 \x01(\x05\x12@\n\x0cstoreLocator\x18\x02 \x01(\x0b\x32*.com.webank.eggroll.core.meta.StoreLocator\x12:\n\tprocessor\x18\x03 \x01(\x0b\x32\'.com.webank.eggroll.core.meta.Processor\x12\x14\n\x0cserverNodeId\x18\x04 \x01(\x03\x12\x12\n\nrankInNode\x18\x05 \x01(\x05\"\x1b\n\x08\x43\x61llInfo\x12\x0f\n\x07\x63\x61llSeq\x18\x01 \x01(\t\"\xf1\x01\n\x05JobIO\x12\x32\n\x05store\x18\x01 \x01(\x0b\x32#.com.webank.eggroll.core.meta.Store\x12\x38\n\nkey_serdes\x18\x02 \x01(\x0b\x32$.com.webank.eggroll.core.meta.Serdes\x12:\n\x0cvalue_serdes\x18\x03 \x01(\x0b\x32$.com.webank.eggroll.core.meta.Serdes\x12>\n\x0bpartitioner\x18\x04 \x01(\x0b\x32).com.webank.eggroll.core.meta.Partitioner\"\xb4\x02\n\x03Job\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x33\n\x06inputs\x18\x03 \x03(\x0b\x32#.com.webank.eggroll.core.meta.JobIO\x12\x34\n\x07outputs\x18\x04 \x03(\x0b\x32#.com.webank.eggroll.core.meta.JobIO\x12\x37\n\x08\x66unctors\x18\x05 \x03(\x0b\x32%.com.webank.eggroll.core.meta.Functor\x12?\n\x07options\x18\x06 \x03(\x0b\x32..com.webank.eggroll.core.meta.Job.OptionsEntry\x1a.\n\x0cOptionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\xc3\x01\n\x04Task\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x37\n\x06inputs\x18\x03 \x03(\x0b\x32\'.com.webank.eggroll.core.meta.Partition\x12\x38\n\x07outputs\x18\x04 \x03(\x0b\x32\'.com.webank.eggroll.core.meta.Partition\x12.\n\x03job\x18\x05 \x01(\x0b\x32!.com.webank.eggroll.core.meta.Job\"\xfa\x01\n\x0bSessionMeta\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x0e\n\x06status\x18\x03 \x01(\t\x12\x0b\n\x03tag\x18\x04 \x01(\t\x12;\n\nprocessors\x18\x05 \x03(\x0b\x32\'.com.webank.eggroll.core.meta.Processor\x12G\n\x07options\x18\x06 \x03(\x0b\x32\x36.com.webank.eggroll.core.meta.SessionMeta.OptionsEntry\x1a.\n\x0cOptionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\x9d\x01\n\x12ResourceAllocation\x12\x14\n\x0cserverNodeId\x18\x01 \x01(\x03\x12\x0e\n\x06status\x18\x02 \x01(\t\x12\x11\n\tsessionId\x18\x03 \x01(\t\x12\x13\n\x0boperateType\x18\x04 \x01(\t\x12\x39\n\tresources\x18\x05 \x03(\x0b\x32&.com.webank.eggroll.core.meta.Resource\"H\n\x08Resource\x12\x0c\n\x04type\x18\x01 \x01(\t\x12\r\n\x05total\x18\x02 \x01(\x03\x12\x0c\n\x04used\x18\x03 \x01(\x03\x12\x11\n\tallocated\x18\x04 \x01(\x03\"\x9c\x01\n\rNodeHeartbeat\x12\n\n\x02id\x18\x01 \x01(\x04\x12\x36\n\x04node\x18\x02 \x01(\x0b\x32(.com.webank.eggroll.core.meta.ServerNode\x12\x0c\n\x04\x63ode\x18\x03 \x01(\t\x12\x0b\n\x03msg\x18\x04 \x01(\t\x12\x15\n\rgpuProcessors\x18\x05 \x03(\x05\x12\x15\n\rcpuProcessors\x18\x06 \x03(\x05\"\x90\x01\n\x10MetaInfoResponse\x12L\n\x07metaMap\x18\x01 \x03(\x0b\x32;.com.webank.eggroll.core.meta.MetaInfoResponse.MetaMapEntry\x1a.\n\x0cMetaMapEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\x1e\n\x0fMetaInfoRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\"&\n\x11QueueViewResponse\x12\x11\n\tqueueSize\x18\x01 \x01(\x05\"\x1f\n\x10QueueViewRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\"h\n\x1a\x43heckResourceEnoughRequest\x12\x15\n\rresource_type\x18\x01 \x01(\t\x12\x1f\n\x17required_resource_count\x18\x02 \x01(\x03\x12\x12\n\ncheck_type\x18\x03 \x01(\t\"r\n\x1b\x43heckResourceEnoughResponse\x12\x11\n\tis_enough\x18\x01 \x01(\x08\x12@\n\x0b\x63lusterInfo\x18\x02 \x01(\x0b\x32+.com.webank.eggroll.core.meta.ServerCluster\"\x19\n\x17queryClusterInfoRequest\"\\\n\x18queryClusterInfoResponse\x12@\n\x0b\x63lusterInfo\x18\x02 \x01(\x0b\x32+.com.webank.eggroll.core.meta.ServerClusterb\x06proto3')



_ENDPOINT = DESCRIPTOR.message_types_by_name['Endpoint']
_SERVERNODE = DESCRIPTOR.message_types_by_name['ServerNode']
_SERVERCLUSTER = DESCRIPTOR.message_types_by_name['ServerCluster']
_PROCESSOR = DESCRIPTOR.message_types_by_name['Processor']
_PROCESSOR_OPTIONSENTRY = _PROCESSOR.nested_types_by_name['OptionsEntry']
_PROCESSORBATCH = DESCRIPTOR.message_types_by_name['ProcessorBatch']
_FUNCTOR = DESCRIPTOR.message_types_by_name['Functor']
_FUNCTOR_OPTIONSENTRY = _FUNCTOR.nested_types_by_name['OptionsEntry']
_PARTITIONER = DESCRIPTOR.message_types_by_name['Partitioner']
_SERDES = DESCRIPTOR.message_types_by_name['Serdes']
_PAIR = DESCRIPTOR.message_types_by_name['Pair']
_PAIRBATCH = DESCRIPTOR.message_types_by_name['PairBatch']
_STORELOCATOR = DESCRIPTOR.message_types_by_name['StoreLocator']
_STORE = DESCRIPTOR.message_types_by_name['Store']
_STORE_OPTIONSENTRY = _STORE.nested_types_by_name['OptionsEntry']
_STORELIST = DESCRIPTOR.message_types_by_name['StoreList']
_PARTITION = DESCRIPTOR.message_types_by_name['Partition']
_CALLINFO = DESCRIPTOR.message_types_by_name['CallInfo']
_JOBIO = DESCRIPTOR.message_types_by_name['JobIO']
_JOB = DESCRIPTOR.message_types_by_name['Job']
_JOB_OPTIONSENTRY = _JOB.nested_types_by_name['OptionsEntry']
_TASK = DESCRIPTOR.message_types_by_name['Task']
_SESSIONMETA = DESCRIPTOR.message_types_by_name['SessionMeta']
_SESSIONMETA_OPTIONSENTRY = _SESSIONMETA.nested_types_by_name['OptionsEntry']
_RESOURCEALLOCATION = DESCRIPTOR.message_types_by_name['ResourceAllocation']
_RESOURCE = DESCRIPTOR.message_types_by_name['Resource']
_NODEHEARTBEAT = DESCRIPTOR.message_types_by_name['NodeHeartbeat']
_METAINFORESPONSE = DESCRIPTOR.message_types_by_name['MetaInfoResponse']
_METAINFORESPONSE_METAMAPENTRY = _METAINFORESPONSE.nested_types_by_name['MetaMapEntry']
_METAINFOREQUEST = DESCRIPTOR.message_types_by_name['MetaInfoRequest']
_QUEUEVIEWRESPONSE = DESCRIPTOR.message_types_by_name['QueueViewResponse']
_QUEUEVIEWREQUEST = DESCRIPTOR.message_types_by_name['QueueViewRequest']
_CHECKRESOURCEENOUGHREQUEST = DESCRIPTOR.message_types_by_name['CheckResourceEnoughRequest']
_CHECKRESOURCEENOUGHRESPONSE = DESCRIPTOR.message_types_by_name['CheckResourceEnoughResponse']
_QUERYCLUSTERINFOREQUEST = DESCRIPTOR.message_types_by_name['queryClusterInfoRequest']
_QUERYCLUSTERINFORESPONSE = DESCRIPTOR.message_types_by_name['queryClusterInfoResponse']
Endpoint = _reflection.GeneratedProtocolMessageType('Endpoint', (_message.Message,), {
  'DESCRIPTOR' : _ENDPOINT,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Endpoint)
  })
_sym_db.RegisterMessage(Endpoint)

ServerNode = _reflection.GeneratedProtocolMessageType('ServerNode', (_message.Message,), {
  'DESCRIPTOR' : _SERVERNODE,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.ServerNode)
  })
_sym_db.RegisterMessage(ServerNode)

ServerCluster = _reflection.GeneratedProtocolMessageType('ServerCluster', (_message.Message,), {
  'DESCRIPTOR' : _SERVERCLUSTER,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.ServerCluster)
  })
_sym_db.RegisterMessage(ServerCluster)

Processor = _reflection.GeneratedProtocolMessageType('Processor', (_message.Message,), {

  'OptionsEntry' : _reflection.GeneratedProtocolMessageType('OptionsEntry', (_message.Message,), {
    'DESCRIPTOR' : _PROCESSOR_OPTIONSENTRY,
    '__module__' : 'meta_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Processor.OptionsEntry)
    })
  ,
  'DESCRIPTOR' : _PROCESSOR,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Processor)
  })
_sym_db.RegisterMessage(Processor)
_sym_db.RegisterMessage(Processor.OptionsEntry)

ProcessorBatch = _reflection.GeneratedProtocolMessageType('ProcessorBatch', (_message.Message,), {
  'DESCRIPTOR' : _PROCESSORBATCH,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.ProcessorBatch)
  })
_sym_db.RegisterMessage(ProcessorBatch)

Functor = _reflection.GeneratedProtocolMessageType('Functor', (_message.Message,), {

  'OptionsEntry' : _reflection.GeneratedProtocolMessageType('OptionsEntry', (_message.Message,), {
    'DESCRIPTOR' : _FUNCTOR_OPTIONSENTRY,
    '__module__' : 'meta_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Functor.OptionsEntry)
    })
  ,
  'DESCRIPTOR' : _FUNCTOR,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Functor)
  })
_sym_db.RegisterMessage(Functor)
_sym_db.RegisterMessage(Functor.OptionsEntry)

Partitioner = _reflection.GeneratedProtocolMessageType('Partitioner', (_message.Message,), {
  'DESCRIPTOR' : _PARTITIONER,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Partitioner)
  })
_sym_db.RegisterMessage(Partitioner)

Serdes = _reflection.GeneratedProtocolMessageType('Serdes', (_message.Message,), {
  'DESCRIPTOR' : _SERDES,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Serdes)
  })
_sym_db.RegisterMessage(Serdes)

Pair = _reflection.GeneratedProtocolMessageType('Pair', (_message.Message,), {
  'DESCRIPTOR' : _PAIR,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Pair)
  })
_sym_db.RegisterMessage(Pair)

PairBatch = _reflection.GeneratedProtocolMessageType('PairBatch', (_message.Message,), {
  'DESCRIPTOR' : _PAIRBATCH,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.PairBatch)
  })
_sym_db.RegisterMessage(PairBatch)

StoreLocator = _reflection.GeneratedProtocolMessageType('StoreLocator', (_message.Message,), {
  'DESCRIPTOR' : _STORELOCATOR,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.StoreLocator)
  })
_sym_db.RegisterMessage(StoreLocator)

Store = _reflection.GeneratedProtocolMessageType('Store', (_message.Message,), {

  'OptionsEntry' : _reflection.GeneratedProtocolMessageType('OptionsEntry', (_message.Message,), {
    'DESCRIPTOR' : _STORE_OPTIONSENTRY,
    '__module__' : 'meta_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Store.OptionsEntry)
    })
  ,
  'DESCRIPTOR' : _STORE,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Store)
  })
_sym_db.RegisterMessage(Store)
_sym_db.RegisterMessage(Store.OptionsEntry)

StoreList = _reflection.GeneratedProtocolMessageType('StoreList', (_message.Message,), {
  'DESCRIPTOR' : _STORELIST,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.StoreList)
  })
_sym_db.RegisterMessage(StoreList)

Partition = _reflection.GeneratedProtocolMessageType('Partition', (_message.Message,), {
  'DESCRIPTOR' : _PARTITION,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Partition)
  })
_sym_db.RegisterMessage(Partition)

CallInfo = _reflection.GeneratedProtocolMessageType('CallInfo', (_message.Message,), {
  'DESCRIPTOR' : _CALLINFO,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.CallInfo)
  })
_sym_db.RegisterMessage(CallInfo)

JobIO = _reflection.GeneratedProtocolMessageType('JobIO', (_message.Message,), {
  'DESCRIPTOR' : _JOBIO,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.JobIO)
  })
_sym_db.RegisterMessage(JobIO)

Job = _reflection.GeneratedProtocolMessageType('Job', (_message.Message,), {

  'OptionsEntry' : _reflection.GeneratedProtocolMessageType('OptionsEntry', (_message.Message,), {
    'DESCRIPTOR' : _JOB_OPTIONSENTRY,
    '__module__' : 'meta_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Job.OptionsEntry)
    })
  ,
  'DESCRIPTOR' : _JOB,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Job)
  })
_sym_db.RegisterMessage(Job)
_sym_db.RegisterMessage(Job.OptionsEntry)

Task = _reflection.GeneratedProtocolMessageType('Task', (_message.Message,), {
  'DESCRIPTOR' : _TASK,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Task)
  })
_sym_db.RegisterMessage(Task)

SessionMeta = _reflection.GeneratedProtocolMessageType('SessionMeta', (_message.Message,), {

  'OptionsEntry' : _reflection.GeneratedProtocolMessageType('OptionsEntry', (_message.Message,), {
    'DESCRIPTOR' : _SESSIONMETA_OPTIONSENTRY,
    '__module__' : 'meta_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.SessionMeta.OptionsEntry)
    })
  ,
  'DESCRIPTOR' : _SESSIONMETA,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.SessionMeta)
  })
_sym_db.RegisterMessage(SessionMeta)
_sym_db.RegisterMessage(SessionMeta.OptionsEntry)

ResourceAllocation = _reflection.GeneratedProtocolMessageType('ResourceAllocation', (_message.Message,), {
  'DESCRIPTOR' : _RESOURCEALLOCATION,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.ResourceAllocation)
  })
_sym_db.RegisterMessage(ResourceAllocation)

Resource = _reflection.GeneratedProtocolMessageType('Resource', (_message.Message,), {
  'DESCRIPTOR' : _RESOURCE,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.Resource)
  })
_sym_db.RegisterMessage(Resource)

NodeHeartbeat = _reflection.GeneratedProtocolMessageType('NodeHeartbeat', (_message.Message,), {
  'DESCRIPTOR' : _NODEHEARTBEAT,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.NodeHeartbeat)
  })
_sym_db.RegisterMessage(NodeHeartbeat)

MetaInfoResponse = _reflection.GeneratedProtocolMessageType('MetaInfoResponse', (_message.Message,), {

  'MetaMapEntry' : _reflection.GeneratedProtocolMessageType('MetaMapEntry', (_message.Message,), {
    'DESCRIPTOR' : _METAINFORESPONSE_METAMAPENTRY,
    '__module__' : 'meta_pb2'
    # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.MetaInfoResponse.MetaMapEntry)
    })
  ,
  'DESCRIPTOR' : _METAINFORESPONSE,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.MetaInfoResponse)
  })
_sym_db.RegisterMessage(MetaInfoResponse)
_sym_db.RegisterMessage(MetaInfoResponse.MetaMapEntry)

MetaInfoRequest = _reflection.GeneratedProtocolMessageType('MetaInfoRequest', (_message.Message,), {
  'DESCRIPTOR' : _METAINFOREQUEST,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.MetaInfoRequest)
  })
_sym_db.RegisterMessage(MetaInfoRequest)

QueueViewResponse = _reflection.GeneratedProtocolMessageType('QueueViewResponse', (_message.Message,), {
  'DESCRIPTOR' : _QUEUEVIEWRESPONSE,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.QueueViewResponse)
  })
_sym_db.RegisterMessage(QueueViewResponse)

QueueViewRequest = _reflection.GeneratedProtocolMessageType('QueueViewRequest', (_message.Message,), {
  'DESCRIPTOR' : _QUEUEVIEWREQUEST,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.QueueViewRequest)
  })
_sym_db.RegisterMessage(QueueViewRequest)

CheckResourceEnoughRequest = _reflection.GeneratedProtocolMessageType('CheckResourceEnoughRequest', (_message.Message,), {
  'DESCRIPTOR' : _CHECKRESOURCEENOUGHREQUEST,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.CheckResourceEnoughRequest)
  })
_sym_db.RegisterMessage(CheckResourceEnoughRequest)

CheckResourceEnoughResponse = _reflection.GeneratedProtocolMessageType('CheckResourceEnoughResponse', (_message.Message,), {
  'DESCRIPTOR' : _CHECKRESOURCEENOUGHRESPONSE,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.CheckResourceEnoughResponse)
  })
_sym_db.RegisterMessage(CheckResourceEnoughResponse)

queryClusterInfoRequest = _reflection.GeneratedProtocolMessageType('queryClusterInfoRequest', (_message.Message,), {
  'DESCRIPTOR' : _QUERYCLUSTERINFOREQUEST,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.queryClusterInfoRequest)
  })
_sym_db.RegisterMessage(queryClusterInfoRequest)

queryClusterInfoResponse = _reflection.GeneratedProtocolMessageType('queryClusterInfoResponse', (_message.Message,), {
  'DESCRIPTOR' : _QUERYCLUSTERINFORESPONSE,
  '__module__' : 'meta_pb2'
  # @@protoc_insertion_point(class_scope:com.webank.eggroll.core.meta.queryClusterInfoResponse)
  })
_sym_db.RegisterMessage(queryClusterInfoResponse)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _PROCESSOR_OPTIONSENTRY._options = None
  _PROCESSOR_OPTIONSENTRY._serialized_options = b'8\001'
  _FUNCTOR_OPTIONSENTRY._options = None
  _FUNCTOR_OPTIONSENTRY._serialized_options = b'8\001'
  _STORE_OPTIONSENTRY._options = None
  _STORE_OPTIONSENTRY._serialized_options = b'8\001'
  _JOB_OPTIONSENTRY._options = None
  _JOB_OPTIONSENTRY._serialized_options = b'8\001'
  _SESSIONMETA_OPTIONSENTRY._options = None
  _SESSIONMETA_OPTIONSENTRY._serialized_options = b'8\001'
  _METAINFORESPONSE_METAMAPENTRY._options = None
  _METAINFORESPONSE_METAMAPENTRY._serialized_options = b'8\001'
  _ENDPOINT._serialized_start=44
  _ENDPOINT._serialized_end=82
  _SERVERNODE._serialized_start=85
  _SERVERNODE._serialized_end=293
  _SERVERCLUSTER._serialized_start=295
  _SERVERCLUSTER._serialized_end=412
  _PROCESSOR._serialized_start=415
  _PROCESSOR._serialized_end=789
  _PROCESSOR_OPTIONSENTRY._serialized_start=743
  _PROCESSOR_OPTIONSENTRY._serialized_end=789
  _PROCESSORBATCH._serialized_start=791
  _PROCESSORBATCH._serialized_end=907
  _FUNCTOR._serialized_start=910
  _FUNCTOR._serialized_end=1080
  _FUNCTOR_OPTIONSENTRY._serialized_start=743
  _FUNCTOR_OPTIONSENTRY._serialized_end=789
  _PARTITIONER._serialized_start=1082
  _PARTITIONER._serialized_end=1123
  _SERDES._serialized_start=1125
  _SERDES._serialized_end=1161
  _PAIR._serialized_start=1163
  _PAIR._serialized_end=1197
  _PAIRBATCH._serialized_start=1199
  _PAIRBATCH._serialized_end=1261
  _STORELOCATOR._serialized_start=1264
  _STORELOCATOR._serialized_end=1461
  _STORE._serialized_start=1464
  _STORE._serialized_end=1713
  _STORE_OPTIONSENTRY._serialized_start=743
  _STORE_OPTIONSENTRY._serialized_end=789
  _STORELIST._serialized_start=1715
  _STORELIST._serialized_end=1779
  _PARTITION._serialized_start=1782
  _PARTITION._serialized_end=1973
  _CALLINFO._serialized_start=1975
  _CALLINFO._serialized_end=2002
  _JOBIO._serialized_start=2005
  _JOBIO._serialized_end=2246
  _JOB._serialized_start=2249
  _JOB._serialized_end=2557
  _JOB_OPTIONSENTRY._serialized_start=743
  _JOB_OPTIONSENTRY._serialized_end=789
  _TASK._serialized_start=2560
  _TASK._serialized_end=2755
  _SESSIONMETA._serialized_start=2758
  _SESSIONMETA._serialized_end=3008
  _SESSIONMETA_OPTIONSENTRY._serialized_start=743
  _SESSIONMETA_OPTIONSENTRY._serialized_end=789
  _RESOURCEALLOCATION._serialized_start=3011
  _RESOURCEALLOCATION._serialized_end=3168
  _RESOURCE._serialized_start=3170
  _RESOURCE._serialized_end=3242
  _NODEHEARTBEAT._serialized_start=3245
  _NODEHEARTBEAT._serialized_end=3401
  _METAINFORESPONSE._serialized_start=3404
  _METAINFORESPONSE._serialized_end=3548
  _METAINFORESPONSE_METAMAPENTRY._serialized_start=3502
  _METAINFORESPONSE_METAMAPENTRY._serialized_end=3548
  _METAINFOREQUEST._serialized_start=3550
  _METAINFOREQUEST._serialized_end=3580
  _QUEUEVIEWRESPONSE._serialized_start=3582
  _QUEUEVIEWRESPONSE._serialized_end=3620
  _QUEUEVIEWREQUEST._serialized_start=3622
  _QUEUEVIEWREQUEST._serialized_end=3653
  _CHECKRESOURCEENOUGHREQUEST._serialized_start=3655
  _CHECKRESOURCEENOUGHREQUEST._serialized_end=3759
  _CHECKRESOURCEENOUGHRESPONSE._serialized_start=3761
  _CHECKRESOURCEENOUGHRESPONSE._serialized_end=3875
  _QUERYCLUSTERINFOREQUEST._serialized_start=3877
  _QUERYCLUSTERINFOREQUEST._serialized_end=3902
  _QUERYCLUSTERINFORESPONSE._serialized_start=3904
  _QUERYCLUSTERINFORESPONSE._serialized_end=3996
# @@protoc_insertion_point(module_scope)