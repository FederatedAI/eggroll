from google.protobuf import duration_pb2 as _duration_pb2
import meta_pb2 as _meta_pb2
import containers_pb2 as _containers_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar, Iterable, Mapping, Optional, Text, Union

DESCRIPTOR: _descriptor.FileDescriptor

class DsDownloadRequest(_message.Message):
    __slots__ = ["compress_level", "compress_method", "container_ids", "content_type", "session_id"]
    COMPRESS_LEVEL_FIELD_NUMBER: ClassVar[int]
    COMPRESS_METHOD_FIELD_NUMBER: ClassVar[int]
    CONTAINER_IDS_FIELD_NUMBER: ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: ClassVar[int]
    SESSION_ID_FIELD_NUMBER: ClassVar[int]
    compress_level: int
    compress_method: str
    container_ids: _containers.RepeatedScalarFieldContainer[int]
    content_type: _containers_pb2.ContentType
    session_id: str
    def __init__(self, session_id: Optional[str] = ..., container_ids: Optional[Iterable[int]] = ..., compress_method: Optional[str] = ..., compress_level: Optional[int] = ..., content_type: Optional[Union[_containers_pb2.ContentType, str]] = ...) -> None: ...

class DsDownloadResponse(_message.Message):
    __slots__ = ["container_content", "session_id"]
    CONTAINER_CONTENT_FIELD_NUMBER: ClassVar[int]
    SESSION_ID_FIELD_NUMBER: ClassVar[int]
    container_content: _containers.RepeatedCompositeFieldContainer[_containers_pb2.ContainerContent]
    session_id: str
    def __init__(self, session_id: Optional[str] = ..., container_content: Optional[Iterable[Union[_containers_pb2.ContainerContent, Mapping]]] = ...) -> None: ...

class PrepareDownloadRequest(_message.Message):
    __slots__ = ["compress_level", "compress_method", "content_type", "ranks", "session_id"]
    COMPRESS_LEVEL_FIELD_NUMBER: ClassVar[int]
    COMPRESS_METHOD_FIELD_NUMBER: ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: ClassVar[int]
    RANKS_FIELD_NUMBER: ClassVar[int]
    SESSION_ID_FIELD_NUMBER: ClassVar[int]
    compress_level: int
    compress_method: str
    content_type: _containers_pb2.ContentType
    ranks: _containers.RepeatedScalarFieldContainer[int]
    session_id: str
    def __init__(self, session_id: Optional[str] = ..., ranks: Optional[Iterable[int]] = ..., compress_method: Optional[str] = ..., compress_level: Optional[int] = ..., content_type: Optional[Union[_containers_pb2.ContentType, str]] = ...) -> None: ...

class PrepareDownloadResponse(_message.Message):
    __slots__ = ["content", "session_id"]
    CONTENT_FIELD_NUMBER: ClassVar[int]
    SESSION_ID_FIELD_NUMBER: ClassVar[int]
    content: str
    session_id: str
    def __init__(self, session_id: Optional[str] = ..., content: Optional[str] = ...) -> None: ...
