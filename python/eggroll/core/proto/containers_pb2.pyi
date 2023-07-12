from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar, Iterable, Mapping, Optional, Union

ALL: ContentType
DESCRIPTOR: _descriptor.FileDescriptor
LOGS: ContentType
MODELS: ContentType

class ContainerContent(_message.Message):
    __slots__ = ["compress_method", "content", "rank"]
    COMPRESS_METHOD_FIELD_NUMBER: ClassVar[int]
    CONTENT_FIELD_NUMBER: ClassVar[int]
    RANK_FIELD_NUMBER: ClassVar[int]
    compress_method: str
    content: bytes
    rank: int
    def __init__(self, rank: Optional[int] = ..., content: Optional[bytes] = ..., compress_method: Optional[str] = ...) -> None: ...

class DeepspeedContainerConfig(_message.Message):
    __slots__ = ["backend", "cross_rank", "cross_size", "cuda_visible_devices", "local_rank", "local_size", "rank", "store_host", "store_port", "store_prefix", "world_size"]
    BACKEND_FIELD_NUMBER: ClassVar[int]
    CROSS_RANK_FIELD_NUMBER: ClassVar[int]
    CROSS_SIZE_FIELD_NUMBER: ClassVar[int]
    CUDA_VISIBLE_DEVICES_FIELD_NUMBER: ClassVar[int]
    LOCAL_RANK_FIELD_NUMBER: ClassVar[int]
    LOCAL_SIZE_FIELD_NUMBER: ClassVar[int]
    RANK_FIELD_NUMBER: ClassVar[int]
    STORE_HOST_FIELD_NUMBER: ClassVar[int]
    STORE_PORT_FIELD_NUMBER: ClassVar[int]
    STORE_PREFIX_FIELD_NUMBER: ClassVar[int]
    WORLD_SIZE_FIELD_NUMBER: ClassVar[int]
    backend: str
    cross_rank: int
    cross_size: int
    cuda_visible_devices: _containers.RepeatedScalarFieldContainer[int]
    local_rank: int
    local_size: int
    rank: int
    store_host: str
    store_port: int
    store_prefix: str
    world_size: int
    def __init__(self, cuda_visible_devices: Optional[Iterable[int]] = ..., world_size: Optional[int] = ..., cross_rank: Optional[int] = ..., cross_size: Optional[int] = ..., local_size: Optional[int] = ..., local_rank: Optional[int] = ..., rank: Optional[int] = ..., store_host: Optional[str] = ..., store_port: Optional[int] = ..., store_prefix: Optional[str] = ..., backend: Optional[str] = ...) -> None: ...

class DownloadContainersRequest(_message.Message):
    __slots__ = ["compress_level", "compress_method", "content_type", "ranks", "session_id"]
    COMPRESS_LEVEL_FIELD_NUMBER: ClassVar[int]
    COMPRESS_METHOD_FIELD_NUMBER: ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: ClassVar[int]
    RANKS_FIELD_NUMBER: ClassVar[int]
    SESSION_ID_FIELD_NUMBER: ClassVar[int]
    compress_level: int
    compress_method: str
    content_type: ContentType
    ranks: _containers.RepeatedScalarFieldContainer[int]
    session_id: str
    def __init__(self, session_id: Optional[str] = ..., ranks: Optional[Iterable[int]] = ..., compress_method: Optional[str] = ..., compress_level: Optional[int] = ..., content_type: Optional[Union[ContentType, str]] = ...) -> None: ...

class DownloadContainersResponse(_message.Message):
    __slots__ = ["container_content", "session_id"]
    CONTAINER_CONTENT_FIELD_NUMBER: ClassVar[int]
    SESSION_ID_FIELD_NUMBER: ClassVar[int]
    container_content: _containers.RepeatedCompositeFieldContainer[ContainerContent]
    session_id: str
    def __init__(self, session_id: Optional[str] = ..., container_content: Optional[Iterable[Union[ContainerContent, Mapping]]] = ...) -> None: ...

class KillContainersRequest(_message.Message):
    __slots__ = ["container_ids", "session_id"]
    CONTAINER_IDS_FIELD_NUMBER: ClassVar[int]
    SESSION_ID_FIELD_NUMBER: ClassVar[int]
    container_ids: _containers.RepeatedScalarFieldContainer[int]
    session_id: str
    def __init__(self, session_id: Optional[str] = ..., container_ids: Optional[Iterable[int]] = ...) -> None: ...

class KillContainersResponse(_message.Message):
    __slots__ = ["session_id"]
    SESSION_ID_FIELD_NUMBER: ClassVar[int]
    session_id: str
    def __init__(self, session_id: Optional[str] = ...) -> None: ...

class StartContainersRequest(_message.Message):
    __slots__ = ["command_arguments", "environment_variables", "files", "job_type", "name", "options", "session_id", "typed_extra_configs", "zipped_files"]
    class EnvironmentVariablesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: ClassVar[int]
        VALUE_FIELD_NUMBER: ClassVar[int]
        key: str
        value: str
        def __init__(self, key: Optional[str] = ..., value: Optional[str] = ...) -> None: ...
    class FilesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: ClassVar[int]
        VALUE_FIELD_NUMBER: ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: Optional[str] = ..., value: Optional[bytes] = ...) -> None: ...
    class OptionsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: ClassVar[int]
        VALUE_FIELD_NUMBER: ClassVar[int]
        key: str
        value: str
        def __init__(self, key: Optional[str] = ..., value: Optional[str] = ...) -> None: ...
    class TypedExtraConfigsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: ClassVar[int]
        VALUE_FIELD_NUMBER: ClassVar[int]
        key: int
        value: bytes
        def __init__(self, key: Optional[int] = ..., value: Optional[bytes] = ...) -> None: ...
    class ZippedFilesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: ClassVar[int]
        VALUE_FIELD_NUMBER: ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: Optional[str] = ..., value: Optional[bytes] = ...) -> None: ...
    COMMAND_ARGUMENTS_FIELD_NUMBER: ClassVar[int]
    ENVIRONMENT_VARIABLES_FIELD_NUMBER: ClassVar[int]
    FILES_FIELD_NUMBER: ClassVar[int]
    JOB_TYPE_FIELD_NUMBER: ClassVar[int]
    NAME_FIELD_NUMBER: ClassVar[int]
    OPTIONS_FIELD_NUMBER: ClassVar[int]
    SESSION_ID_FIELD_NUMBER: ClassVar[int]
    TYPED_EXTRA_CONFIGS_FIELD_NUMBER: ClassVar[int]
    ZIPPED_FILES_FIELD_NUMBER: ClassVar[int]
    command_arguments: _containers.RepeatedScalarFieldContainer[str]
    environment_variables: _containers.ScalarMap[str, str]
    files: _containers.ScalarMap[str, bytes]
    job_type: str
    name: str
    options: _containers.ScalarMap[str, str]
    session_id: str
    typed_extra_configs: _containers.ScalarMap[int, bytes]
    zipped_files: _containers.ScalarMap[str, bytes]
    def __init__(self, session_id: Optional[str] = ..., name: Optional[str] = ..., job_type: Optional[str] = ..., command_arguments: Optional[Iterable[str]] = ..., environment_variables: Optional[Mapping[str, str]] = ..., files: Optional[Mapping[str, bytes]] = ..., zipped_files: Optional[Mapping[str, bytes]] = ..., typed_extra_configs: Optional[Mapping[int, bytes]] = ..., options: Optional[Mapping[str, str]] = ...) -> None: ...

class StartContainersResponse(_message.Message):
    __slots__ = ["session_id"]
    SESSION_ID_FIELD_NUMBER: ClassVar[int]
    session_id: str
    def __init__(self, session_id: Optional[str] = ...) -> None: ...

class StopContainersRequest(_message.Message):
    __slots__ = ["container_ids", "session_id"]
    CONTAINER_IDS_FIELD_NUMBER: ClassVar[int]
    SESSION_ID_FIELD_NUMBER: ClassVar[int]
    container_ids: _containers.RepeatedScalarFieldContainer[int]
    session_id: str
    def __init__(self, session_id: Optional[str] = ..., container_ids: Optional[Iterable[int]] = ...) -> None: ...

class StopContainersResponse(_message.Message):
    __slots__ = ["session_id"]
    SESSION_ID_FIELD_NUMBER: ClassVar[int]
    session_id: str
    def __init__(self, session_id: Optional[str] = ...) -> None: ...

class ContentType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
