from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OutputStream(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OUTPUT_STREAM_UNSPECIFIED: _ClassVar[OutputStream]
    OUTPUT_STREAM_STDOUT: _ClassVar[OutputStream]
    OUTPUT_STREAM_STDERR: _ClassVar[OutputStream]

class RunStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_STATUS_UNSPECIFIED: _ClassVar[RunStatus]
    RUN_STATUS_COMPLETED: _ClassVar[RunStatus]
    RUN_STATUS_TIMED_OUT: _ClassVar[RunStatus]
    RUN_STATUS_SIGNALED: _ClassVar[RunStatus]
    RUN_STATUS_SKIPPED: _ClassVar[RunStatus]
OUTPUT_STREAM_UNSPECIFIED: OutputStream
OUTPUT_STREAM_STDOUT: OutputStream
OUTPUT_STREAM_STDERR: OutputStream
RUN_STATUS_UNSPECIFIED: RunStatus
RUN_STATUS_COMPLETED: RunStatus
RUN_STATUS_TIMED_OUT: RunStatus
RUN_STATUS_SIGNALED: RunStatus
RUN_STATUS_SKIPPED: RunStatus

class File(_message.Message):
    __slots__ = ("name", "content")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    name: str
    content: bytes
    def __init__(self, name: _Optional[str] = ..., content: _Optional[bytes] = ...) -> None: ...

class FileChunk(_message.Message):
    __slots__ = ("name", "data")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    name: str
    data: bytes
    def __init__(self, name: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class FileInfo(_message.Message):
    __slots__ = ("name", "size_bytes")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    name: str
    size_bytes: int
    def __init__(self, name: _Optional[str] = ..., size_bytes: _Optional[int] = ...) -> None: ...

class OutputChunk(_message.Message):
    __slots__ = ("stream", "data")
    STREAM_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    stream: OutputStream
    data: bytes
    def __init__(self, stream: _Optional[_Union[OutputStream, str]] = ..., data: _Optional[bytes] = ...) -> None: ...

class RunStarted(_message.Message):
    __slots__ = ("model", "file_root")
    MODEL_FIELD_NUMBER: _ClassVar[int]
    FILE_ROOT_FIELD_NUMBER: _ClassVar[int]
    model: str
    file_root: str
    def __init__(self, model: _Optional[str] = ..., file_root: _Optional[str] = ...) -> None: ...

class RunCompleted(_message.Message):
    __slots__ = ("status", "exit_code", "elapsed_seconds", "output_files")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    EXIT_CODE_FIELD_NUMBER: _ClassVar[int]
    ELAPSED_SECONDS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FILES_FIELD_NUMBER: _ClassVar[int]
    status: RunStatus
    exit_code: int
    elapsed_seconds: float
    output_files: _containers.RepeatedCompositeFieldContainer[FileInfo]
    def __init__(self, status: _Optional[_Union[RunStatus, str]] = ..., exit_code: _Optional[int] = ..., elapsed_seconds: _Optional[float] = ..., output_files: _Optional[_Iterable[_Union[FileInfo, _Mapping]]] = ...) -> None: ...

class RunSyncRequest(_message.Message):
    __slots__ = ("model", "file_root", "inputs", "timeout_seconds")
    MODEL_FIELD_NUMBER: _ClassVar[int]
    FILE_ROOT_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    model: str
    file_root: str
    inputs: _containers.RepeatedCompositeFieldContainer[File]
    timeout_seconds: int
    def __init__(self, model: _Optional[str] = ..., file_root: _Optional[str] = ..., inputs: _Optional[_Iterable[_Union[File, _Mapping]]] = ..., timeout_seconds: _Optional[int] = ...) -> None: ...

class RunSyncResponse(_message.Message):
    __slots__ = ("status", "exit_code", "stdout", "stderr", "elapsed_seconds", "outputs")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    EXIT_CODE_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    ELAPSED_SECONDS_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    status: RunStatus
    exit_code: int
    stdout: bytes
    stderr: bytes
    elapsed_seconds: float
    outputs: _containers.RepeatedCompositeFieldContainer[File]
    def __init__(self, status: _Optional[_Union[RunStatus, str]] = ..., exit_code: _Optional[int] = ..., stdout: _Optional[bytes] = ..., stderr: _Optional[bytes] = ..., elapsed_seconds: _Optional[float] = ..., outputs: _Optional[_Iterable[_Union[File, _Mapping]]] = ...) -> None: ...

class UploadResponse(_message.Message):
    __slots__ = ("name", "size_bytes")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    name: str
    size_bytes: int
    def __init__(self, name: _Optional[str] = ..., size_bytes: _Optional[int] = ...) -> None: ...

class GetFileRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class DeleteFileRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class DeleteFileResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListFilesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListFilesResponse(_message.Message):
    __slots__ = ("files",)
    FILES_FIELD_NUMBER: _ClassVar[int]
    files: _containers.RepeatedCompositeFieldContainer[FileInfo]
    def __init__(self, files: _Optional[_Iterable[_Union[FileInfo, _Mapping]]] = ...) -> None: ...

class RunRequest(_message.Message):
    __slots__ = ("model", "file_root", "timeout_seconds")
    MODEL_FIELD_NUMBER: _ClassVar[int]
    FILE_ROOT_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    model: str
    file_root: str
    timeout_seconds: int
    def __init__(self, model: _Optional[str] = ..., file_root: _Optional[str] = ..., timeout_seconds: _Optional[int] = ...) -> None: ...

class RunOutput(_message.Message):
    __slots__ = ("started", "output", "completed", "file")
    STARTED_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    FILE_FIELD_NUMBER: _ClassVar[int]
    started: RunStarted
    output: OutputChunk
    completed: RunCompleted
    file: FileChunk
    def __init__(self, started: _Optional[_Union[RunStarted, _Mapping]] = ..., output: _Optional[_Union[OutputChunk, _Mapping]] = ..., completed: _Optional[_Union[RunCompleted, _Mapping]] = ..., file: _Optional[_Union[FileChunk, _Mapping]] = ...) -> None: ...

class RunPipelineRequest(_message.Message):
    __slots__ = ("steps", "timeout_seconds")
    STEPS_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    steps: _containers.RepeatedCompositeFieldContainer[PipelineStep]
    timeout_seconds: int
    def __init__(self, steps: _Optional[_Iterable[_Union[PipelineStep, _Mapping]]] = ..., timeout_seconds: _Optional[int] = ...) -> None: ...

class PipelineStep(_message.Message):
    __slots__ = ("id", "model", "file_root", "inputs", "depends_on", "timeout_seconds")
    ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    FILE_ROOT_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    DEPENDS_ON_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    id: str
    model: str
    file_root: str
    inputs: _containers.RepeatedCompositeFieldContainer[File]
    depends_on: _containers.RepeatedScalarFieldContainer[str]
    timeout_seconds: int
    def __init__(self, id: _Optional[str] = ..., model: _Optional[str] = ..., file_root: _Optional[str] = ..., inputs: _Optional[_Iterable[_Union[File, _Mapping]]] = ..., depends_on: _Optional[_Iterable[str]] = ..., timeout_seconds: _Optional[int] = ...) -> None: ...

class PipelineOutput(_message.Message):
    __slots__ = ("pipeline_started", "step", "pipeline_completed")
    PIPELINE_STARTED_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_COMPLETED_FIELD_NUMBER: _ClassVar[int]
    pipeline_started: PipelineStarted
    step: StepEvent
    pipeline_completed: PipelineCompleted
    def __init__(self, pipeline_started: _Optional[_Union[PipelineStarted, _Mapping]] = ..., step: _Optional[_Union[StepEvent, _Mapping]] = ..., pipeline_completed: _Optional[_Union[PipelineCompleted, _Mapping]] = ...) -> None: ...

class PipelineStarted(_message.Message):
    __slots__ = ("step_ids",)
    STEP_IDS_FIELD_NUMBER: _ClassVar[int]
    step_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, step_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class StepEvent(_message.Message):
    __slots__ = ("step_id", "started", "output", "completed", "file")
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    STARTED_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    FILE_FIELD_NUMBER: _ClassVar[int]
    step_id: str
    started: RunStarted
    output: OutputChunk
    completed: RunCompleted
    file: FileChunk
    def __init__(self, step_id: _Optional[str] = ..., started: _Optional[_Union[RunStarted, _Mapping]] = ..., output: _Optional[_Union[OutputChunk, _Mapping]] = ..., completed: _Optional[_Union[RunCompleted, _Mapping]] = ..., file: _Optional[_Union[FileChunk, _Mapping]] = ...) -> None: ...

class PipelineCompleted(_message.Message):
    __slots__ = ("all_succeeded", "elapsed_seconds", "skipped_steps")
    ALL_SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    ELAPSED_SECONDS_FIELD_NUMBER: _ClassVar[int]
    SKIPPED_STEPS_FIELD_NUMBER: _ClassVar[int]
    all_succeeded: bool
    elapsed_seconds: float
    skipped_steps: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, all_succeeded: bool = ..., elapsed_seconds: _Optional[float] = ..., skipped_steps: _Optional[_Iterable[str]] = ...) -> None: ...
