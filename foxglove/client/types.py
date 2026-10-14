"""Dictionary inputs for episode versions and dataset membership."""

from datetime import datetime
from typing import Dict, List, Optional, Union

from typing_extensions import Literal, TypedDict


class TextLabel(TypedDict):
    type: Literal["TEXT"]
    content: str


class UriLabelContent(TypedDict):
    uri: str
    checksum: str
    content_type: str


class UriLabel(TypedDict):
    type: Literal["URI"]
    content: UriLabelContent


EpisodeLabelValue = Union[TextLabel, UriLabel]


class RecordingChanges(TypedDict, total=False):
    add: List[str]
    remove: List[str]


class EpisodeUpdate(TypedDict, total=False):
    applies_to_version: int
    start_time: datetime
    end_time: datetime
    recordings: RecordingChanges
    labels: Dict[str, Optional[EpisodeLabelValue]]


class BulkEpisodeUpdate(EpisodeUpdate):
    id: str


class _DatasetEpisodeId(TypedDict):
    id: str


class DatasetEpisodePin(_DatasetEpisodeId, total=False):
    version: int


DatasetEpisodeReference = Union[str, DatasetEpisodePin]
