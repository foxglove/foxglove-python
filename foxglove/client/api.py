import base64
from urllib.parse import quote as urlquote
import copy
import datetime
import json
import os
import warnings
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import IO, Any, Callable, Dict, List, Optional, TypeVar, Union, cast

import arrow
import requests
from mcap.decoder import DecoderFactory
from mcap.reader import make_reader
from mcap.records import Schema as McapSchema
from mcap.well_known import MessageEncoding
from typing_extensions import Protocol

from .pagination import Page
from .dataset_download import download_episode, export_dataset


class _JsonDecoderFactory(DecoderFactory):
    def decoder_for(self, message_encoding: str, schema: Optional[McapSchema]):
        _ = schema

        def decoder(message_content: bytes):
            return json.loads(message_content.decode("utf-8"))

        if message_encoding == MessageEncoding.JSON:
            return decoder
        return None


DEFAULT_DECODER_FACTORIES: List[DecoderFactory] = [_JsonDecoderFactory()]

T = TypeVar("T")
_UNSET = object()


def _validate_episode_range(start, end):
    if (start is None) != (end is None):
        raise ValueError("start and end must be supplied together")


def _episode_include(
    include_recordings: bool,
    include_properties: bool,
    include_creation_context: bool = False,
):
    include = []
    if include_recordings:
        include.append("recordings")
    if include_properties:
        include.append("properties")
    if include_creation_context:
        include.append("creationContext")
    return ",".join(include) or None


try:
    from mcap_ros1.decoder import DecoderFactory as Ros1DecoderFactory

    DEFAULT_DECODER_FACTORIES.append(Ros1DecoderFactory())
except ModuleNotFoundError:
    pass

try:
    from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory

    DEFAULT_DECODER_FACTORIES.append(ProtobufDecoderFactory())
except ModuleNotFoundError:
    pass

try:
    from mcap_ros2.decoder import DecoderFactory as Ros2DecoderFactory

    DEFAULT_DECODER_FACTORIES.append(Ros2DecoderFactory())
except ModuleNotFoundError:
    pass


def camelize(snake_name: Optional[str]) -> Optional[str]:
    """
    Convert a valid snake_case field name to camelCase for the API
    """
    if not snake_name:
        return snake_name
    parts = snake_name.split("_")
    return "".join([parts[0]] + [w.title() for w in parts[1:]])


def bool_query_param(val: bool) -> Optional[str]:
    """
    Serialize a bool to an API query parameter (e.g. True -> "true")
    """
    return str(val).lower() if val is not None else None


def without_nulls(params: Dict[str, Union[T, None]]) -> Dict[str, T]:
    """
    Filter out `None` values from params
    """
    return {key: val for key, val in params.items() if val is not None}


class FoxgloveException(Exception):
    pass


class ProgressCallback(Protocol):
    def __call__(self, progress: int) -> None:
        pass


class SizeProgressCallback(Protocol):
    def __call__(self, size: int, progress: int) -> None:
        pass


class OutputFormat(Enum):
    bag = "bag1"
    mcap = "mcap"
    mcap0 = "mcap0"


class CompressionFormat(Enum):
    none = ""
    zstd = "zstd"
    lz4 = "lz4"


class ProgressBufferReader(IO[Any]):
    def __init__(
        self,
        buf: Union[bytes, IO[Any]],
        callback: Optional[SizeProgressCallback] = None,
    ):
        self.__callback = callback
        self.__progress = 0
        if isinstance(buf, bytes):
            self.__length = len(buf)
            self.__buf = BytesIO(buf)
        else:
            self.__length = os.fstat(buf.fileno()).st_size
            self.__buf = buf

    def __len__(self):
        return self.__length

    def read(self, n: int = -1) -> bytes:
        chunk = self.__buf.read(n) or bytes()
        self.__progress += int(len(chunk))
        if self.__callback:
            self.__callback(size=self.__length or 0, progress=self.__progress)
        return chunk

    def tell(self) -> int:
        return self.__progress


def json_or_raise(response: requests.Response):
    """
    Returns parsed JSON response, or raises if API returned an error.
    For client errors (4xx), the server message is included.
    """
    try:
        json = response.json()
    except ValueError:
        raise requests.exceptions.HTTPError(
            "500 Server Error: Unexpected format", response=response
        )

    if 400 <= response.status_code < 500 and isinstance(json, dict):
        response.reason = json.get("error", response.reason)

    response.raise_for_status()

    return json


def _download_response_with_progress(
    response: requests.Response,
    callback: Optional[ProgressCallback] = None,
):
    try:
        response.raise_for_status()
        data = BytesIO()
        for chunk in response.iter_content(chunk_size=32 * 1024):
            data.write(chunk)
            if callback:
                callback(progress=data.tell())
        return data.getvalue()
    finally:
        response.close()


def _iter_decoded_messages(response: requests.Response, decoder_factories):
    try:
        reader = make_reader(
            cast(IO[bytes], response.raw), decoder_factories=decoder_factories
        )
        # messages from Foxglove are already in log-time order.
        # specifying log_time_order=false allows us to skip a sort() in the MCAP library
        # after all messages are loaded.
        yield from reader.iter_decoded_messages(log_time_order=False)
    finally:
        response.close()


def _download_stream_with_progress(
    url: str,
    callback: Optional[ProgressCallback] = None,
):
    response = requests.get(url, stream=True)
    return _download_response_with_progress(response, callback=callback)


class Client:
    def __init__(self, token: str, host: str = "api.foxglove.dev"):
        self.__token = token
        self.__session = requests.Session()
        self.__session.headers.update(
            {
                "Content-type": "application/json",
                "Authorization": "Bearer " + self.__token,
            }
        )
        self.__host = host

    def __url__(self, path: str):
        return f"https://{self.__host}{path}"

    def create_event(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        project_id: Optional[str] = None,
        start: datetime.datetime,
        end: Optional[datetime.datetime],
        metadata: Optional[Dict[str, str]] = None,
        properties: Optional[Dict[str, Union[str, bool, float, int]]] = None,
        event_type_id: Optional[str] = None,
    ):
        """
        Creates a new event.

        device_id: The id of the device associated with this event.
        device_name: The name of the device associated with this event.
        project_id: Optional Project to associate with this event.
            Required when `device_name` is shared across projects.
        start: The event start time.
        end: The event end time. If not provided, an instantaneous event (with end == start)
            is created.
        metadata: Optional metadata attached to the event.
        properties: Optional custom properties for the event.
            Each key must be defined as a custom property for your organization,
            and each value must be of the appropriate type
        event_type_id: Optional Event Type ID for the event.
            If provided the event's custom properties must conform to the Event Type's schema.
        """
        if metadata is None:
            metadata = {}
        if end is None:
            end = start
        if device_id is None and device_name is None:
            raise RuntimeError(
                "device_id or device_name must be provided to create_event"
            )
        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "projectId": project_id,
            "start": start.astimezone().isoformat(),
            "end": end.astimezone().isoformat(),
            "metadata": metadata,
            "properties": properties,
            "eventTypeId": event_type_id,
        }
        response = self.__session.post(
            self.__url__("/v1/events"),
            json={k: v for k, v in params.items() if v is not None},
        )

        return _event_dict(json_or_raise(response))

    def update_event(
        self,
        *,
        event_id: str,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        metadata: Optional[Dict[str, str]] = None,
        properties: Optional[Dict[str, Union[str, bool, float, int]]] = None,
        event_type_id: Optional[str] = None,
    ):
        """
        Updates an existing event.

        event_id: The id of the event to update.
        start: New event start time.
        end: New event end time.
        metadata: An object with user-defined string keys and string values.
            Key order is not preserved. Will replace all existing metadata.
        properties: A key-value map, where each key is one of your pre-defined device
            custom property keys. Keys which are not recognized as custom properties
            will be ignored. Keys which are not included in the request, but exist on
            the device, will be unchanged. To unset a property, pass None as the value.

            Must conform to the event_type_id schema if provided.
        event_type_id: New Event Type ID for the event.
        """
        params = {
            "start": start.astimezone().isoformat() if start else None,
            "end": end.astimezone().isoformat() if end else None,
            # allow sending {} for metadata or properties
            "metadata": metadata if metadata is not None else None,
            "properties": properties if properties is not None else None,
            # allow sending "" to unset the event_type_id
            "eventTypeId": event_type_id if event_type_id is not None else None,
        }
        response = self.__session.patch(
            self.__url__(f"/v1/events/{event_id}"),
            json=without_nulls(params),
        )

        return _event_dict(json_or_raise(response))

    def delete_event(
        self,
        *,
        event_id: str,
    ):
        """
        Deletes an event.

        event_id: The id of the event to delete.
        """
        response = self.__session.delete(self.__url__(f"/v1/events/{event_id}"))
        return json_or_raise(response)

    def get_event(self, *, event_id: str):
        """
        Gets a single event.

        event_id: The id of the event.
        """
        response = self.__session.get(self.__url__(f"/v1/events/{event_id}"))
        return _event_dict(json_or_raise(response))

    def get_events(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        query: Optional[str] = None,
        project_id: Optional[str] = None,
        event_type_id: Optional[str] = None,
        event_id: Optional[str] = None,
        created_after: Optional[datetime.datetime] = None,
        updated_after: Optional[datetime.datetime] = None,
        query_fields: Optional[List[str]] = None,
    ):
        """
        Retrieves events.

        device_id: Id of the device associated with the events.
        device_name: Name of the device associated with the events.
        sort_by: Optionally sort records by this field name (e.g. "device_id").
        sort_order: Optionally specify the sort order, either "asc" or "desc".
        limit: Optionally limit the number of records returned.
        offset: Optionally offset the results by this many records.
        start: Optionally exclude records before this time.
        end: Optionally exclude records after this time.
        query: optional query string to filter events by metadata.
            See https://foxglove.dev/docs/api#tag/Events/paths/~1events/get for a syntax definition
            of `query`.
        project_id: Optional Project to filter events by.
        event_type_id: Optional Event Type ID to filter events by.
        event_id: Optionally filter events by exact event ID.
        created_after: Optionally return only events created after this time.
        updated_after: Optionally return only events updated after this time.
        query_fields: Optionally specify which fields `query` filters on:
            "metadata" (the default) and/or "properties".
        """
        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "sortBy": camelize(sort_by),
            "sortOrder": sort_order,
            "limit": limit,
            "offset": offset,
            "start": start.astimezone().isoformat() if start else None,
            "end": end.astimezone().isoformat() if end else None,
            "query": query,
            "projectId": project_id,
            "eventTypeId": event_type_id,
            "eventId": event_id,
            "createdAfter": (
                created_after.astimezone().isoformat() if created_after else None
            ),
            "updatedAfter": (
                updated_after.astimezone().isoformat() if updated_after else None
            ),
            "queryFields": ",".join(query_fields) if query_fields else None,
        }
        response = self.__session.get(
            self.__url__("/v1/events"),
            params={k: v for k, v in params.items() if v is not None},
        )

        return [_event_dict(event) for event in json_or_raise(response)]

    def get_event_types(self):
        """Return all event types."""
        response = self.__session.get(self.__url__("/v1/event-types"))
        return [_event_type_dict(event_type) for event_type in json_or_raise(response)]

    def get_event_type(self, *, event_type_id: str):
        """
        Gets a single event type.

        :param event_type_id: The id of the event type.
        """
        response = self.__session.get(self.__url__(f"/v1/event-types/{event_type_id}"))
        return _event_type_dict(json_or_raise(response))

    def create_event_type(
        self,
        *,
        name: str,
        custom_properties: List[Dict[str, Any]],
        color_name: Optional[str] = None,
    ):
        """
        Creates an event type.

        :param name: Name of the event type.
        :param custom_properties: Ordered custom properties for events of this type,
            each a dict with the property's ``id`` and whether it is ``required``.
        :param color_name: Optional color name, e.g. "red".
        """
        response = self.__session.post(
            self.__url__("/v1/event-types"),
            json=without_nulls(
                {
                    "name": name,
                    "colorName": color_name,
                    "customProperties": custom_properties,
                }
            ),
        )
        return _event_type_dict(json_or_raise(response))

    def update_event_type(
        self,
        *,
        event_type_id: str,
        name: Optional[str] = None,
        color_name: Optional[str] = None,
        custom_properties: Optional[List[Dict[str, Any]]] = None,
    ):
        """
        Updates an event type. At least one field must be set.

        :param event_type_id: The id of the event type.
        :param name: Optional new name.
        :param color_name: Optional new color name.
        :param custom_properties: Optional custom properties, each a dict with the
            property's ``id`` and whether it is ``required``. Replaces the existing list.
        """
        response = self.__session.patch(
            self.__url__(f"/v1/event-types/{event_type_id}"),
            json=without_nulls(
                {
                    "name": name,
                    "colorName": color_name,
                    "customProperties": custom_properties,
                }
            ),
        )
        return _event_type_dict(json_or_raise(response))

    def delete_event_type(self, *, event_type_id: str):
        """
        Deletes an event type.

        :param event_type_id: The id of the event type.
        """
        response = self.__session.delete(
            self.__url__(f"/v1/event-types/{event_type_id}")
        )
        json_or_raise(response)

    def get_messages(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        start: datetime.datetime,
        end: datetime.datetime,
        topics: Optional[List[str]] = None,
        decoder_factories: Optional[List[DecoderFactory]] = None,
    ):
        """
        Returns a list of tuples of (topic, raw mcap record, decoded message).

        .. deprecated:: 0.13.0
            Use :func:`iter_messages` instead.

        device_id: The id of the device that originated the desired data.
        device_name: The name of the device that originated the desired data.
        start: The earliest time from which to retrieve data.
        end: The latest time from which to retrieve data.
        topics: An optional list of topics to retrieve.
            All topics will be retrieved if this is omitted.
        decoder_factories: an optional list of :py:class:`~mcap.decoder.DecoderFactory` instances
            used to decode message content.
        """
        if topics is None:
            topics = []
        warnings.warn("Use `iter_messages` instead.", DeprecationWarning, stacklevel=2)
        data = self.download_data(
            device_name=device_name,
            device_id=device_id,
            start=start,
            end=end,
            topics=topics,
        )
        if decoder_factories is None:
            # We deep-copy here as these factories might be mutated
            decoder_factories = copy.deepcopy(DEFAULT_DECODER_FACTORIES)
        reader = make_reader(BytesIO(data), decoder_factories=decoder_factories)
        return [
            (channel.topic, message, decoded_message)
            # messages from Foxglove are already in log-time order.
            # specifying log_time_order=false allows us to skip a sort() in the MCAP library
            # after all messages are loaded.
            for _, channel, message, decoded_message in reader.iter_decoded_messages(
                log_time_order=False,
            )
        ]

    def iter_messages(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        topics: Optional[List[str]] = None,
        decoder_factories: Optional[List[DecoderFactory]] = None,
        project_id: Optional[str] = None,
        episode_id: Optional[str] = None,
        episode_version: Optional[int] = None,
        recording_id: Optional[str] = None,
        recording_key: Optional[str] = None,
        replay_policy: Optional[str] = None,
        replay_lookback_seconds: Optional[float] = None,
    ):
        """
        yields a stream of (schema, channel, message, decoded message) values.

        device_id: The id of the device that originated the desired data.
        device_name: The name of the device that originated the desired data.
        session_id: ID of a session to download data from
        session_key: Key of a session to download data from
        start: The earliest time from which to retrieve data.
        end: The latest time from which to retrieve data.
        topics: An optional list of topics to retrieve.
            All topics will be retrieved if this is omitted.
        decoder_factories: an optional list of :py:class:`~mcap.decoder.DecoderFactory` instances
            used to decode message content.
        project_id: The id of the project associated with the device. Required when using
            device_name as an identifier in multi-project organizations.
        episode_id: ID of an episode to download. Its time range is used when start and end
            are omitted.
        episode_version: Episode version to stream. Requires ``episode_id``; the latest
            version is used when omitted.
        recording_id: ID of a recording to stream.
        recording_key: Key of a recording to stream.
        replay_policy: Optional replay policy. "lastPerChannel" includes the most recent
            message on each channel from before ``start``, within ``replay_lookback_seconds``.
        replay_lookback_seconds: How far before ``start`` to look for those messages.
            Only used with ``replay_policy="lastPerChannel"``.
        """
        if topics is None:
            topics = []
        stream_link = self._make_stream_link(
            device_id=device_id,
            device_name=device_name,
            session_id=session_id,
            session_key=session_key,
            start=start,
            end=end,
            topics=topics,
            project_id=project_id,
            episode_id=episode_id,
            episode_version=episode_version,
            recording_id=recording_id,
            recording_key=recording_key,
            replay_policy=replay_policy,
            replay_lookback_seconds=replay_lookback_seconds,
        )
        response = requests.get(stream_link, stream=True)
        try:
            response.raise_for_status()
        except Exception:
            response.close()
            raise
        if decoder_factories is None:
            # We deep-copy here as these factories might be mutated
            decoder_factories = copy.deepcopy(DEFAULT_DECODER_FACTORIES)
        return _iter_decoded_messages(response, decoder_factories)

    def download_recording_data(
        self,
        *,
        id: Optional[str] = None,
        key: Optional[str] = None,
        output_format: OutputFormat = OutputFormat.mcap,
        include_attachments: bool = False,
        callback: Optional[ProgressCallback] = None,
        topics: Optional[List[str]] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        compression_format: Optional[CompressionFormat] = None,
        replay_policy: Optional[str] = None,
        replay_lookback_seconds: Optional[float] = None,
    ):
        """
        Returns raw data bytes for a recording.

        :param id: the ID of the recording.
        :param key: the key of the recording.
        :param include_attachments: whether to include MCAP attachments in the returned data.
        :param output_format: The output format of the data, defaulting to .mcap.
            Note: You can only export a .bag file if you originally uploaded a .bag file.
        :param callback: an optional callback to report download progress.
        :param topics: An optional list of topics to retrieve; all topics when omitted.
        :param start: Optional start of a time range within the recording. Requires ``end``.
        :param end: Optional end of a time range within the recording. Requires ``start``.
        :param compression_format: Compression format for MCAP chunks.
        :param replay_policy: Optional replay policy. "lastPerChannel" includes the most
            recent message on each channel from before ``start``, within
            ``replay_lookback_seconds``.
        :param replay_lookback_seconds: How far before ``start`` to look for those
            messages. Only used with ``replay_policy="lastPerChannel"``.
        """
        if id is None and key is None:
            raise RuntimeError("id or key must be provided")
        link = self._make_stream_link(
            recording_id=id,
            recording_key=key,
            include_attachments=include_attachments,
            output_format=output_format,
            topics=topics,
            start=start,
            end=end,
            compression_format=compression_format,
            replay_policy=replay_policy,
            replay_lookback_seconds=replay_lookback_seconds,
        )
        return _download_stream_with_progress(link, callback=callback)

    def _make_stream_link(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        topics: Optional[List[str]] = None,
        output_format: OutputFormat = OutputFormat.mcap,
        compression_format: Optional[CompressionFormat] = None,
        project_id: Optional[str] = None,
        episode_id: Optional[str] = None,
        episode_version: Optional[int] = None,
        include_attachments: Optional[bool] = None,
        recording_id: Optional[str] = None,
        recording_key: Optional[str] = None,
        replay_policy: Optional[str] = None,
        replay_lookback_seconds: Optional[float] = None,
    ) -> str:
        if topics is None:
            topics = []
        by_recording = recording_id is not None or recording_key is not None
        if episode_version is not None and episode_id is None:
            raise RuntimeError("episode_version requires episode_id")
        if (
            device_id is None
            and device_name is None
            and session_id is None
            and session_key is None
            and episode_id is None
            and not by_recording
        ):
            raise RuntimeError(
                "device_id or device_name or session_id or session_key or episode_id "
                "or recording_id or recording_key must be provided"
            )
        by_device = device_id is not None or device_name is not None
        if episode_id is not None and (
            by_device
            or session_id is not None
            or session_key is not None
            or by_recording
        ):
            raise RuntimeError("episode_id cannot be combined with another identifier")
        if by_recording and (session_id is not None or session_key is not None):
            raise RuntimeError(
                "recording_id or recording_key cannot be combined with a session"
            )
        needs_range = episode_id is None and (by_device or not by_recording)
        if needs_range and (start is None or end is None):
            raise RuntimeError(
                "start and end must be provided unless using episode_id, "
                "recording_id, or recording_key"
            )

        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "sessionId": session_id,
            "sessionKey": session_key,
            "episodeId": episode_id,
            "episodeVersion": episode_version,
            "includeAttachments": include_attachments,
            "recordingId": recording_id,
            "recordingKey": recording_key,
            "replayPolicy": replay_policy,
            "replayLookbackSeconds": replay_lookback_seconds,
            "end": end.astimezone().isoformat() if end else None,
            "outputFormat": output_format.value,
            "start": start.astimezone().isoformat() if start else None,
            "topics": topics,
            "projectId": project_id,
        }
        if compression_format is not None:
            params["compressionFormat"] = compression_format.value

        link_response = self.__session.post(
            self.__url__("/v1/data/stream"),
            json={k: v for k, v in params.items() if v is not None},
        )

        json = json_or_raise(link_response)
        return json["link"]

    def download_data(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        topics: Optional[List[str]] = None,
        output_format: OutputFormat = OutputFormat.mcap,
        compression_format: Optional[CompressionFormat] = None,
        callback: Optional[ProgressCallback] = None,
        project_id: Optional[str] = None,
        episode_id: Optional[str] = None,
        episode_version: Optional[int] = None,
        include_attachments: Optional[bool] = None,
        recording_id: Optional[str] = None,
        recording_key: Optional[str] = None,
        replay_policy: Optional[str] = None,
        replay_lookback_seconds: Optional[float] = None,
    ) -> bytes:
        """
        Returns raw data bytes for a device and time range.

        device_id: The id of the device that originated the desired data.
        device_name: The name of the device that originated the desired data.
        session_id: ID of a session to download data from
        session_key: Key of a session to download data from
        start: The earliest time from which to retrieve data.
        end: The latest time from which to retrieve data.
        topics: An optional list of topics to retrieve.
            All topics will be retrieved if this is omitted.
        output_format: The output format of the data, either .bag or .mcap, defaulting to .mcap.
        compression_format: Compression format for MCAP chunks. Can be lz4, zstd or no compression.
            If omitted the API will select a default compression format. See API documentation
            for more info https://docs.foxglove.dev/api#tag/Stream-data/paths/~1data~1stream/post
        project_id: The id of the project associated with the device. Required when using
            device_name as an identifier in multi-project organizations.
        episode_id: ID of an episode to download. Its time range is used when start and end
            are omitted.
        episode_version: Episode version to download. Requires ``episode_id``; the latest
            version is used when omitted.
        include_attachments: Whether to include MCAP attachments. Only valid for MCAP
            output and when downloading by recording, session, or episode.
        recording_id: ID of a recording to download.
        recording_key: Key of a recording to download.
        replay_policy: Optional replay policy. "lastPerChannel" includes the most recent
            message on each channel from before ``start``, within ``replay_lookback_seconds``.
        replay_lookback_seconds: How far before ``start`` to look for those messages.
            Only used with ``replay_policy="lastPerChannel"``.
        """
        if topics is None:
            topics = []
        return _download_stream_with_progress(
            self._make_stream_link(
                device_id=device_id,
                device_name=device_name,
                session_id=session_id,
                session_key=session_key,
                start=start,
                end=end,
                topics=topics,
                output_format=output_format,
                compression_format=compression_format,
                project_id=project_id,
                episode_id=episode_id,
                episode_version=episode_version,
                include_attachments=include_attachments,
                recording_id=recording_id,
                recording_key=recording_key,
                replay_policy=replay_policy,
                replay_lookback_seconds=replay_lookback_seconds,
            ),
            callback=callback,
        )

    def get_site_tokens(self, *, site_id: Optional[str] = None):
        """
        Returns a list of site tokens.

        :param site_id: Optional site to filter tokens by.
        """
        response = self.__session.get(
            self.__url__("/v1/site-tokens"),
            params=without_nulls({"siteId": site_id}),
        )
        return [_site_token_dict(token) for token in json_or_raise(response)]

    def create_site_token(self, *, site_id: str):
        """
        Creates a site token. The returned ``token`` is only available on creation.

        :param site_id: The site to associate with the token.
        """
        response = self.__session.post(
            self.__url__("/v1/site-tokens"), json={"siteId": site_id}
        )
        return _site_token_dict(json_or_raise(response))

    def delete_site_token(self, *, site_token_id: str):
        """
        Deletes a site token.

        :param site_token_id: The id of the site token.
        """
        response = self.__session.delete(
            self.__url__(f"/v1/site-tokens/{site_token_id}")
        )
        json_or_raise(response)

    def get_coverage(
        self,
        *,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        tolerance: Optional[int] = None,
        project_id: Optional[str] = None,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
        recording_id: Optional[str] = None,
        recording_key: Optional[str] = None,
        include_edge_recordings: Optional[bool] = None,
    ):
        """
        List coverage ranges for data.

        :param start: The earliest time after which to retrieve data. Required with
            ``end`` unless filtering by recording or session.
        :param end: The latest time before which to retrieve data. Required with
            ``start`` unless filtering by recording or session.
        :param device_id: Optional device id to limit data by.
        :param tolerance: Minimum interval (in seconds) that ranges must be separated by
            to be considered discrete.
        :param project_id: Optional Project to filter coverage by.
        :param session_id: Optional Session ID to filter coverage by.
        :param session_key: Optional Session key to filter coverage by.
        :param recording_id: Optional recording ID to filter coverage by.
        :param recording_key: Optional recording key to filter coverage by.
        :param include_edge_recordings: Optionally include recordings from an Edge Site
            or a device running Foxlet. Ranges then include ``import_status``.
        """
        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "tolerance": tolerance,
            "start": start.astimezone().isoformat() if start else None,
            "end": end.astimezone().isoformat() if end else None,
            "projectId": project_id,
            "sessionId": session_id,
            "sessionKey": session_key,
            "recordingId": recording_id,
            "recordingKey": recording_key,
            "includeEdgeRecordings": (
                bool_query_param(include_edge_recordings)
                if include_edge_recordings is not None
                else None
            ),
        }
        response = self.__session.get(
            self.__url__("/v1/data/coverage"),
            params={k: v for k, v in params.items() if v is not None},
        )
        json = json_or_raise(response)

        return [
            {
                "device_id": c.get("deviceId"),
                "device": c.get("device"),
                "start": arrow.get(c["start"]).datetime,
                "end": arrow.get(c["end"]).datetime,
                "import_status": c.get("importStatus"),
            }
            for c in json
        ]

    def get_sites(self):
        """Returns a list of all sites."""
        response = self.__session.get(self.__url__("/v1/sites"))
        return [_site_dict(site) for site in json_or_raise(response)]

    def get_site(self, *, site_id: str):
        """
        Gets a single site.

        :param site_id: The id of the site.
        """
        response = self.__session.get(self.__url__(f"/v1/sites/{site_id}"))
        return _site_dict(json_or_raise(response))

    def create_site(
        self,
        *,
        name: str,
        site_type: str,
        retain_recordings_seconds: Optional[float] = None,
    ):
        """
        Creates a site.

        :param name: A name for the site.
        :param site_type: The type of site to create: "self-hosted" or "edge".
        :param retain_recordings_seconds: Optional retention period for recordings
            created at the site. Zero retains recordings indefinitely.
            Only available on Edge Sites.
        """
        response = self.__session.post(
            self.__url__("/v1/sites"),
            json=without_nulls(
                {
                    "name": name,
                    "type": site_type,
                    "retainRecordingsSeconds": retain_recordings_seconds,
                }
            ),
        )
        return _site_dict(json_or_raise(response))

    def update_site(
        self,
        *,
        site_id: str,
        name: Optional[str] = None,
        retain_recordings_seconds: Optional[float] = None,
        url: Optional[str] = None,
    ):
        """
        Updates a site.

        :param site_id: The id of the site.
        :param name: Optional new name.
        :param retain_recordings_seconds: Optional retention period for recordings
            created at the site. Zero retains recordings indefinitely.
            Only available on Edge Sites.
        :param url: Optional URL that a self-hosted Primary Site exposes for
            accessing its data.
        """
        response = self.__session.patch(
            self.__url__(f"/v1/sites/{site_id}"),
            json=without_nulls(
                {
                    "name": name,
                    "retainRecordingsSeconds": retain_recordings_seconds,
                    "url": url,
                }
            ),
        )
        return _site_dict(json_or_raise(response))

    def delete_site(self, *, site_id: str):
        """
        Deletes a site. Deletion is permanent and cannot be undone.

        :param site_id: The id of the site.
        """
        response = self.__session.delete(self.__url__(f"/v1/sites/{site_id}"))
        json_or_raise(response)

    def get_site_ingest_path_policy(self, *, site_id: str):
        """
        Gets the latest ingest path policy revision for a site.
        Returns None if no policy has been saved.

        :param site_id: The id of the site.
        """
        response = self.__session.get(
            self.__url__(f"/v1/sites/{site_id}/ingest-path-policy")
        )
        if response.status_code == 204:
            return None
        return _site_ingest_path_policy_dict(json_or_raise(response))

    def save_site_ingest_path_policy(
        self,
        *,
        site_id: str,
        rules: List[Dict[str, Any]],
        schema_version: int = 1,
        previous_revision: Optional[int] = None,
    ):
        """
        Saves an ingest path policy for a Bring Your Own Storage site.
        A new revision is recorded only when the policy changes.

        :param site_id: The id of the site.
        :param rules: Ordered rules, each a dict with a ``pattern`` and an ``action``
            in the API's format. The first matching rule wins; an empty list
            ignores every path.
        :param schema_version: Policy document version.
        :param previous_revision: Optional revision that was current when the policy
            was edited; use 0 when the site has no saved policy. The request fails
            if it is stale and the policy differs from the current one.
        """
        response = self.__session.post(
            self.__url__(f"/v1/sites/{site_id}/ingest-path-policy"),
            json=without_nulls(
                {
                    "schemaVersion": schema_version,
                    "rules": rules,
                    "previousRevision": previous_revision,
                }
            ),
        )
        return _site_ingest_path_policy_dict(json_or_raise(response))

    def notify_site_bucket(
        self,
        *,
        message_id: str,
        file_name: str,
        bucket_name: str,
        storage_provider: str,
        azure_storage_account_name: Optional[str] = None,
        notification_type: Optional[str] = None,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        project_id: Optional[str] = None,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
    ):
        """
        Notifies a Primary Site that an object in its bucket was created or removed.
        The client must be created with a site inbox notification token.

        :param message_id: Unique message identifier, used to deduplicate notifications.
        :param file_name: The name of the file in the bucket.
        :param bucket_name: The bucket name.
        :param storage_provider: "google_cloud", "aws", "azure", or "s3_compatible".
        :param azure_storage_account_name: Azure storage account containing the
            container. Required for Azure Bring Your Own Storage sites.
        :param notification_type: "object_created" (the default) or "object_removed".
        :param device_id: Optional device to associate with the recording.
        :param device_name: Optional name of the device to associate with the
            recording; the device is created if it does not exist.
        :param project_id: Optional project to associate with the recording.
        :param session_id: Optional id of a session to associate with the recording.
        :param session_key: Optional key of a session to associate with the recording.
        """
        response = self.__session.post(
            self.__url__("/v1/site-bucket-notifications"),
            json=without_nulls(
                {
                    "messageId": message_id,
                    "fileName": file_name,
                    "bucketName": bucket_name,
                    "storageProvider": storage_provider,
                    "azureStorageAccountName": azure_storage_account_name,
                    "notificationType": notification_type,
                    "deviceId": device_id,
                    "deviceName": device_name,
                    "projectId": project_id,
                    "sessionId": session_id,
                    "sessionKey": session_key,
                }
            ),
        )
        json_or_raise(response)

    def get_device(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Gets a single device by name or id.

        :param device_id: The id of the device to retrieve.
        :param device_name: The name of the device to retrieve.
        :param project_id: Project to retrieve the device from.
            Required for multi-project organizations.
        """
        identifier = _device_identifier_for_path(device_id, device_name)
        response = self.__session.get(
            self.__url__(f"/v1/devices/{identifier}"),
            params={"projectId": project_id} if project_id is not None else None,
        )

        return _device_dict(json_or_raise(response))

    def get_devices(
        self,
        *,
        project_id: Optional[str] = None,
        query: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ):
        """
        Returns a list of all devices.

        :param project_id: Optional Project to filter devices by.
        :param query: Optional query string to filter devices by custom properties,
            e.g. "properties.key:value". See
            https://foxglove.dev/docs/api#tag/Devices/paths/~1devices/get for the syntax.
        :param sort_by: Optionally sort devices by "id", "name", or a custom property
            key prefixed with "properties.".
        :param sort_order: Optionally specify the sort order, either "asc" or "desc".
        :param limit: Optionally limit the number of devices returned.
        :param offset: Optionally offset the results by this many devices.
        """
        response = self.__session.get(
            self.__url__("/v1/devices"),
            params=without_nulls(
                {
                    "projectId": project_id,
                    "query": query,
                    "sortBy": sort_by,
                    "sortOrder": sort_order,
                    "limit": limit,
                    "offset": offset,
                }
            ),
        )

        json = json_or_raise(response)

        return [_device_dict(d) for d in json]

    def create_device(
        self,
        *,
        name: str,
        properties: Optional[Dict[str, Union[str, bool, float, int]]] = None,
        project_id: Optional[str] = None,
    ):
        """
        Creates a new device.

        :param name: The name of the device.
        :param properties: Optional custom properties for the device.
            Each key must be defined as a custom property for your organization,
            and each value must be of the appropriate type
        :param project_id: Project to create the device in.
            Required for multi-project organizations.
        """
        response = self.__session.post(
            self.__url__("/v1/devices"),
            json=without_nulls(
                {
                    "name": name,
                    "properties": properties,
                    "projectId": project_id,
                }
            ),
        )

        return _device_dict(json_or_raise(response))

    def update_device(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        new_name: Optional[str] = None,
        properties: Optional[Dict[str, Union[str, bool, float, int]]] = None,
        project_id: Optional[str] = None,
        retain_recordings_seconds: Optional[int] = None,
        enabled: Optional[bool] = None,
        remote_access_enabled: Optional[bool] = None,
        auto_delete_after_import: Optional[bool] = None,
    ):
        """
        Updates a device.

        :param device_id: The id of the device to retrieve.
        :param device_name: The name of the device to retrieve.
        :param new_name: Optional new name to assign to the device.
        :param properties: Optional custom properties to add to or edit on the device.
            Each key must be defined as a custom property for your organization
            and each value must be of the appropriate type.
        :param project_id: Project to retrieve the device from.
            Required for multi-project organizations.
        :param retain_recordings_seconds: Optional retention period for recordings
            created on a device running Foxlet. Zero retains recordings indefinitely.
        :param enabled: Optionally enable or disable the device. Recordings can't be
            uploaded for disabled devices.
        :param remote_access_enabled: Optionally enable or disable remote access.
        :param auto_delete_after_import: Optionally set whether a device running Foxlet
            deletes recordings once they are uploaded and accepted for import.
        """
        identifier = _device_identifier_for_path(device_id, device_name)

        response = self.__session.patch(
            self.__url__(f"/v1/devices/{identifier}"),
            params={"projectId": project_id} if project_id is not None else None,
            json=without_nulls(
                {
                    "name": new_name,
                    "properties": properties,
                    "retainRecordingsSeconds": retain_recordings_seconds,
                    "enabled": enabled,
                    "remoteAccessEnabled": remote_access_enabled,
                    "autoDeleteAfterImport": auto_delete_after_import,
                }
            ),
        )

        return _device_dict(json_or_raise(response))

    def delete_device(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """
        Deletes an existing device.

        Note: you must first delete all imports from the device; see `delete_import`.

        :param device_id: The id of the device.
        :param device_name: The name of the device.
        :param project_id: Project to delete the device from.
            Required for multi-project organizations.
        """
        identifier = _device_identifier_for_path(device_id, device_name)
        response = self.__session.delete(
            self.__url__(f"/v1/devices/{identifier}"),
            params={"projectId": project_id} if project_id is not None else None,
        )
        json_or_raise(response)

    def get_device_tokens(self, *, device_id: Optional[str] = None):
        """
        Returns a list of device tokens.

        :param device_id: Optional device ID to filter tokens by.
        """
        response = self.__session.get(
            self.__url__("/v1/device-tokens"),
            params=without_nulls({"deviceId": device_id}),
        )
        return [_device_token_dict(token) for token in json_or_raise(response)]

    def get_device_token(self, *, device_token_id: str):
        """
        Gets a single device token.

        :param device_token_id: The id of the device token.
        """
        response = self.__session.get(
            self.__url__(f"/v1/device-tokens/{device_token_id}")
        )
        return _device_token_dict(json_or_raise(response))

    def create_device_token(
        self,
        *,
        device_id: Optional[str] = None,
        project_id: Optional[str] = None,
        enabled: Optional[bool] = None,
        preapproved: Optional[bool] = None,
    ):
        """
        Creates a device token.

        The result includes the secret ``token`` value, which is only returned here.

        :param device_id: Optional device to associate the token with. Without one,
            the token can only be used to create a device.
        :param project_id: Project for the token. Required when `device_id` is not set;
            otherwise it must match the device's project.
        :param enabled: Optionally set whether the token is enabled.
        :param preapproved: Optionally set whether devices created with the token
            are preapproved.
        """
        response = self.__session.post(
            self.__url__("/v1/device-tokens"),
            json=without_nulls(
                {
                    "deviceId": device_id,
                    "projectId": project_id,
                    "enabled": enabled,
                    "preapproved": preapproved,
                }
            ),
        )
        json = json_or_raise(response)
        device_token = _device_token_dict(json)
        device_token["token"] = json.get("token")
        return device_token

    def update_device_token(self, *, device_token_id: str, enabled: bool):
        """
        Enables or disables a device token.

        :param device_token_id: The id of the device token.
        :param enabled: Whether the token is enabled.
        """
        response = self.__session.patch(
            self.__url__(f"/v1/device-tokens/{device_token_id}"),
            json={"enabled": enabled},
        )
        return _device_token_dict(json_or_raise(response))

    def delete_device_token(self, *, device_token_id: str):
        """
        Deletes a device token.

        :param device_token_id: The id of the device token.
        """
        response = self.__session.delete(
            self.__url__(f"/v1/device-tokens/{device_token_id}")
        )
        json_or_raise(response)

    def delete_import(self, *, device_id: Optional[str] = None, import_id: str):
        """
        Deletes an existing import.

        .. deprecated:: 0.16.2
            Use :func:`delete_recording` with a `recording_id` instead.

        :param device_id: The id of the device associated with the import. (Deprecated; ignored.)
        :param import_id: The id of the import to delete.
        """
        warnings.warn(
            "Use `delete_recording` with a `recording_id` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        response = self.__session.delete(
            self.__url__(f"/v1/data/imports/{import_id}"),
        )
        json_or_raise(response)

    def delete_recording(self, *, recording_id: str):
        response = self.__session.delete(
            self.__url__(f"/v1/recordings/{recording_id}"),
        )
        json_or_raise(response)

    def get_imports(
        self,
        *,
        device_id: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        data_start: Optional[datetime.datetime] = None,
        data_end: Optional[datetime.datetime] = None,
        include_deleted: bool = False,
        filename: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ):
        """
        Fetches imports.

        .. deprecated:: 0.16.0
            Use :func:`get_recordings` with `import_status: "complete"` instead.

        :param device_id: The id of the device associated with the import.
        :param start: Optionally filter by import start time.
        :param end: Optionally filter by import end time.
        :param data_start: Optionally filter by data start time.
        :param data_end: Optionally filter by data end time.
        :param include_deleted: Include deleted imports.
        :param filename: Optionally filter by matching filename.
        :param sort_by: Optionally sort records by this field name (e.g. "device_id").
        :param sort_order: Optionally specify the sort order, either "asc" or "desc".
        :param limit: Optionally limit the number of records returned.
        :param offset: Optionally offset the results by this many records.
        """
        warnings.warn(
            "Use `get_recordings` with `import_status: 'complete'` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        all_params = {
            "deviceId": device_id,
            "start": start.astimezone().isoformat() if start else None,
            "end": end.astimezone().isoformat() if end else None,
            "dataStart": data_start.astimezone().isoformat() if data_start else None,
            "dataEnd": data_end.astimezone().isoformat() if data_end else None,
            "includeDeleted": bool_query_param(include_deleted),
            "filename": filename,
            "sortBy": camelize(sort_by),
            "sortOrder": sort_order,
            "limit": limit,
            "offset": offset,
        }
        response = self.__session.get(
            self.__url__("/v1/data/imports"),
            params={k: v for k, v in all_params.items() if v is not None},
        )
        json = json_or_raise(response)

        return [
            {
                "import_id": i["importId"],
                "device_id": i.get("deviceId"),
                "import_time": arrow.get(i["importTime"]).datetime,
                "start": arrow.get(i["start"]).datetime,
                "end": arrow.get(i["end"]).datetime,
                "input_type": i["inputType"],
                "output_type": i["outputType"],
                "filename": i["filename"],
                "input_size": i["inputSize"],
                "total_output_size": i["totalOutputSize"],
            }
            for i in json
        ]

    def get_recording(
        self,
        *,
        recording_id: Optional[str] = None,
        recording_key: Optional[str] = None,
    ):
        """
        Fetches a single recording.

        :param recording_id: The ID of the recording to fetch.
        :param recording_key: The key of the recording to fetch.
        """
        identifier = _recording_identifier_for_path(recording_id, recording_key)
        response = self.__session.get(self.__url__(f"/v1/recordings/{identifier}"))
        return _recording_dict(json_or_raise(response))

    def import_recording_from_edge(self, *, recording_id: str):
        """
        Requests import of a recording from its Edge Site to its Primary Site.

        Returns the recording ID and its current import status. Poll
        :func:`get_recording` to observe changes to the import status.

        :param recording_id: The ID of the recording to import.
        """
        response = self.__session.post(
            self.__url__(f"/v1/recordings/{urlquote(recording_id, safe='')}/import"),
        )
        json = json_or_raise(response)
        return {"id": json.get("id"), "import_status": json.get("importStatus")}

    def get_pending_imports(
        self,
        *,
        request_id: Optional[str] = None,
        key: Optional[str] = None,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        site_backfill_job_id: Optional[str] = None,
        filename: Optional[str] = None,
        updated_since: Optional[datetime.datetime] = None,
        show_completed: bool = False,
        show_quarantined: bool = False,
        site_id: Optional[str] = None,
        project_id: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ):
        """
        Fetches pending imports: in-progress import jobs for uploaded recordings.

        :param request_id: Optionally filter by import request ID.
        :param key: Optionally filter by the recording key provided when uploading.
        :param device_id: Optionally filter by device ID.
        :param device_name: Optionally filter by device name.
        :param site_backfill_job_id: Optionally filter to imports enqueued by this site
            backfill job.
        :param filename: Optionally filter by exactly matching filename.
        :param updated_since: Optionally filter to imports updated since this time.
        :param show_completed: Include completed imports.
        :param show_quarantined: Include quarantined imports.
        :param site_id: Optionally filter by Primary Site ID.
        :param project_id: Optionally filter by project ID.
        :param sort_by: Optionally sort records by this field name (e.g. "updated_at").
        :param sort_order: Optionally specify the sort order, either "asc" or "desc".
        :param limit: Optionally limit the number of records returned.
        :param offset: Optionally offset the results by this many records.
        """
        response = self.__session.get(
            self.__url__("/v1/data/pending-imports"),
            params=without_nulls(
                {
                    "requestId": request_id,
                    "key": key,
                    "deviceId": device_id,
                    "deviceName": device_name,
                    "siteBackfillJobId": site_backfill_job_id,
                    "filename": filename,
                    "updatedSince": (
                        updated_since.astimezone().isoformat()
                        if updated_since
                        else None
                    ),
                    "showCompleted": bool_query_param(show_completed),
                    "showQuarantined": bool_query_param(show_quarantined),
                    "siteId": site_id,
                    "projectId": project_id,
                    "sortBy": camelize(sort_by),
                    "sortOrder": sort_order,
                    "limit": limit,
                    "offset": offset,
                }
            ),
        )
        return [_pending_import_dict(i) for i in json_or_raise(response)]

    def get_import_errors(
        self,
        *,
        device_name: Optional[str] = None,
        filename_matches: Optional[str] = None,
        error: Optional[str] = None,
        site_backfill_job_id: Optional[str] = None,
        project_id: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ):
        """
        Fetches imports that failed with an error.

        :param device_name: Optionally filter by device name.
        :param filename_matches: Optionally filter by a pattern matched against the filename.
        :param error: Optionally filter by a string in the error message.
        :param site_backfill_job_id: Optionally filter to errors from imports enqueued by
            this site backfill job.
        :param project_id: Optionally filter by project ID.
        :param sort_by: Optionally sort records by this field name (e.g. "updated_at").
        :param sort_order: Optionally specify the sort order, either "asc" or "desc".
        :param limit: Optionally limit the number of records returned.
        :param offset: Optionally offset the results by this many records.
        """
        response = self.__session.get(
            self.__url__("/v1/data/import-errors"),
            params=without_nulls(
                {
                    "deviceName": device_name,
                    "filenameMatches": filename_matches,
                    "error": error,
                    "siteBackfillJobId": site_backfill_job_id,
                    "projectId": project_id,
                    "sortBy": camelize(sort_by),
                    "sortOrder": sort_order,
                    "limit": limit,
                    "offset": offset,
                }
            ),
        )
        return [_pending_import_dict(i) for i in json_or_raise(response)]

    def get_recordings(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        path: Optional[str] = None,
        site_id: Optional[str] = None,
        edge_site_id: Optional[str] = None,
        import_status: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        project_id: Optional[str] = None,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
        topic: Optional[str] = None,
        metadata_query: Optional[str] = None,
    ):
        """Fetches recordings.

        :param device_id: Optionally filter responses by this device ID.
        :param device_name: Optionally filter responses by this device name.
        :param start: Optionally specify the start of an inclusive time range.
            Only recordings with messages within this time range will be returned.
        :param end: Optionally specify the end of an inclusive time range.
            Only recordings with messages within this time range will be returned.
        :param path: Optionally filter responses to recordings with a matching path.
        :param site_id: Optionally filter responses to recordings stored at this primary site.
        :param edge_site_id: Optionally filter responses to recordings stored at this edge site.
        :param import_status: Optionally filter responses to recordings with this import status.
        :param sort_by: Optionally sort returned recordings by a field in the response type.
            Specifying duration sorts by the duration between the recording start and end fields.
        :param sort_order: Optionally specify the sort order, either "asc" or "desc".
        :param limit: Optionally limit the number of records returned.
        :param offset: Optionally offset the results by this many records.
        :param project_id: Optional Project to filter recordings by.
        :param session_id: Optional Session ID to filter recordings by.
        :param session_key: Optional Session key to filter recordings by.
        :param topic: Optionally filter responses to recordings containing this topic.
        :param metadata_query: Optionally filter recordings by metadata using the
            recordings search query syntax (e.g. "key:value").
        """
        all_params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "start": start.astimezone().isoformat() if start else None,
            "end": end.astimezone().isoformat() if end else None,
            "site.id": site_id,
            "edgeSite.id": edge_site_id,
            "importStatus": import_status,
            "path": path,
            "sortBy": camelize(sort_by),
            "sortOrder": sort_order,
            "limit": limit,
            "offset": offset,
            "projectId": project_id,
            "sessionId": session_id,
            "sessionKey": session_key,
            "topic": topic,
            "metadataQuery": metadata_query,
        }
        response = self.__session.get(
            self.__url__("/v1/recordings"),
            params={k: v for k, v in all_params.items() if v is not None},
        )
        json = json_or_raise(response)

        out = [_recording_dict(i) for i in json]

        return out

    def get_attachments(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        recording_id: Optional[str] = None,
        site_id: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """List recording attachments.

        :param device_id: Optionally filter responses by this device ID.
        :param device_name: Optionally filter responses by this device name.
        :param recording_id: Optionally filter responses by this recording ID.
        :param site_id: Optionally filter responses by this site ID.
        :param sort_by: Optionally sort responses by this field name.
            currently only "log_time" is supported.
        :param sort_order: Optionally specify the sort order, either "asc" or "desc".
        :param limit: Optionally limit the number of records returned.
        :param offset: Optionally offset the results by this many records.
        :param project_id: Optional Project to filter attachments by.
        """
        all_params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "siteId": site_id,
            "recordingId": recording_id,
            "sortBy": camelize(sort_by),
            "sortOrder": sort_order,
            "limit": limit,
            "offset": offset,
            "projectId": project_id,
        }
        response = self.__session.get(
            self.__url__("/v1/recording-attachments"),
            params={k: v for k, v in all_params.items() if v is not None},
        )
        json = json_or_raise(response)
        return [_attachment_dict(i) for i in json]

    def download_attachment(
        self,
        *,
        id: str,
        callback: Optional[ProgressCallback] = None,
    ):
        """Download an attachment by ID.

        :param id: the attachment ID.
        :param callback: a callback to track download progress
        :returns: The downloaded attachment bytes.
        """
        response = self.__session.get(
            self.__url__(f"/v1/recording-attachments/{id}/download"),
            stream=True,
            allow_redirects=False,
        )
        if response.is_redirect:
            location = response.headers.get("Location")
            response.close()
            if location is None:
                raise requests.exceptions.HTTPError(
                    "Redirect response missing Location header",
                    response=response,
                )
            return _download_stream_with_progress(location, callback=callback)
        return _download_response_with_progress(response, callback=callback)

    def get_attachment(self, *, id: str):
        """Fetch a single attachment by ID.

        :param id: the attachment ID.
        """
        response = self.__session.get(self.__url__(f"/v1/recording-attachments/{id}"))
        return _attachment_dict(json_or_raise(response))

    def get_topics(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        include_schemas: bool = False,
        project_id: Optional[str] = None,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
        episode_id: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        recording_id: Optional[str] = None,
        recording_key: Optional[str] = None,
        episode_version: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
    ):
        """
        List topics.

        :param device_id: Optionally filter topics by this device ID.
        :param device_name: Optionally filter topics by this device name.
        :param start: Filter topics by this start time.
        :param end: Filter topics by this end time.
        :param include_schemas: Optionally include the schema in the response.
        :param project_id: Optional Project to filter topics by.
        :param session_id: ID of a session to list topics from
        :param session_key: Key of a session to list topics from
        :param episode_id: ID of an episode to list topics from. Its time range is used when
            start and end are omitted.
        :param limit: Maximum topics in this page (server default and maximum: 2000).
        :param offset: Number of topics to skip to retrieve a subsequent page.
        :param recording_id: ID of a recording to list topics from. Topics for a
            recording that hasn't been imported are only returned when no time range,
            device, or session is also given.
        :param recording_key: Key of a recording to list topics from. The same
            condition applies to recordings that haven't been imported.
        :param episode_version: Episode version to list topics from. Requires
            ``episode_id``; the latest version is used when omitted.
        :param sort_by: Optionally sort by "topic" or "version".
        :param sort_order: Optionally specify the sort order, either "asc" or "desc".
        """
        by_recording = recording_id is not None or recording_key is not None
        if episode_version is not None and episode_id is None:
            raise RuntimeError("episode_version requires episode_id")
        if episode_id is not None and (
            device_id is not None
            or device_name is not None
            or session_id is not None
            or session_key is not None
            or by_recording
        ):
            raise RuntimeError("episode_id cannot be combined with another identifier")
        if episode_id is None and not by_recording and (start is None or end is None):
            raise RuntimeError(
                "start and end must be provided unless using episode_id, "
                "recording_id, or recording_key"
            )
        response = self.__session.get(
            self.__url__("/v1/data/topics"),
            params={
                "deviceId": device_id,
                "deviceName": device_name,
                "start": start.astimezone().isoformat() if start else None,
                "end": end.astimezone().isoformat() if end else None,
                "includeSchemas": "true" if include_schemas else "false",
                "projectId": project_id,
                "sessionId": session_id,
                "sessionKey": session_key,
                "episodeId": episode_id,
                "episodeVersion": episode_version,
                "recordingId": recording_id,
                "recordingKey": recording_key,
                "sortBy": sort_by,
                "sortOrder": sort_order,
                "limit": limit,
                "offset": offset,
            },
        )

        json = json_or_raise(response)

        results = []
        for t in json:
            result = {
                "topic": t["topic"],
                "version": t["version"],
                "encoding": t["encoding"],
                "schema_encoding": t["schemaEncoding"],
                "schema_name": t["schemaName"],
            }
            if include_schemas:
                result["schema"] = base64.b64decode(t["schema"])
            results.append(result)
        return results

    def get_custom_properties(self, *, resource_type: Optional[str] = None):
        """
        List custom properties.

        :param resource_type: Optionally filter by resource type: "device", "event",
            "session" or "episode".
        """
        response = self.__session.get(
            self.__url__("/v1/custom-properties"),
            params=without_nulls({"resourceType": resource_type}),
        )
        return [_custom_property_dict(p) for p in json_or_raise(response)]

    def get_custom_property(self, *, custom_property_id: str):
        """
        Gets a single custom property.

        :param custom_property_id: The id of the custom property.
        """
        response = self.__session.get(
            self.__url__(f"/v1/custom-properties/{custom_property_id}")
        )
        return _custom_property_dict(json_or_raise(response))

    def create_custom_property(
        self,
        *,
        key: str,
        label: str,
        resource_type: str,
        value_type: str,
        enum_values: Optional[List[str]] = None,
    ):
        """
        Creates a custom property.

        :param key: Immutable identifier used to refer to the property in queries.
        :param label: Display label.
        :param resource_type: Resource type the property applies to: "device",
            "event", "session" or "episode".
        :param value_type: One of "string", "multiline-string", "number", "enum",
            "boolean" or "multi-enum".
        :param enum_values: Allowed values; required for "enum" and "multi-enum".
        """
        response = self.__session.post(
            self.__url__("/v1/custom-properties"),
            json=without_nulls(
                {
                    "key": key,
                    "label": label,
                    "resourceType": resource_type,
                    "valueType": value_type,
                    "enumValues": enum_values,
                }
            ),
        )
        return _custom_property_dict(json_or_raise(response))

    def update_custom_property(
        self,
        *,
        custom_property_id: str,
        label: Optional[str] = None,
        enum_values: Optional[List[str]] = None,
    ):
        """
        Updates a custom property.

        :param custom_property_id: The id of the custom property.
        :param label: Optional new display label.
        :param enum_values: Optional enum values. Must be a superset of the existing
            values; use this to reorder or add values.
        """
        response = self.__session.patch(
            self.__url__(f"/v1/custom-properties/{custom_property_id}"),
            json=without_nulls({"label": label, "enumValues": enum_values}),
        )
        return _custom_property_dict(json_or_raise(response))

    def delete_custom_property(self, *, custom_property_id: str):
        """
        Deletes a custom property.

        :param custom_property_id: The id of the custom property.
        """
        response = self.__session.delete(
            self.__url__(f"/v1/custom-properties/{custom_property_id}")
        )
        json_or_raise(response)

    def get_projects(self):
        """
        List all available projects in the organization.
        """
        response = self.__session.get(self.__url__("/v1/projects"))
        json = json_or_raise(response)

        return [
            {
                "id": p["id"],
                "name": p.get("name"),
                "org_member_count": p.get("orgMemberCount", 0),
                "last_seen_at": (
                    arrow.get(p["lastSeenAt"]).datetime if p.get("lastSeenAt") else None
                ),
                "is_invite_only": p.get("isInviteOnly"),
            }
            for p in json
        ]

    def create_dataset(
        self,
        *,
        project_id: str,
        name: str,
        description: Optional[str] = None,
        episode_ids: Optional[List[str]] = None,
    ):
        """Create a dataset, optionally seeding its first version with episodes."""
        response = self.__session.post(
            self.__url__("/v1/datasets"),
            json=without_nulls(
                {
                    "projectId": project_id,
                    "name": name,
                    "description": description,
                    "episodeIds": episode_ids,
                }
            ),
        )
        return _dataset_dict(json_or_raise(response))

    def get_datasets(
        self,
        *,
        project_id: Optional[str] = None,
        name: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ):
        """Return a Page of datasets; name is a case-insensitive substring filter.

        Use ``cursor`` to advance pages and ``limit`` to set the size of the page.
        Use ``page.auto_paging_iter()`` to traverse all matching items.
        """
        return self._get_page(
            "/v1/datasets",
            _dataset_dict,
            params=without_nulls(
                {
                    "projectId": project_id,
                    "name": name,
                    "sortBy": camelize(sort_by),
                    "sortOrder": sort_order,
                    "limit": limit,
                    "cursor": cursor,
                }
            ),
        )

    def _get_page(
        self,
        path: str,
        mapper: Callable[[Any], T],
        *,
        params: Dict[str, Any],
        collection: Optional[str] = None,
    ) -> Page[T]:
        response = self.__session.get(self.__url__(path), params=params)
        result = json_or_raise(response)
        items = result[collection] if collection else result

        def fetch_page(cursor: str) -> Page[T]:
            return self._get_page(
                path, mapper, params={**params, "cursor": cursor}, collection=collection
            )

        return Page(
            [mapper(item) for item in items],
            next_cursor=response.headers.get("fg-pagination-next-cursor"),
            previous_cursor=response.headers.get("fg-pagination-previous-cursor"),
            fetch_page=fetch_page,
        )

    def get_dataset(self, *, dataset_id: str):
        """Return dataset metadata and its current episode count."""
        response = self.__session.get(self.__url__(f"/v1/datasets/{dataset_id}"))
        return _dataset_dict(json_or_raise(response))

    def update_dataset(
        self,
        *,
        dataset_id: str,
        name: Optional[str] = None,
        description: Any = _UNSET,
    ):
        """Update a dataset. Pass ``description=None`` to clear its description."""
        params = {}
        if name is not None:
            params["name"] = name
        if description is not _UNSET:
            params["description"] = description
        response = self.__session.patch(
            self.__url__(f"/v1/datasets/{dataset_id}"), json=params
        )
        return _dataset_dict(json_or_raise(response))

    def delete_dataset(self, *, dataset_id: str):
        """Delete a dataset without deleting its episodes."""
        response = self.__session.delete(self.__url__(f"/v1/datasets/{dataset_id}"))
        json_or_raise(response)

    def get_dataset_episodes(
        self,
        *,
        dataset_id: str,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        has_missing_recordings: Optional[bool] = None,
        recording_id: Optional[str] = None,
        properties_query: Optional[str] = None,
        include_recordings: bool = False,
        include_properties: bool = False,
        include_creation_context: bool = False,
    ):
        """Return a Page of the latest committed or initial editable membership.

        Set ``include_creation_context`` to include each episode's ``creation_context``,
        which keeps the API's field names.

        Supply ``start`` and ``end`` together to filter overlapping episode windows.
        Use ``cursor`` to advance pages and ``limit`` to set the size of the page.
        Use ``page.auto_paging_iter()`` to traverse all matching episodes.
        """
        return self._get_dataset_episodes(
            dataset_id=dataset_id,
            version_number=None,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            cursor=cursor,
            start=start,
            end=end,
            has_missing_recordings=has_missing_recordings,
            recording_id=recording_id,
            properties_query=properties_query,
            include_recordings=include_recordings,
            include_properties=include_properties,
            include_creation_context=include_creation_context,
        )

    def update_dataset_episodes(
        self,
        *,
        dataset_id: str,
        add: Optional[List[Union[str, Dict[str, Any]]]] = None,
        remove: Optional[List[str]] = None,
        update: Optional[List[Union[str, Dict[str, Any]]]] = None,
    ):
        """
        Stage episode additions, version updates, and removals in the editable version.

        :param dataset_id: The id of the dataset.
        :param add: Episodes to add. Each item is an episode id, which uses the latest
            episode version, or a dict ``{"id": ..., "version": ...}`` selecting an
            episode version (``version`` is optional).
        :param remove: Episode ids to remove.
        :param update: Existing members whose episode version should change, in the same
            forms as ``add``. Omitting ``version`` selects the latest episode version.
        """
        response = self.__session.patch(
            self.__url__(f"/v1/datasets/{dataset_id}/episodes"),
            json=without_nulls({"add": add, "update": update, "remove": remove}),
        )
        result = json_or_raise(response)
        return {
            "added": result["added"],
            "updated": result["updated"],
            "removed": result["removed"],
            "already_present": result["alreadyPresent"],
        }

    def get_dataset_versions(
        self,
        *,
        dataset_id: str,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ):
        """Return a Page of committed versions and the current editable version.

        Use ``cursor`` to advance pages and ``limit`` to set the size of the page.
        Use ``page.auto_paging_iter()`` to traverse all versions.
        """
        return self._get_page(
            f"/v1/datasets/{dataset_id}/versions",
            _dataset_version_dict,
            collection="versions",
            params=without_nulls(
                {
                    "sortOrder": sort_order,
                    "limit": limit,
                    "cursor": cursor,
                }
            ),
        )

    def get_dataset_version(self, *, dataset_id: str, version_number: int):
        """Return one version, including its recording availability."""
        response = self.__session.get(
            self.__url__(f"/v1/datasets/{dataset_id}/versions/{version_number}")
        )
        return _dataset_version_dict(json_or_raise(response))

    def update_dataset_version(
        self, *, dataset_id: str, version_number: int, description: Optional[str]
    ):
        """
        Update a dataset version's description. Pass ``description=None`` to clear it.

        Works on both the editable version and committed versions; episodes and commit
        attribution are unchanged.
        """
        response = self.__session.patch(
            self.__url__(f"/v1/datasets/{dataset_id}/versions/{version_number}"),
            json={"description": description},
        )
        return _dataset_version_dict(json_or_raise(response))

    def get_dataset_version_episodes(
        self,
        *,
        dataset_id: str,
        version_number: int,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        has_missing_recordings: Optional[bool] = None,
        recording_id: Optional[str] = None,
        properties_query: Optional[str] = None,
        include_recordings: bool = False,
        include_properties: bool = False,
        include_creation_context: bool = False,
    ):
        """Return a Page of episode membership in a specific version.

        Set ``include_creation_context`` to include each episode's ``creation_context``,
        which keeps the API's field names.

        Supply ``start`` and ``end`` together to filter overlapping episode windows.
        Use ``cursor`` to advance pages and ``limit`` to set the size of the page.
        Use ``page.auto_paging_iter()`` to traverse all matching episodes.
        """
        return self._get_dataset_episodes(
            dataset_id=dataset_id,
            version_number=version_number,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            cursor=cursor,
            start=start,
            end=end,
            has_missing_recordings=has_missing_recordings,
            recording_id=recording_id,
            properties_query=properties_query,
            include_recordings=include_recordings,
            include_properties=include_properties,
            include_creation_context=include_creation_context,
        )

    def _get_dataset_episodes(
        self,
        *,
        dataset_id: str,
        version_number: Optional[int],
        sort_by: Optional[str],
        sort_order: Optional[str],
        limit: Optional[int],
        cursor: Optional[str],
        start: Optional[datetime.datetime],
        end: Optional[datetime.datetime],
        has_missing_recordings: Optional[bool],
        recording_id: Optional[str],
        properties_query: Optional[str],
        include_recordings: bool,
        include_properties: bool,
        include_creation_context: bool,
    ):
        _validate_episode_range(start, end)
        path = f"/v1/datasets/{dataset_id}"
        if version_number is not None:
            path += f"/versions/{version_number}"
        path += "/episodes"
        return self._get_page(
            path,
            _dataset_episode_dict,
            collection="episodes",
            params=without_nulls(
                {
                    "sortBy": camelize(sort_by),
                    "sortOrder": sort_order,
                    "limit": limit,
                    "cursor": cursor,
                    "start": start.astimezone().isoformat() if start else None,
                    "end": end.astimezone().isoformat() if end else None,
                    "hasMissingRecordings": (
                        bool_query_param(has_missing_recordings)
                        if has_missing_recordings is not None
                        else None
                    ),
                    "recordingId": recording_id,
                    "propertiesQuery": properties_query,
                    "include": _episode_include(
                        include_recordings, include_properties, include_creation_context
                    ),
                }
            ),
        )

    def compare_dataset_versions(
        self,
        *,
        dataset_id: str,
        version_number: int,
        base_version: int,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        include_recordings: bool = False,
        include_properties: bool = False,
        include_creation_context: bool = False,
    ):
        """Return membership changes from ``base_version`` to ``version_number``.

        Set ``include_creation_context`` to include each episode's ``creation_context``,
        which keeps the API's field names.
        """
        response = self.__session.get(
            self.__url__(
                f"/v1/datasets/{dataset_id}/versions/{version_number}/compare"
            ),
            params=without_nulls(
                {
                    "version": base_version,
                    "limit": limit,
                    "cursor": cursor,
                    "include": _episode_include(
                        include_recordings, include_properties, include_creation_context
                    ),
                }
            ),
        )
        result = json_or_raise(response)
        changes = []
        for change in result["changes"]:
            converted = _dataset_episode_dict(change)
            if "previousEpisode" in change:
                converted["previous_episode"] = _episode_dict(change["previousEpisode"])
            if "previousVersion" in change:
                converted["previous_version"] = change["previousVersion"]
            changes.append(converted)
        return {
            "changes": changes,
            "added_count": result["addedCount"],
            "removed_count": result["removedCount"],
            "changed_count": result["changedCount"],
            "next_cursor": result["nextCursor"],
        }

    def commit_dataset(self, *, dataset_id: str, description: Any = _UNSET):
        """Commit staged changes and open the next editable version.

        ``description`` sets the committed version's description. Pass
        ``description=None`` to clear it; if omitted, the editable version's
        description is kept.
        """
        body = {} if description is _UNSET else {"description": description}
        response = self.__session.post(
            self.__url__(f"/v1/datasets/{dataset_id}/commit"), json=body
        )
        result = json_or_raise(response)
        return {
            "committed": _dataset_version_dict(result["committed"]),
            "editable_version_number": result["editableVersionNumber"],
        }

    def discard_dataset(self, *, dataset_id: str):
        """Discard staged changes from the editable version."""
        response = self.__session.post(
            self.__url__(f"/v1/datasets/{dataset_id}/discard"), json={}
        )
        result = json_or_raise(response)
        return {
            "discarded_adds": result["discardedAdds"],
            "discarded_removes": result["discardedRemoves"],
            "discarded_updates": result["discardedUpdates"],
        }

    def restore_dataset_version(
        self, *, dataset_id: str, version_number: int, force: bool = False
    ):
        """Stage the membership changes needed to restore a previous version."""
        response = self.__session.post(
            self.__url__(
                f"/v1/datasets/{dataset_id}/versions/{version_number}/restore"
            ),
            params={"force": bool_query_param(force)},
            json={},
        )
        result = json_or_raise(response)
        return {
            "added": result["added"],
            "updated": result["updated"],
            "removed": result["removed"],
            "discarded_adds": result["discardedAdds"],
            "discarded_removes": result["discardedRemoves"],
            "discarded_updates": result["discardedUpdates"],
        }

    def clone_dataset_version(
        self,
        *,
        dataset_id: str,
        version_number: int,
        name: str,
        description: Optional[str] = None,
        include_history: Optional[bool] = None,
    ):
        """Create a new dataset from the episodes of a committed version.

        The new dataset is independent of the source. By default it has a single
        committed version; ``include_history=True`` copies every version up to and
        including the cloned one. Either way, it has an empty editable version on top.
        """
        response = self.__session.post(
            self.__url__(f"/v1/datasets/{dataset_id}/versions/{version_number}/clone"),
            json=without_nulls(
                {
                    "name": name,
                    "description": description,
                    "includeHistory": include_history,
                }
            ),
        )
        return _dataset_dict(json_or_raise(response))

    def create_episodes(self, *, project_id: str, episodes: List[Dict[str, Any]]):
        """Create episodes, reusing any whose latest version has identical membership, bounds,
        and custom properties.

        :param project_id: Project in which to create the episodes.
        :param episodes: Episode definitions. Each dictionary requires exactly one of
            ``recordings`` or ``event_id``, and may include ``properties``.
            Recording inputs may also include ``start_time`` and ``end_time``. An ``event_id``
            input snapshots the event's current time range and the completed recordings from
            its device that overlap it; later edits to the event or new imports do not change
            the episode. ``properties`` keys must be episode custom properties defined for your
            organization, with values of the matching type; multi-enum values are lists.
        """
        serialized = []
        for index, episode in enumerate(episodes):
            event_id = episode.get("event_id")
            recordings = episode.get("recordings")
            start_time = episode.get("start_time")
            end_time = episode.get("end_time")
            if (event_id is None) == (recordings is None):
                raise ValueError(
                    f"episodes[{index}]: provide exactly one of recordings or event_id"
                )
            if event_id is not None and (
                start_time is not None or end_time is not None
            ):
                raise ValueError(
                    f"episodes[{index}]: event_id cannot be combined with "
                    "start_time or end_time"
                )
            serialized.append(
                without_nulls(
                    {
                        "eventId": event_id,
                        "recordings": recordings,
                        "startTime": (
                            start_time.astimezone().isoformat() if start_time else None
                        ),
                        "endTime": (
                            end_time.astimezone().isoformat() if end_time else None
                        ),
                        "properties": episode.get("properties"),
                    }
                )
            )
        response = self.__session.post(
            self.__url__("/v1/episodes"),
            json={"projectId": project_id, "episodes": serialized},
        )
        return json_or_raise(response)["episodes"]

    def update_episodes(self, *, episodes: List[Dict[str, Any]]):
        """Update episodes in one transaction and return each episode's resulting version.

        If any update fails, no updates are applied. An unchanged episode keeps its version.

        :param episodes: Episode updates. Each dictionary requires ``id`` and may include
            ``applies_to_version``, ``start_time``, ``end_time``, ``recordings``, and
            ``properties``. ``applies_to_version`` rejects the update if the episode's latest
            version differs. ``recordings`` is a dictionary with optional ``add`` and
            ``remove`` lists of recording IDs. ``properties`` keys not included in the
            update keep their values; to unset a property, pass None as the value.
        """
        serialized = [
            without_nulls(
                {
                    "id": episode["id"],
                    "appliesToVersion": episode.get("applies_to_version"),
                    "startTime": (
                        episode["start_time"].astimezone().isoformat()
                        if episode.get("start_time")
                        else None
                    ),
                    "endTime": (
                        episode["end_time"].astimezone().isoformat()
                        if episode.get("end_time")
                        else None
                    ),
                    "recordings": episode.get("recordings"),
                    "properties": episode.get("properties"),
                }
            )
            for episode in episodes
        ]
        response = self.__session.patch(
            self.__url__("/v1/episodes"),
            json={"episodes": serialized},
        )
        return json_or_raise(response)["episodes"]

    def get_episodes(
        self,
        *,
        project_id: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        has_missing_recordings: Optional[bool] = None,
        recording_id: Optional[str] = None,
        exact_recording_ids: Optional[List[str]] = None,
        properties_query: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        include_recordings: bool = False,
        include_properties: bool = False,
        include_creation_context: bool = False,
    ):
        """Return a Page of episodes, optionally with recording details and custom properties.

        Set ``include_creation_context`` to include each episode's ``creation_context``,
        which keeps the API's field names.

        Supply ``start`` and ``end`` together to filter overlapping episode windows.
        Use ``exact_recording_ids`` to match episodes whose recordings are exactly
        these recording IDs. Pass None to leave the recording set unrestricted;
        an empty list raises ValueError.
        Use ``properties_query`` to filter by custom property values.
        Use ``cursor`` to advance pages and ``limit`` to set the size of the page.
        Use ``page.auto_paging_iter()`` to traverse all matching episodes.
        """
        _validate_episode_range(start, end)
        if exact_recording_ids is not None and not exact_recording_ids:
            raise ValueError(
                "exact_recording_ids must contain at least one recording ID"
            )
        return self._get_page(
            "/v1/episodes",
            _episode_dict,
            collection="episodes",
            params=without_nulls(
                {
                    "projectId": project_id,
                    "start": start.astimezone().isoformat() if start else None,
                    "end": end.astimezone().isoformat() if end else None,
                    "hasMissingRecordings": (
                        bool_query_param(has_missing_recordings)
                        if has_missing_recordings is not None
                        else None
                    ),
                    "recordingId": recording_id,
                    "exactRecordingIds": (
                        ",".join(exact_recording_ids)
                        if exact_recording_ids is not None
                        else None
                    ),
                    "propertiesQuery": properties_query,
                    "sortBy": camelize(sort_by),
                    "sortOrder": sort_order,
                    "limit": limit,
                    "cursor": cursor,
                    "include": _episode_include(
                        include_recordings, include_properties, include_creation_context
                    ),
                }
            ),
        )

    def get_episode(
        self,
        *,
        episode_id: str,
        include_recordings: bool = False,
        include_properties: bool = False,
        version: Optional[int] = None,
    ):
        """Return an episode, optionally with recording details and custom properties.

        Pass ``version`` to read a specific episode version instead of the latest.
        The result always includes ``creation_context``, which keeps the API's field names.
        """
        response = self.__session.get(
            self.__url__(f"/v1/episodes/{episode_id}"),
            params=without_nulls(
                {
                    "version": version,
                    "include": _episode_include(include_recordings, include_properties),
                }
            ),
        )
        return _episode_dict(json_or_raise(response))

    def delete_episode(self, *, episode_id: str):
        """Delete an episode if it does not belong to a dataset."""
        response = self.__session.delete(self.__url__(f"/v1/episodes/{episode_id}"))
        json_or_raise(response)

    def get_episode_versions(
        self,
        *,
        episode_id: str,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ):
        """Return a Page of an episode's versions, newest first.

        Each version lists its author and the fields it changed.
        Use ``cursor`` to advance pages and ``limit`` to set the size of the page.
        Use ``page.auto_paging_iter()`` to traverse all versions.
        """
        return self._get_page(
            f"/v1/episodes/{episode_id}/versions",
            _episode_version_dict,
            collection="versions",
            params=without_nulls({"limit": limit, "cursor": cursor}),
        )

    def update_episode(
        self,
        *,
        episode_id: str,
        applies_to_version: Optional[int] = None,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        recordings: Optional[Dict[str, List[str]]] = None,
        properties: Optional[Dict[str, Any]] = None,
    ):
        """Update an episode and return it at the resulting version.

        An unchanged episode keeps its version.

        :param episode_id: The id of the episode.
        :param applies_to_version: Reject the update if the episode's latest version
            differs.
        :param start_time: Optional new start of the episode window.
        :param end_time: Optional new end of the episode window.
        :param recordings: Optional dictionary with ``add`` and ``remove`` lists of
            recording IDs.
        :param properties: Optional custom properties to merge. Keys not included keep
            their values; to unset a property, pass None as the value.
        """
        response = self.__session.patch(
            self.__url__(f"/v1/episodes/{episode_id}"),
            json=without_nulls(
                {
                    "appliesToVersion": applies_to_version,
                    "startTime": (
                        start_time.astimezone().isoformat() if start_time else None
                    ),
                    "endTime": end_time.astimezone().isoformat() if end_time else None,
                    "recordings": recordings,
                    "properties": properties,
                }
            ),
        )
        return _episode_dict(json_or_raise(response))

    def download_dataset(
        self,
        *,
        dataset_id: str,
        version_number: int,
        output_directory: Union[str, os.PathLike],
        topics: Optional[List[str]] = None,
        include_properties: bool = False,
    ) -> Path:
        """Export a committed version to MCAP files (including attachments) and a manifest.

        The manifest uses the app's schema, but ``file`` paths
        are relative to the returned output directory, which contains manifest.json.
        App ZIP exports instead use paths relative to the ZIP root.

        ``include_properties=True`` adds episode custom properties to the manifest
        and requires access to episode properties. Properties are omitted by default.

        ``topics`` selects topics; None or [] downloads all topics. A version with
        any episode whose recordings are no longer available can't be downloaded;
        this raises before writing anything. Request failures before streaming are
        recorded in the manifest as failed and the export continues; if every episode
        fails, this raises after writing the manifest.

        Exports with failed episodes emit ``DatasetDownloadWarning`` after writing the
        manifest. The return value remains a Path. Promoting this warning to an error
        does not undo files.

        Interrupted transfers or filesystem failures stop the export and preserve
        completed MCAPs and the failed episode's .part file. The manifest is written
        if possible before re-raising. Its entries may then be incomplete, while
        selection.episodeCount still describes the selected version. Retrying requires
        a new or empty directory. .part files must not be treated as complete MCAPs.
        """
        version = self.get_dataset_version(
            dataset_id=dataset_id, version_number=version_number
        )
        if version["committed_at"] is None:
            raise RuntimeError("Cannot download an editable dataset version")
        destination = Path(output_directory)
        if destination.exists():
            if not destination.is_dir():
                raise RuntimeError("output_directory must be a directory")
            if any(destination.iterdir()):
                raise RuntimeError("output_directory must be empty")
        dataset = self.get_dataset(dataset_id=dataset_id)
        # Capture the complete selection before transfers, so incomplete versions are
        # refused before anything is written. MCAP bytes stay streamed.
        episodes = list(
            self.get_dataset_version_episodes(
                dataset_id=dataset_id,
                version_number=version_number,
                sort_by="start_time",
                sort_order="asc",
                limit=2000,
                include_properties=include_properties,
            ).auto_paging_iter()
        )
        incomplete = sum(member["has_missing_recordings"] for member in episodes)
        if incomplete:
            raise RuntimeError(
                f"Version {version_number} can't be downloaded: {incomplete} "
                f"{'episode has' if incomplete == 1 else 'episodes have'} recordings "
                "that are no longer available. Download another version, or remove "
                "the affected episodes from the dataset and commit a new version."
            )
        destination.mkdir(parents=True, exist_ok=True)
        selected_topics = list(topics) if topics else None
        return export_dataset(
            dataset=dataset,
            version=version,
            episodes=episodes,
            destination=destination,
            topics=selected_topics,
            download=lambda episode_id, episode_version, output_path: (
                self._download_episode_to_file(
                    episode_id=episode_id,
                    episode_version=episode_version,
                    output_path=output_path,
                    topics=selected_topics,
                )
            ),
        )

    def _download_episode_to_file(
        self,
        *,
        episode_id: str,
        output_path: Path,
        episode_version: Optional[int] = None,
        topics: Optional[List[str]] = None,
    ) -> int:
        return download_episode(
            output_path,
            lambda: self._make_stream_link(
                episode_id=episode_id,
                episode_version=episode_version,
                topics=topics,
                include_attachments=True,
            ),
        )

    def get_lake_files(
        self,
        *,
        site_id: str,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        recording_id: Optional[str] = None,
        recording_key: Optional[str] = None,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        topic: Optional[str] = None,
    ):
        """
        Lists the MCAP files in the lake bucket of a self-managed Primary Site.
        Each imported recording produces one file per topic.

        The query must be limited to a device or recording. Querying by device requires
        `start` and `end` (at most 24 hours apart) unless a recording or session is also given.

        :param site_id: The ID of the self-managed Primary Site.
        :param device_id: Optionally filter files by device ID.
        :param device_name: Optionally filter files by device name.
        :param recording_id: Optionally filter files by recording ID.
        :param recording_key: Optionally filter files by recording key.
        :param session_id: Optionally filter files to recordings in the session with this ID.
        :param session_key: Optionally filter files to recordings in the session with this key.
        :param start: Optionally specify the inclusive start of a recording time range.
        :param end: Optionally specify the inclusive end of a recording time range.
        :param topic: Optionally include only files for this topic.
        """
        response = self.__session.get(
            self.__url__("/v1/lake-files"),
            params=without_nulls(
                {
                    "siteId": site_id,
                    "deviceId": device_id,
                    "deviceName": device_name,
                    "recordingId": recording_id,
                    "recordingKey": recording_key,
                    "sessionId": session_id,
                    "sessionKey": session_key,
                    "start": start.astimezone().isoformat() if start else None,
                    "end": end.astimezone().isoformat() if end else None,
                    "topic": topic,
                }
            ),
        )
        return [_lake_file_dict(lake_file) for lake_file in json_or_raise(response)]

    def upload_data(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        key: Optional[str] = None,
        filename: str,
        data: Union[bytes, IO[Any]],
        callback: Optional[SizeProgressCallback] = None,
        project_id: Optional[str] = None,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
    ):
        """
        Uploads data in bytes.

        device_id: Device id of the device from which this data originated.
        device_name: Name id of the device from which this data originated.
        key: an optional string key to associate with the recording. Any subsequent upload
          with the same key will be de-duplicated with this recording.
        filename: A filename to associate with the data. The data format will be
            inferred from the file extension.
        data: The raw data in .bag or .mcap format.
        callback: An optional callback to report progress on the upload.
        project_id: Optional Project to upload data to. Required for multi-project
            organizations if an existing device is not specified.
        session_id: ID of an existing session to associate the upload with.
        session_key: Key of a session to associate the upload with. If no session exists
            with this key, a new session will be created using the provided device.
        """
        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "filename": filename,
            "key": key,
            "projectId": project_id,
            "sessionId": session_id,
            "sessionKey": session_key,
        }
        link_response = self.__session.post(
            self.__url__("/v1/data/upload"),
            json={k: v for k, v in params.items() if v is not None},
        )

        json = json_or_raise(link_response)

        link = json["link"]
        buffer = ProgressBufferReader(data, callback=callback)
        upload_request = requests.put(
            link,
            data=buffer,
            headers={"Content-Type": "application/octet-stream"},
        )
        return {
            "link": link,
            "request_id": json.get("requestId"),
            "text": upload_request.text,
            "code": upload_request.status_code,
        }

    def get_sessions(
        self,
        *,
        project_id: Optional[str] = None,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        key_matches: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        query: Optional[str] = None,
    ):
        """Fetches sessions.

        project_id: Optionally filter sessions by this project ID.
        device_id: Optionally filter sessions by this device ID.
        device_name: Optionally filter sessions by this device name.
        key_matches: Optionally filter sessions by partially matching on this key.
        start: Optionally specify the start of an inclusive time range.
            Only sessions with messages within this time range will be returned.
        end: Optionally specify the end of an inclusive time range.
            Only sessions with messages within this time range will be returned.
        sort_by: Optionally sort returned sessions by a field in the response type.
            Must be one of "id", "created_at", "updated_at".
        sort_order: Optionally specify the sort order, either "asc" or "desc".
        limit: Optionally limit the number of records returned.
        offset: Optionally offset the results by this many records.
        query: Optionally filter sessions on custom properties using a space-separated
            query string (e.g. "key:value").
        """

        all_params = {
            "projectId": project_id,
            "deviceId": device_id,
            "deviceName": device_name,
            "keyMatches": key_matches,
            "start": start.astimezone().isoformat() if start else None,
            "end": end.astimezone().isoformat() if end else None,
            "sortBy": camelize(sort_by),
            "sortOrder": sort_order,
            "limit": limit,
            "offset": offset,
            "query": query,
        }
        response = self.__session.get(
            self.__url__("/v1/sessions"),
            params={k: v for k, v in all_params.items() if v is not None},
        )
        json = json_or_raise(response)

        return [_session_dict(s) for s in json]

    def get_session(
        self,
        *,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
        project_id: str,
    ):
        """Fetches a single session.

        session_id: The ID of the session to fetch
        session_key: The key of the session to fetch
        project_id: The project ID to fetch the session from.
        """
        identifier = _session_identifier_for_path(session_id, session_key)

        response = self.__session.get(
            self.__url__(f"/v1/sessions/{identifier}"),
            params={"projectId": project_id},
        )
        return _session_dict(json_or_raise(response))

    def create_session(
        self,
        *,
        device_id: Optional[str] = None,
        key: Optional[str] = None,
        recording_ids: Optional[List[str]] = None,
        properties: Optional[Dict[str, Union[str, bool, float, int]]] = None,
    ):
        """Creates a new session.

        device_id: The ID of the device to associate with the session.
            If omitted, inferred from recording_ids.
        key: An optional user-supplied identifier, unique within the project.
        recording_ids: IDs of recordings to associate with the new session.
            All recordings must belong to the same device.
        properties: Optional custom properties for the session.
            Each key must be defined as a custom property for your organization,
            and each value must be of the appropriate type.
        """

        if device_id is None and recording_ids is None:
            raise RuntimeError("device_id or recording_ids must be provided")

        params = {
            "deviceId": device_id,
            "key": key,
            "recordingIds": recording_ids,
            "properties": properties,
        }
        response = self.__session.post(
            self.__url__("/v1/sessions"),
            json={k: v for k, v in params.items() if v is not None},
        )

        return _session_dict(json_or_raise(response))

    def update_session(
        self,
        *,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
        project_id: str,
        new_key: Optional[str] = None,
        add_recording_ids: Optional[List[str]] = None,
        remove_recording_ids: Optional[List[str]] = None,
        properties: Optional[Dict[str, Union[str, bool, float, int]]] = None,
    ):
        """Updates a session.

        session_id: The ID of the session to update.
        session_key: The current key of the session to update.
        project_id: The Project ID to which the session belongs.
        new_key: Optional new user-supplied identifier, unique within the project.
        add_recording_ids: IDs of recordings to add to the session.
        remove_recording_ids: IDs of recordings to remove from the session.
        properties: Optional custom properties to add to or edit on the session.
            Each key must be defined as a custom property for your organization,
            and each value must be of the appropriate type.
        """
        identifier = _session_identifier_for_path(session_id, session_key)

        params = {
            "key": new_key,
            "addRecordingIds": add_recording_ids,
            "removeRecordingIds": remove_recording_ids,
            "properties": properties,
        }
        response = self.__session.patch(
            self.__url__(f"/v1/sessions/{identifier}"),
            params={"projectId": project_id},
            json={k: v for k, v in params.items() if v is not None},
        )

        return _session_dict(json_or_raise(response))

    def delete_session(
        self,
        *,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
        project_id: str,
    ):
        """Deletes a session.

        session_id: The ID of the session to delete.
        session_key: The key of the session to delete.
        project_id: The Project ID to which the session belongs.
        """
        identifier = _session_identifier_for_path(session_id, session_key)

        response = self.__session.delete(
            self.__url__(f"/v1/sessions/{identifier}"),
            params={"projectId": project_id},
        )

        return json_or_raise(response)

    def get_layouts(
        self,
        *,
        updated_since: Optional[datetime.datetime] = None,
        include_data: bool = True,
    ):
        """
        Returns a list of layouts shared with the organization.

        :param updated_since: Optionally return only layouts updated since this time.
        :param include_data: Whether to include each layout's ``data``.
        """
        response = self.__session.get(
            self.__url__("/v1/layouts"),
            params=without_nulls(
                {
                    "updatedSince": (
                        updated_since.astimezone().isoformat()
                        if updated_since
                        else None
                    ),
                    "includeData": bool_query_param(include_data),
                }
            ),
        )
        return [_layout_dict(layout) for layout in json_or_raise(response)]

    def get_layout(self, *, layout_id: str, include_data: bool = False):
        """
        Gets a single layout.

        :param layout_id: The id of the layout.
        :param include_data: Whether to include the layout's ``data``.
        """
        response = self.__session.get(
            self.__url__(f"/v1/layouts/{layout_id}"),
            params={"includeData": bool_query_param(include_data)},
        )
        return _layout_dict(json_or_raise(response))

    def create_layout(
        self,
        *,
        name: str,
        permission: str,
        data: Dict[str, Any],
        layout_id: Optional[str] = None,
        saved_at: Optional[datetime.datetime] = None,
        folder_name: Optional[str] = None,
    ):
        """
        Creates a layout. Organization API keys must use "ORG_WRITE" permission.
        Personal access tokens may use any of the listed permissions.

        :param name: The name of the layout.
        :param permission: "CREATOR_WRITE", "ORG_READ", or "ORG_WRITE".
        :param data: The layout contents. Treat this as opaque and round-trip
            values returned by the API.
        :param layout_id: Optional id for the new layout.
        :param saved_at: Optional time the layout was last saved locally.
        :param folder_name: Optional folder name used to organize layouts.
            Cannot contain forward slashes.
        """
        response = self.__session.post(
            self.__url__("/v1/layouts"),
            json=without_nulls(
                {
                    "id": layout_id,
                    "name": name,
                    "savedAt": saved_at.astimezone().isoformat() if saved_at else None,
                    "folderName": folder_name,
                    "permission": permission,
                    "data": data,
                }
            ),
        )
        return _layout_dict(json_or_raise(response))

    def update_layout(
        self,
        *,
        layout_id: str,
        name: Optional[str] = None,
        folder_name: Optional[str] = None,
        permission: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
    ):
        """
        Updates a layout. At least one field must be set.
        Organization API keys can only update "ORG_WRITE" layouts and may only
        set permission to "ORG_WRITE". Personal access tokens follow the owner's
        layout permissions.

        :param layout_id: The id of the layout.
        :param name: Optional new name.
        :param folder_name: Optional new folder name. An empty string removes the
            layout from its folder.
        :param permission: Optional new permission: "CREATOR_WRITE", "ORG_READ",
            or "ORG_WRITE".
        :param data: Optional new layout contents.
        """
        response = self.__session.patch(
            self.__url__(f"/v1/layouts/{layout_id}"),
            json=without_nulls(
                {
                    "name": name,
                    "folderName": folder_name,
                    "permission": permission,
                    "data": data,
                }
            ),
        )
        return _layout_dict(json_or_raise(response))

    def delete_layout(self, *, layout_id: str):
        """
        Soft-deletes a layout. It can be restored with `restore_layout`.

        :param layout_id: The id of the layout.
        """
        response = self.__session.delete(self.__url__(f"/v1/layouts/{layout_id}"))
        json_or_raise(response)

    def restore_layout(self, *, layout_id: str):
        """
        Restores a soft-deleted layout.

        :param layout_id: The id of the layout.
        """
        response = self.__session.post(self.__url__(f"/v1/layouts/{layout_id}/restore"))
        json_or_raise(response)

    def get_site_inbox_notification_tokens(self, *, site_id: Optional[str] = None):
        """
        Returns a list of site inbox notification tokens.

        :param site_id: Optional site to filter tokens by.
        """
        response = self.__session.get(
            self.__url__("/v1/site-inbox-notification-tokens"),
            params=without_nulls({"siteId": site_id}),
        )
        return [
            _site_inbox_notification_token_dict(token)
            for token in json_or_raise(response)
        ]

    def create_site_inbox_notification_token(self, *, site_id: str):
        """
        Creates a token that authenticates bucket notifications for a Primary Site.
        The returned ``token`` is only available on creation.

        :param site_id: The site to associate with the token.
        """
        response = self.__session.post(
            self.__url__("/v1/site-inbox-notification-tokens"),
            json={"siteId": site_id},
        )
        return _site_inbox_notification_token_dict(json_or_raise(response))

    def delete_site_inbox_notification_token(
        self, *, site_inbox_notification_token_id: str
    ):
        """
        Deletes a site inbox notification token.

        :param site_inbox_notification_token_id: The id of the token.
        """
        response = self.__session.delete(
            self.__url__(
                f"/v1/site-inbox-notification-tokens/{site_inbox_notification_token_id}"
            )
        )
        json_or_raise(response)

    def get_device_custom_property_time_interval(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        project_id: Optional[str] = None,
        id: str,
    ):
        """
        Fetches a single device custom property time interval record for the given device.

        device_id: The ID of the device to retrieve the time interval record from.
            Use this or device_name.
        device_name: The name of the device to retrieve the time interval record from.
            Use this or device_id.
        project_id: Project associated with the device. Required for multi-project organizations.
        id: The ID of the time interval record to fetch.
        """
        identifier = _device_identifier_for_path(device_id, device_name)

        response = self.__session.get(
            self.__url__(
                f"/v1/devices/{identifier}/property-time-intervals/"
                f"{urlquote(id, safe='')}"
            ),
            params=without_nulls({"projectId": project_id}),
        )
        return _device_custom_property_time_interval_dict(json_or_raise(response))

    def get_device_custom_property_time_intervals(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        project_id: Optional[str] = None,
        query: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ):
        """
        Lists device custom property time intervals.

        device_id: The ID of the device to retrieve time intervals from.
            Use this or device_name.
        device_name: The name of the device to retrieve time intervals from.
            Use this or device_id.
        project_id: Project associated with the device. Required for multi-project organizations.
        query: optional query string to filter property time intervals by metadata.
            See https://docs.foxglove.dev/api#tag/Devices/paths/~1devices/get for
            a syntax definition of `query`.
        start: Optionally include intervals active at or after this time.
        end: Optionally include intervals active before this time.
        sort_by: Optionally sort records by this field name.
        sort_order: Optionally specify the sort order, either "asc" or "desc".
        limit: Optionally limit the number of time intervals returned.
        offset: Optionally offset the time intervals by this many intervals.
        """
        identifier = _device_identifier_for_path(device_id, device_name)

        params = {
            "projectId": project_id,
            "query": query,
            "start": start.astimezone().isoformat() if start else None,
            "end": end.astimezone().isoformat() if end else None,
            "sortBy": camelize(sort_by),
            "sortOrder": sort_order,
            "limit": limit,
            "offset": offset,
        }

        response = self.__session.get(
            self.__url__(f"/v1/devices/{identifier}/property-time-intervals"),
            params={k: v for k, v in params.items() if v is not None},
        )
        return [
            _device_custom_property_time_interval_dict(r)
            for r in json_or_raise(response)
        ]

    def update_device_custom_property_time_interval(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        project_id: Optional[str] = None,
        key: str,
        value: Optional[Union[str, bool, float, int, List[str]]] = None,
        start: datetime.datetime,
        end: datetime.datetime,
    ):
        """
        Updates device custom property time intervals over a time range for the given device.

        The request is treated as an assertion of truth for the given range,
        so existing intervals may be split, trimmed, or deleted as needed.

        device_id: The ID of the device to update time intervals for.
            Use this or device_name.
        device_name: The name of the device to update time intervals for.
            Use this or device_id.
        project_id: Project associated with the device. Required for multi-project organizations.
        key: The property key to update.
        value: The value to apply over the given time range.
            When omitted, the value will be treated as explicitly unset for that range.
        start: Inclusive start of the property's effective time range.
        end: Exclusive end of the property's effective time range.
        """
        identifier = _device_identifier_for_path(device_id, device_name)

        params: Dict[str, Any] = {
            "projectId": project_id,
            "key": key,
            "value": value,
            "start": start.astimezone().isoformat(),
            "end": end.astimezone().isoformat(),
        }

        response = self.__session.post(
            self.__url__(
                f"/v1/actions/devices/{identifier}/update-property-time-interval"
            ),
            json=without_nulls(params),
        )
        # This endpoint returns 204 No Content on success (no body)
        if response.status_code == 204:
            return None
        return json_or_raise(response)

    def get_extensions(self):
        """Return all extensions published to the organization."""
        response = self.__session.get(self.__url__("/v1/extensions"))
        return [_extension_dict(extension) for extension in json_or_raise(response)]

    def get_extension(self, *, extension_id: str):
        """
        Gets a single extension, including a signed ``foxe`` link to download its
        active version.

        :param extension_id: The id of the extension.
        """
        response = self.__session.get(self.__url__(f"/v1/extensions/{extension_id}"))
        return _extension_dict(json_or_raise(response))

    def publish_extension(
        self,
        *,
        data: Union[bytes, IO[Any]],
        callback: Optional[SizeProgressCallback] = None,
    ):
        """
        Publishes a new extension, or a new version of an existing extension, to the
        organization. Returns a dict with the extension's ``id``.

        The extension's name, publisher, display name, and version are read from the
        package.json inside the package.

        :param data: The contents of the extension package (.foxe file).
        :param callback: An optional callback to report progress on the upload.
        """
        response = self.__session.post(
            self.__url__("/v1/extension-upload"),
            data=ProgressBufferReader(data, callback=callback),
            headers={"Content-Type": "application/octet-stream"},
        )
        return {"id": json_or_raise(response)["id"]}

    def delete_extension(self, *, extension_id: str):
        """
        Deletes an extension, unpublishing it from the organization.

        :param extension_id: The id of the extension.
        """
        response = self.__session.delete(self.__url__(f"/v1/extensions/{extension_id}"))
        json_or_raise(response)

    def get_automations(
        self,
        *,
        project_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ):
        """List a project's automations, newest first.

        Each automation has the fields returned by ``get_automation`` plus its
        lifetime run count, whether its trigger is enabled (``None`` without a
        trigger), and the time and status of its latest run (``None`` before its
        first run).

        Automations are in beta. Personal access tokens and organization API keys
        can list automations.

        :param project_id: The project to list automations from.
        :param limit: Optionally limit the number of automations returned.
        :param offset: Optionally skip this many automations.
        """
        response = self.__session.get(
            self.__url__("/v1/automations"),
            params=without_nulls(
                {"projectId": project_id, "limit": limit, "offset": offset}
            ),
        )
        return [
            _automation_list_item_dict(automation)
            for automation in json_or_raise(response)["automations"]
        ]

    def get_automation(self, *, automation_id: str):
        """Get an automation's saved definition.

        Automations are in beta. ``permissions`` is passed through unchanged with
        the API's field names (``canEdit``, ``canRun``, ``canDelete``).

        :param automation_id: The ID of the automation.
        """
        response = self.__session.get(
            self.__url__(f"/v1/automations/{urlquote(automation_id, safe='')}")
        )
        return _automation_dict(json_or_raise(response))

    def create_automation(self, *, project_id: str, name: str, action_prompt: str):
        """Create an automation and return it as ``get_automation`` does.

        Automations are in beta. Use a personal access token: the automation acts
        as its owner, with the owner's access to data in the project. Organization
        API keys can't create automations.

        :param project_id: The project the automation belongs to.
        :param name: A name for the automation.
        :param action_prompt: The saved instructions the automation runs with.
        """
        response = self.__session.post(
            self.__url__("/v1/automations"),
            json={"projectId": project_id, "name": name, "actionPrompt": action_prompt},
        )
        return _automation_dict(json_or_raise(response))

    def update_automation(
        self,
        *,
        automation_id: str,
        name: Optional[str] = None,
        action_prompt: Optional[str] = None,
    ):
        """Edit an automation's name or saved instructions.

        Automations are in beta. Use a personal access token. Existing runs keep
        the instructions they ran with.

        :param automation_id: The ID of the automation.
        :param name: Optionally a new name.
        :param action_prompt: Optionally new saved instructions.
        """
        if name is None and action_prompt is None:
            raise ValueError("name or action_prompt must be provided")
        response = self.__session.patch(
            self.__url__(f"/v1/automations/{urlquote(automation_id, safe='')}"),
            json=without_nulls({"name": name, "actionPrompt": action_prompt}),
        )
        return _automation_dict(json_or_raise(response))

    def delete_automation(self, *, automation_id: str):
        """Delete an automation and its trigger.

        Automations are in beta. Use a personal access token. Its runs are no
        longer listed; runs in progress finish, and runs that have not started are
        rejected.

        :param automation_id: The ID of the automation.
        """
        response = self.__session.delete(
            self.__url__(f"/v1/automations/{urlquote(automation_id, safe='')}")
        )
        json_or_raise(response)


def _session_identifier(session_id: Optional[str], session_key: Optional[str]) -> str:
    if session_id is not None and session_key is not None:
        raise RuntimeError("session_id and session_key are mutually exclusive")
    if session_id is None and session_key is None:
        raise RuntimeError("session_id or session_key must be provided")

    identifier = session_id if session_id is not None else session_key
    assert identifier is not None, "one of session_id or session_key must be provided"
    return identifier


def _recording_identifier_for_path(
    recording_id: Optional[str], recording_key: Optional[str]
) -> str:
    if recording_id is not None and recording_key is not None:
        raise RuntimeError("recording_id and recording_key are mutually exclusive")
    if recording_id is None and recording_key is None:
        raise RuntimeError("recording_id or recording_key must be provided")
    identifier = recording_id if recording_id is not None else recording_key
    assert identifier is not None
    return urlquote(identifier, safe="")


def _recording_dict(recording):
    imported_at = recording.get("importedAt")
    return {
        "id": recording["id"],
        "path": recording["path"],
        "size": recording["size"],
        "message_count": recording.get("messageCount"),
        "created_at": arrow.get(recording["createdAt"]).datetime,
        "imported_at": arrow.get(imported_at).datetime if imported_at else None,
        "start": arrow.get(recording["start"]).datetime,
        "end": arrow.get(recording["end"]).datetime,
        "import_status": recording["importStatus"],
        "site": recording.get("site"),
        "edge_site": recording.get("edgeSite"),
        "device": recording.get("device"),
        "metadata": recording.get("metadata"),
        "key": recording.get("key"),
        "project_id": recording.get("projectId"),
        "session_id": recording.get("sessionId"),
        "session_key": recording.get("sessionKey"),
        "location": recording.get("location"),
    }


def _pending_import_dict(pending_import):
    quarantined_at = pending_import.get("quarantinedAt")
    return {
        "request_id": pending_import["requestId"],
        "import_id": pending_import.get("importId"),
        "org_id": pending_import["orgId"],
        "project_id": pending_import.get("projectId"),
        "site_id": pending_import["siteId"],
        "device_id": pending_import.get("deviceId"),
        "device_name": pending_import.get("deviceName"),
        "filename": pending_import["filename"],
        "pipeline_stage": pending_import["pipelineStage"],
        "status": pending_import.get("status"),
        "error": pending_import.get("error"),
        "created_at": arrow.get(pending_import["createdAt"]).datetime,
        "updated_at": arrow.get(pending_import["updatedAt"]).datetime,
        "quarantined_at": (
            arrow.get(quarantined_at).datetime if quarantined_at else None
        ),
    }


def _session_identifier_for_path(
    session_id: Optional[str], session_key: Optional[str]
) -> str:
    return urlquote(_session_identifier(session_id, session_key), safe="")


def _attachment_dict(attachment):
    return {
        "id": attachment["id"],
        "recording_id": attachment["recordingId"],
        "site_id": attachment["siteId"],
        "name": attachment["name"],
        "media_type": attachment["mediaType"],
        "size": attachment["size"],
        "crc": attachment["crc"],
        "fingerprint": attachment["fingerprint"],
        "log_time": arrow.get(attachment["logTime"]).datetime,
        "create_time": arrow.get(attachment["createTime"]).datetime,
        "data_offset": attachment.get("dataOffset"),
        "lake_path": attachment.get("lakePath"),
    }


def _device_identifier(device_id: Optional[str], device_name: Optional[str]) -> str:
    if device_id is not None and device_name is not None:
        raise RuntimeError("device_id and device_name are mutually exclusive")
    if device_id is None and device_name is None:
        raise RuntimeError("device_id or device_name must be provided")

    identifier = device_id if device_id is not None else device_name
    assert identifier is not None, "one of device_id or device_name must be provided"
    return identifier


def _custom_property_dict(custom_property):
    return {
        "id": custom_property["id"],
        "key": custom_property["key"],
        "label": custom_property["label"],
        "resource_type": custom_property["resourceType"],
        "value_type": custom_property["valueType"],
        "enum_values": custom_property.get("enumValues"),
        "has_associated_data": custom_property.get("hasAssociatedData"),
    }


def _device_identifier_for_path(
    device_id: Optional[str], device_name: Optional[str]
) -> str:
    return urlquote(_device_identifier(device_id, device_name), safe="")


def _lake_file_dict(lake_file):
    return {
        "path": lake_file["path"],
        "topics": lake_file["topics"],
    }


def _event_dict(json_event):
    return {
        "id": json_event["id"],
        "device_id": json_event.get("deviceId"),
        "device": json_event.get("device"),
        "start": arrow.get(json_event["start"]).datetime,
        "end": arrow.get(json_event["end"]).datetime,
        "metadata": json_event["metadata"],
        "created_at": arrow.get(json_event["createdAt"]).datetime,
        "updated_at": arrow.get(json_event["updatedAt"]).datetime,
        "properties": json_event.get("properties"),
        "event_type_id": json_event.get("eventTypeId"),
    }


def _event_type_dict(event_type):
    return {
        "id": event_type["id"],
        "name": event_type["name"],
        "color_name": event_type.get("colorName"),
        "custom_properties": event_type.get("customProperties", []),
        "created_at": arrow.get(event_type["createdAt"]).datetime,
        "updated_at": arrow.get(event_type["updatedAt"]).datetime,
    }


def _device_dict(device):
    created_at = device.get("createdAt")
    updated_at = device.get("updatedAt")
    return {
        "id": device["id"],
        "name": device["name"],
        "properties": device.get("properties"),
        "project_id": device.get("projectId"),
        "org_id": device.get("orgId"),
        "retain_recordings_seconds": device.get("retainRecordingsSeconds"),
        "enabled": device.get("enabled"),
        "remote_access_enabled": device.get("remoteAccessEnabled"),
        "auto_delete_after_import": device.get("autoDeleteAfterImport"),
        "created_at": arrow.get(created_at).datetime if created_at else None,
        "updated_at": arrow.get(updated_at).datetime if updated_at else None,
    }


def _device_token_dict(device_token):
    return {
        "id": device_token["id"],
        "created_at": arrow.get(device_token["createdAt"]).datetime,
        "device_id": device_token.get("deviceId"),
        "project_id": device_token["projectId"],
        "enabled": device_token["enabled"],
        "preapproved": device_token["preapproved"],
    }


def _session_dict(session):
    recording_start = session.get("recordingStart")
    recording_end = session.get("recordingEnd")
    return {
        "id": session["id"],
        "project_id": session["projectId"],
        "device": session["device"],
        "key": session.get("key"),
        "created_at": arrow.get(session["createdAt"]).datetime,
        "updated_at": arrow.get(session["updatedAt"]).datetime,
        "recordings": session["recordings"],
        "properties": session.get("properties"),
        # Only the list endpoint returns the recording summary; elsewhere these are None.
        "recording_count": session.get("recordingCount"),
        "recording_start": (
            arrow.get(recording_start).datetime if recording_start else None
        ),
        "recording_end": arrow.get(recording_end).datetime if recording_end else None,
    }


def _layout_dict(layout):
    saved_at = layout.get("savedAt")
    return {
        "id": layout["id"],
        "name": layout["name"],
        "created_at": arrow.get(layout["createdAt"]).datetime,
        "updated_at": arrow.get(layout["updatedAt"]).datetime,
        "saved_at": arrow.get(saved_at).datetime if saved_at else None,
        "folder_name": layout.get("folderName"),
        "permission": layout["permission"],
        "data": layout.get("data"),
    }


def _dataset_dict(dataset):
    result = {
        "id": dataset["id"],
        "project_id": dataset["projectId"],
        "name": dataset["name"],
        "description": dataset.get("description"),
        "creator_org_member_id": dataset.get("creatorOrgMemberId"),
        "creator_api_key_id": dataset.get("creatorApiKeyId"),
        "created_at": arrow.get(dataset["createdAt"]).datetime,
        "updated_at": arrow.get(dataset["updatedAt"]).datetime,
    }
    if "episodeCount" in dataset:
        result["episode_count"] = dataset["episodeCount"]
    if "added" in dataset:
        result["added"] = dataset["added"]
    if "updated" in dataset:
        result["updated"] = dataset["updated"]
    if "removed" in dataset:
        result["removed"] = dataset["removed"]
    if "alreadyPresent" in dataset:
        result["already_present"] = dataset["alreadyPresent"]
    return result


def _extension_dict(extension):
    result = {
        "id": extension["id"],
        "name": extension["name"],
        "publisher": extension["publisher"],
        "display_name": extension["displayName"],
        "description": extension.get("description"),
        "active_version": extension.get("activeVersion"),
        "sha256_sum": extension.get("sha256Sum"),
    }
    if "foxe" in extension:
        result["foxe"] = extension["foxe"]
    return result


def _episode_recording_dict(recording):
    return {
        "id": recording["id"],
        "path": recording["path"],
        "location": recording.get("location"),
        "start": arrow.get(recording["start"]).datetime,
        "end": arrow.get(recording["end"]).datetime,
        "device_id": recording.get("deviceId"),
        "available": recording["available"],
    }


def _site_dict(site):
    return {
        "id": site["id"],
        "name": site["name"],
        "type": site["type"],
        "url": site.get("url"),
        "retain_recordings_seconds": site.get("retainRecordingsSeconds"),
    }


def _site_ingest_path_policy_dict(policy):
    return {
        "revision": policy["revision"],
        "created_at": arrow.get(policy["createdAt"]).datetime,
        "policy": policy["policy"],
    }


def _episode_dict(episode):
    recordings = episode.get("recordings")
    result = {
        "id": episode["id"],
        "project_id": episode["projectId"],
        "start_time": arrow.get(episode["startTime"]).datetime,
        "end_time": arrow.get(episode["endTime"]).datetime,
        "version": episode["version"],
        "latest_version": episode["latestVersion"],
        "creator_org_member_id": episode.get("creatorOrgMemberId"),
        "creator_api_key_id": episode.get("creatorApiKeyId"),
        "created_at": arrow.get(episode["createdAt"]).datetime,
    }
    if "properties" in episode:
        result["properties"] = episode["properties"]
    if recordings is not None:
        result["recordings"] = [
            _episode_recording_dict(recording) for recording in recordings
        ]
    if "hasMissingRecordings" in episode:
        result["has_missing_recordings"] = episode["hasMissingRecordings"]
    # Creation context is passed through with API field names.
    if "creationContext" in episode:
        result["creation_context"] = episode["creationContext"]
    return result


_EPISODE_FIELD_NAMES = {"startTime": "start_time", "endTime": "end_time"}


def _episode_version_dict(version):
    return {
        "version": version["version"],
        "created_at": arrow.get(version["createdAt"]).datetime,
        "creator_org_member_id": version.get("creatorOrgMemberId"),
        "creator_api_key_id": version.get("creatorApiKeyId"),
        "changed_fields": [
            _EPISODE_FIELD_NAMES.get(field, field) for field in version["changedFields"]
        ],
    }


def _dataset_episode_dict(dataset_episode):
    result = {
        "added_at": arrow.get(dataset_episode["addedAt"]).datetime,
        "added_in_version": dataset_episode["addedInVersion"],
        "episode": _episode_dict(dataset_episode["episode"]),
    }
    if "hasMissingRecordings" in dataset_episode:
        result["has_missing_recordings"] = dataset_episode["hasMissingRecordings"]
    if "change" in dataset_episode:
        result["change"] = dataset_episode["change"]
    return result


def _site_token_dict(token):
    result = {
        "id": token["id"],
        "site_id": token["siteId"],
        "created_at": arrow.get(token["createdAt"]).datetime,
    }
    if "token" in token:
        result["token"] = token["token"]
    return result


def _dataset_version_dict(version):
    committed_at = version.get("committedAt")
    result = {
        "version_number": version["versionNumber"],
        "created_at": arrow.get(version["createdAt"]).datetime,
        "committed_at": arrow.get(committed_at).datetime if committed_at else None,
        "committed_by_org_member_id": version.get("committedByOrgMemberId"),
        "committed_by_api_key_id": version.get("committedByApiKeyId"),
        "episode_count": version["episodeCount"],
        "added_episode_count": version["addedEpisodeCount"],
        "removed_episode_count": version["removedEpisodeCount"],
        "changed_episode_count": version["changedEpisodeCount"],
        "description": version.get("description"),
    }
    if "hasMissingRecordings" in version:
        result["has_missing_recordings"] = version["hasMissingRecordings"]
    return result


def _site_inbox_notification_token_dict(token):
    result = {
        "id": token["id"],
        "org_id": token["orgId"],
        "site_id": token["siteId"],
        "created_at": arrow.get(token["createdAt"]).datetime,
    }
    if "token" in token:
        result["token"] = token["token"]
    return result


def _device_custom_property_time_interval_dict(interval):
    end = interval.get("end")
    return {
        "id": interval["id"],
        "device_id": interval["deviceId"],
        "key": interval["key"],
        "value": interval["value"],
        "start": arrow.get(interval["start"]).datetime,
        "end": arrow.get(end).datetime if end else None,
    }


def _automation_dict(automation):
    return {
        "id": automation["id"],
        "project_id": automation["projectId"],
        "name": automation["name"],
        "action_prompt": automation["actionPrompt"],
        "is_owner": automation["isOwner"],
        # Passed through with API field names.
        "permissions": automation["permissions"],
        "created_at": arrow.get(automation["createdAt"]).datetime,
        "updated_at": arrow.get(automation["updatedAt"]).datetime,
    }


def _automation_list_item_dict(automation):
    last_run_at = automation["lastRunAt"]
    return {
        **_automation_dict(automation),
        "lifetime_run_count": automation["lifetimeRunCount"],
        "trigger_enabled": automation["triggerEnabled"],
        "last_run_at": arrow.get(last_run_at).datetime if last_run_at else None,
        "last_run_status": automation["lastRunStatus"],
    }


__all__ = [
    "Client",
    "CompressionFormat",
    "FoxgloveException",
    "OutputFormat",
]
