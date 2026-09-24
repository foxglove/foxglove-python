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
        }
        response = self.__session.get(
            self.__url__("/v1/events"),
            params={k: v for k, v in params.items() if v is not None},
        )

        return [_event_dict(event) for event in json_or_raise(response)]

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
    ):
        """
        Returns raw data bytes for a recording.

        :param id: the ID of the recording.
        :param key: the key of the recording.
        :param include_attachments: whether to include MCAP attachments in the returned data.
        :param output_format: The output format of the data, defaulting to .mcap.
            Note: You can only export a .bag file if you originally uploaded a .bag file.
        :param callback: an optional callback to report download progress.
        """
        if id is None and key is None:
            raise RuntimeError("id or key must be provided")
        params = {
            "recordingId": id,
            "key": key,
            "includeAttachments": include_attachments,
            "outputFormat": output_format.value,
        }
        link_response = self.__session.post(
            self.__url__("/v1/data/stream"),
            json={k: v for k, v in params.items() if v is not None},
        )

        json = json_or_raise(link_response)

        return _download_stream_with_progress(json["link"], callback=callback)

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
    ) -> str:
        if topics is None:
            topics = []
        if (
            device_id is None
            and device_name is None
            and session_id is None
            and session_key is None
            and episode_id is None
        ):
            raise RuntimeError(
                "device_id or device_name or session_id or session_key or episode_id "
                "must be provided"
            )
        if episode_id is not None and (
            device_id is not None
            or device_name is not None
            or session_id is not None
            or session_key is not None
        ):
            raise RuntimeError("episode_id cannot be combined with another identifier")
        if episode_id is None and (start is None or end is None):
            raise RuntimeError("start and end must be provided unless using episode_id")

        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "sessionId": session_id,
            "sessionKey": session_key,
            "episodeId": episode_id,
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
            ),
            callback=callback,
        )

    def get_coverage(
        self,
        *,
        start: datetime.datetime,
        end: datetime.datetime,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        tolerance: Optional[int] = None,
        project_id: Optional[str] = None,
        session_id: Optional[str] = None,
        session_key: Optional[str] = None,
    ):
        """
        List coverage ranges for data.

        :param start: The earliest time after which to retrieve data.
        :param end: The latest time before which to retrieve data.
        :param device_id: Optional device id to limit data by.
        :param tolerance: Minimum interval (in seconds) that ranges must be separated by
            to be considered discrete.
        :param project_id: Optional Project to filter coverage by.
        :param session_id: Optional Session ID to filter coverage by.
        :param session_key: Optional Session key to filter coverage by.
        """
        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "tolerance": tolerance,
            "start": start.astimezone().isoformat(),
            "end": end.astimezone().isoformat(),
            "projectId": project_id,
            "sessionId": session_id,
            "sessionKey": session_key,
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
            }
            for c in json
        ]

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

    def get_devices(self, *, project_id: Optional[str] = None):
        """
        Returns a list of all devices.

        :param project_id: Optional Project to filter devices by.
        """
        response = self.__session.get(
            self.__url__("/v1/devices"),
            params=without_nulls({"projectId": project_id}),
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
        """
        identifier = _device_identifier_for_path(device_id, device_name)

        response = self.__session.patch(
            self.__url__(f"/v1/devices/{identifier}"),
            params={"projectId": project_id} if project_id is not None else None,
            json=without_nulls({"name": new_name, "properties": properties}),
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
        }
        response = self.__session.get(
            self.__url__("/v1/recordings"),
            params={k: v for k, v in all_params.items() if v is not None},
        )
        json = json_or_raise(response)

        out = []
        for i in json:
            imported_at = i.get("importedAt")
            if imported_at is not None:
                imported_at = arrow.get(imported_at).datetime
            out.append(
                {
                    "id": i["id"],
                    "path": i["path"],
                    "size": i["size"],
                    "message_count": i.get("messageCount"),
                    "created_at": arrow.get(i["createdAt"]).datetime,
                    "imported_at": imported_at,
                    "start": arrow.get(i["start"]).datetime,
                    "end": arrow.get(i["end"]).datetime,
                    "import_status": i["importStatus"],
                    "site": i.get("site"),
                    "edge_site": i.get("edgeSite"),
                    "device": i.get("device"),
                    "metadata": i.get("metadata"),
                    "key": i.get("key"),
                    "project_id": i.get("projectId"),
                    "session_id": i.get("sessionId"),
                    "session_key": i.get("sessionKey"),
                }
            )

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
        return [
            {
                "id": i["id"],
                "recording_id": i["recordingId"],
                "site_id": i["siteId"],
                "name": i["name"],
                "media_type": i["mediaType"],
                "size": i["size"],
                "crc": i["crc"],
                "fingerprint": i["fingerprint"],
                "log_time": arrow.get(i["logTime"]).datetime,
                "create_time": arrow.get(i["createTime"]).datetime,
            }
            for i in json
        ]

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
        """
        if episode_id is not None and (
            device_id is not None
            or device_name is not None
            or session_id is not None
            or session_key is not None
        ):
            raise RuntimeError("episode_id cannot be combined with another identifier")
        if episode_id is None and (start is None or end is None):
            raise RuntimeError("start and end must be provided unless using episode_id")
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
        offset: Optional[int] = None,
        cursor: Optional[str] = None,
    ):
        """Return a Page of datasets; name is a case-insensitive substring filter.

        ``limit`` is the page size. Use ``cursor`` for continuation; ``offset`` is
        deprecated. Use ``page.auto_paging_iter()`` to traverse all matching items.
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
                    "offset": offset,
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
        if params.get("cursor") is not None and params.get("offset", 0) != 0:
            raise ValueError("cursor cannot be combined with a nonzero offset")
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
            legacy_offset=params.get("offset", 0) != 0,
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
        offset: Optional[int] = None,
        cursor: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        has_missing_recordings: Optional[bool] = None,
        recording_id: Optional[str] = None,
        include_recordings: bool = False,
    ):
        """Return a Page of the latest committed or initial editable membership.

        Supply ``start`` and ``end`` together to filter overlapping episode windows.
        ``limit`` is page size; ``cursor`` continues a page and ``offset`` is deprecated.
        Use ``page.auto_paging_iter()`` to traverse all matching episodes.
        """
        return self._get_dataset_episodes(
            dataset_id=dataset_id,
            version_number=None,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset,
            cursor=cursor,
            start=start,
            end=end,
            has_missing_recordings=has_missing_recordings,
            recording_id=recording_id,
            include_recordings=include_recordings,
        )

    def update_dataset_episodes(
        self,
        *,
        dataset_id: str,
        add: Optional[List[str]] = None,
        remove: Optional[List[str]] = None,
    ):
        """Stage episode additions and removals in the editable version."""
        response = self.__session.patch(
            self.__url__(f"/v1/datasets/{dataset_id}/episodes"),
            json=without_nulls({"add": add, "remove": remove}),
        )
        result = json_or_raise(response)
        return {
            "added": result["added"],
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
        offset: Optional[int] = None,
    ):
        """Return a Page of committed versions and the current editable version.

        ``limit`` is page size; ``cursor`` continues a page and ``offset`` is deprecated.
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
                    "offset": offset,
                }
            ),
        )

    def get_dataset_version(self, *, dataset_id: str, version_number: int):
        """Return one version, including its recording availability."""
        response = self.__session.get(
            self.__url__(f"/v1/datasets/{dataset_id}/versions/{version_number}")
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
        offset: Optional[int] = None,
        cursor: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        has_missing_recordings: Optional[bool] = None,
        recording_id: Optional[str] = None,
        include_recordings: bool = False,
    ):
        """Return a Page of episode membership in a specific version.

        Supply ``start`` and ``end`` together to filter overlapping episode windows.
        ``limit`` is page size; ``cursor`` continues a page and ``offset`` is deprecated.
        Use ``page.auto_paging_iter()`` to traverse all matching episodes.
        """
        return self._get_dataset_episodes(
            dataset_id=dataset_id,
            version_number=version_number,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset,
            cursor=cursor,
            start=start,
            end=end,
            has_missing_recordings=has_missing_recordings,
            recording_id=recording_id,
            include_recordings=include_recordings,
        )

    def _get_dataset_episodes(
        self,
        *,
        dataset_id: str,
        version_number: Optional[int],
        sort_by: Optional[str],
        sort_order: Optional[str],
        limit: Optional[int],
        offset: Optional[int],
        cursor: Optional[str],
        start: Optional[datetime.datetime],
        end: Optional[datetime.datetime],
        has_missing_recordings: Optional[bool],
        recording_id: Optional[str],
        include_recordings: bool,
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
                    "offset": offset,
                    "cursor": cursor,
                    "start": start.astimezone().isoformat() if start else None,
                    "end": end.astimezone().isoformat() if end else None,
                    "hasMissingRecordings": (
                        bool_query_param(has_missing_recordings)
                        if has_missing_recordings is not None
                        else None
                    ),
                    "recordingId": recording_id,
                    "include": "recordings" if include_recordings else None,
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
    ):
        """Return membership changes from ``base_version`` to ``version_number``."""
        response = self.__session.get(
            self.__url__(
                f"/v1/datasets/{dataset_id}/versions/{version_number}/compare"
            ),
            params=without_nulls(
                {
                    "version": base_version,
                    "limit": limit,
                    "cursor": cursor,
                    "include": "recordings" if include_recordings else None,
                }
            ),
        )
        result = json_or_raise(response)
        return {
            "changes": [_dataset_episode_dict(change) for change in result["changes"]],
            "added_count": result["addedCount"],
            "removed_count": result["removedCount"],
            "next_cursor": result["nextCursor"],
        }

    def commit_dataset(self, *, dataset_id: str):
        """Commit staged changes and open the next editable version."""
        response = self.__session.post(
            self.__url__(f"/v1/datasets/{dataset_id}/commit"), json={}
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
            "removed": result["removed"],
            "discarded_adds": result["discardedAdds"],
            "discarded_removes": result["discardedRemoves"],
        }

    def create_episodes(self, *, project_id: str, episodes: List[Dict[str, Any]]):
        """Create episodes, reusing any with identical membership and bounds.

        :param project_id: Project in which to create the episodes.
        :param episodes: Episode definitions. Each dictionary requires ``recordings`` and
            may include ``start_time``, ``end_time``, and ``metadata``.
        """
        serialized = []
        for episode in episodes:
            start_time = episode.get("start_time")
            end_time = episode.get("end_time")
            serialized.append(
                without_nulls(
                    {
                        "recordings": episode["recordings"],
                        "startTime": (
                            start_time.astimezone().isoformat() if start_time else None
                        ),
                        "endTime": (
                            end_time.astimezone().isoformat() if end_time else None
                        ),
                        "metadata": episode.get("metadata"),
                    }
                )
            )
        response = self.__session.post(
            self.__url__("/v1/episodes"),
            json={"projectId": project_id, "episodes": serialized},
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
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        cursor: Optional[str] = None,
        include_recordings: bool = False,
    ):
        """Return a Page of episodes, optionally with recording details.

        Supply ``start`` and ``end`` together to filter overlapping episode windows.
        ``limit`` is page size; ``cursor`` continues a page and ``offset`` is deprecated.
        Use ``page.auto_paging_iter()`` to traverse all matching episodes.
        """
        _validate_episode_range(start, end)
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
                    "sortBy": camelize(sort_by),
                    "sortOrder": sort_order,
                    "limit": limit,
                    "offset": offset,
                    "cursor": cursor,
                    "include": "recordings" if include_recordings else None,
                }
            ),
        )

    def get_episode(self, *, episode_id: str, include_recordings: bool = False):
        """Return an episode, optionally with recording details."""
        response = self.__session.get(
            self.__url__(f"/v1/episodes/{episode_id}"),
            params={"include": "recordings"} if include_recordings else None,
        )
        return _episode_dict(json_or_raise(response))

    def delete_episode(self, *, episode_id: str):
        """Delete an episode if it does not belong to a dataset."""
        response = self.__session.delete(self.__url__(f"/v1/episodes/{episode_id}"))
        json_or_raise(response)

    def download_dataset(
        self,
        *,
        dataset_id: str,
        version_number: int,
        output_directory: Union[str, os.PathLike],
        topics: Optional[List[str]] = None,
    ) -> Path:
        """Export a committed version to MCAP files and an app-format manifest.json.

        ``topics`` selects topics; None or [] downloads all topics. Episodes with
        some missing recordings are downloaded partially and flagged in the manifest.
        Episodes with no streamable recordings are skipped. Other request failures
        before streaming are recorded and the export continues; if all attempts fail,
        this raises after writing the manifest. An all-skipped export succeeds.

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
        # Capture the complete selection before transfers, so even an interrupted
        # export has the same selection digest as the app. MCAP bytes stay streamed.
        episodes = list(
            self.get_dataset_version_episodes(
                dataset_id=dataset_id,
                version_number=version_number,
                sort_by="start_time",
                sort_order="asc",
                limit=2000,
            ).auto_paging_iter()
        )
        destination.mkdir(parents=True, exist_ok=True)
        selected_topics = list(topics) if topics else None
        return export_dataset(
            dataset=dataset,
            version=version,
            episodes=episodes,
            destination=destination,
            topics=selected_topics,
            download=lambda episode_id, output_path: self._download_episode_to_file(
                episode_id=episode_id, output_path=output_path, topics=selected_topics
            ),
        )

    def _download_episode_to_file(
        self, *, episode_id: str, output_path: Path, topics: Optional[List[str]] = None
    ) -> int:
        return download_episode(
            output_path,
            lambda: self._make_stream_link(episode_id=episode_id, topics=topics),
        )

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


def _session_identifier(session_id: Optional[str], session_key: Optional[str]) -> str:
    if session_id is not None and session_key is not None:
        raise RuntimeError("session_id and session_key are mutually exclusive")
    if session_id is None and session_key is None:
        raise RuntimeError("session_id or session_key must be provided")

    identifier = session_id if session_id is not None else session_key
    assert identifier is not None, "one of session_id or session_key must be provided"
    return identifier


def _session_identifier_for_path(
    session_id: Optional[str], session_key: Optional[str]
) -> str:
    return urlquote(_session_identifier(session_id, session_key), safe="")


def _device_identifier(device_id: Optional[str], device_name: Optional[str]) -> str:
    if device_id is not None and device_name is not None:
        raise RuntimeError("device_id and device_name are mutually exclusive")
    if device_id is None and device_name is None:
        raise RuntimeError("device_id or device_name must be provided")

    identifier = device_id if device_id is not None else device_name
    assert identifier is not None, "one of device_id or device_name must be provided"
    return identifier


def _device_identifier_for_path(
    device_id: Optional[str], device_name: Optional[str]
) -> str:
    return urlquote(_device_identifier(device_id, device_name), safe="")


def _event_dict(json_event):
    return {
        "id": json_event["id"],
        "device_id": json_event["deviceId"],
        "device": json_event["device"],
        "start": arrow.get(json_event["start"]).datetime,
        "end": arrow.get(json_event["end"]).datetime,
        "metadata": json_event["metadata"],
        "created_at": arrow.get(json_event["createdAt"]).datetime,
        "updated_at": arrow.get(json_event["updatedAt"]).datetime,
        "properties": json_event.get("properties"),
        "event_type_id": json_event.get("eventTypeId"),
    }


def _device_dict(device):
    created_at = device.get("createdAt")
    updated_at = device.get("updatedAt")
    return {
        "id": device["id"],
        "name": device["name"],
        "properties": device.get("properties"),
        "project_id": device.get("projectId"),
        "created_at": arrow.get(created_at).datetime if created_at else None,
        "updated_at": arrow.get(updated_at).datetime if updated_at else None,
    }


def _session_dict(session):
    return {
        "id": session["id"],
        "project_id": session["projectId"],
        "device": session["device"],
        "key": session.get("key"),
        "created_at": arrow.get(session["createdAt"]).datetime,
        "updated_at": arrow.get(session["updatedAt"]).datetime,
        "recordings": session["recordings"],
        "properties": session.get("properties"),
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
    if "removed" in dataset:
        result["removed"] = dataset["removed"]
    if "alreadyPresent" in dataset:
        result["already_present"] = dataset["alreadyPresent"]
    return result


def _episode_recording_dict(recording):
    return {
        "id": recording["id"],
        "path": recording["path"],
        "start": arrow.get(recording["start"]).datetime,
        "end": arrow.get(recording["end"]).datetime,
        "device_id": recording.get("deviceId"),
        "available": recording["available"],
        "resolvable": recording["resolvable"],
    }


def _episode_dict(episode):
    recordings = episode.get("recordings")
    result = {
        "id": episode["id"],
        "project_id": episode["projectId"],
        "start_time": arrow.get(episode["startTime"]).datetime,
        "end_time": arrow.get(episode["endTime"]).datetime,
        "metadata": episode["metadata"],
        "creator_org_member_id": episode.get("creatorOrgMemberId"),
        "creator_api_key_id": episode.get("creatorApiKeyId"),
        "created_at": arrow.get(episode["createdAt"]).datetime,
    }
    if recordings is not None:
        result["recordings"] = [
            _episode_recording_dict(recording) for recording in recordings
        ]
    if "hasMissingRecordings" in episode:
        result["has_missing_recordings"] = episode["hasMissingRecordings"]
    return result


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
    }
    if "hasMissingRecordings" in version:
        result["has_missing_recordings"] = version["hasMissingRecordings"]
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


__all__ = [
    "Client",
    "CompressionFormat",
    "FoxgloveException",
    "OutputFormat",
]
