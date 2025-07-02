import copy
import datetime
import os
from enum import Enum
from io import BytesIO
import json
from typing import IO, Any, Dict, List, Optional, TypeVar, Union
import base64
import warnings

import arrow
import requests
from typing_extensions import Protocol

from mcap.records import Schema as McapSchema
from mcap.well_known import MessageEncoding
from mcap.reader import make_reader
from mcap.decoder import DecoderFactory


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

    if 400 <= response.status_code < 500:
        response.reason = json.get("error", response.reason)

    response.raise_for_status()

    return json


def _download_stream_with_progress(
    url: str,
    session: requests.Session,
    callback: Optional[ProgressCallback] = None,
):
    response = session.get(url, stream=True)
    response.raise_for_status()
    data = BytesIO()
    for chunk in response.iter_content(chunk_size=32 * 1024):
        data.write(chunk)
        if callback:
            callback(progress=data.tell())
    return data.getvalue()


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
        start: datetime.datetime,
        end: Optional[datetime.datetime],
        metadata: Optional[Dict[str, str]] = {},
    ):
        """
        Creates a new event.

        device_id: The id of the device associated with this event.
        device_name: The name of the device associated with this event.
        start: The event start time.
        end: The event end time. If not provided, an instantaneous event (with end == start)
            is created.
        metadata: Optional metadata attached to the event.
        """
        if end is None:
            end = start
        if device_id is None and device_name is None:
            raise RuntimeError(
                "device_id or device_name must be provided to create_event"
            )
        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "start": start.astimezone().isoformat(),
            "end": end.astimezone().isoformat(),
            "metadata": metadata,
        }
        response = self.__session.post(
            self.__url__("/v1/events"),
            json={k: v for k, v in params.items() if v is not None},
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
        topics: List[str] = [],
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
        warnings.warn("Use `iter_messages` instead.", DeprecationWarning)
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
        start: datetime.datetime,
        end: datetime.datetime,
        topics: List[str] = [],
        decoder_factories: Optional[List[DecoderFactory]] = None,
    ):
        """
        yields a stream of (schema, channel, message, decoded message) values.

        device_id: The id of the device that originated the desired data.
        device_name: The name of the device that originated the desired data.
        start: The earliest time from which to retrieve data.
        end: The latest time from which to retrieve data.
        topics: An optional list of topics to retrieve.
            All topics will be retrieved if this is omitted.
        decoder_factories: an optional list of :py:class:`~mcap.decoder.DecoderFactory` instances
            used to decode message content.
        """
        stream_link = self._make_stream_link(
            device_id=device_id,
            device_name=device_name,
            start=start,
            end=end,
            topics=topics,
        )
        response = self.__session.get(stream_link, stream=True)
        response.raise_for_status()
        if decoder_factories is None:
            # We deep-copy here as these factories might be mutated
            decoder_factories = copy.deepcopy(DEFAULT_DECODER_FACTORIES)
        reader = make_reader(response.raw, decoder_factories=decoder_factories)
        # messages from Foxglove are already in log-time order.
        # specifying log_time_order=false allows us to skip a sort() in the MCAP library
        # after all messages are loaded.
        return reader.iter_decoded_messages(log_time_order=False)

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

        return _download_stream_with_progress(
            json["link"], self.__session, callback=callback
        )

    def _make_stream_link(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        start: datetime.datetime,
        end: datetime.datetime,
        topics: List[str] = [],
        output_format: OutputFormat = OutputFormat.mcap,
    ) -> str:
        if device_id is None and device_name is None:
            raise RuntimeError("device_id or device_name must be provided")

        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "end": end.astimezone().isoformat(),
            "outputFormat": output_format.value,
            "start": start.astimezone().isoformat(),
            "topics": topics,
        }
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
        start: datetime.datetime,
        end: datetime.datetime,
        topics: List[str] = [],
        output_format: OutputFormat = OutputFormat.mcap,
        callback: Optional[ProgressCallback] = None,
    ) -> bytes:
        """
        Returns raw data bytes for a device and time range.

        device_id: The id of the device that originated the desired data.
        device_name: The name of the device that originated the desired data.
        start: The earliest time from which to retrieve data.
        end: The latest time from which to retrieve data.
        topics: An optional list of topics to retrieve.
            All topics will be retrieved if this is omitted.
        output_format: The output format of the data, either .bag or .mcap, defaulting to .mcap.
        """
        return _download_stream_with_progress(
            self._make_stream_link(
                device_id=device_id,
                device_name=device_name,
                start=start,
                end=end,
                topics=topics,
                output_format=output_format,
            ),
            self.__session,
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
    ):
        """
        List coverage ranges for data.

        :param start: The earliest time after which to retrieve data.
        :param end: The latest time before which to retrieve data.
        :param device_id: Optional device id to limit data by.
        :param tolerance: Minimum interval (in seconds) that ranges must be separated by
            to be considered discrete.
        :param project_id: Optional Project to filter coverage by.
        """
        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "tolerance": tolerance,
            "start": start.astimezone().isoformat(),
            "end": end.astimezone().isoformat(),
            "projectId": project_id,
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
        self, *, device_id: Optional[str] = None, device_name: Optional[str] = None
    ):
        """
        Gets a single device by name or id.

        :param device_id: The id of the device to retrieve.
        :param device_name: The name of the device to retrieve.
        """
        if device_name and device_id:
            raise RuntimeError("device_id and device_name are mutually exclusive")
        if device_name is None and device_id is None:
            raise RuntimeError("device_id or device_name must be provided")
        response = self.__session.get(
            self.__url__(f"/v1/devices/{device_name or device_id}"),
        )

        device = json_or_raise(response)

        return {
            "id": device["id"],
            "name": device["name"],
            "properties": device["properties"] if "properties" in device else None,
            "project_id": device["projectId"] if "projectId" in device else None,
        }

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

        return [
            {
                "id": d["id"],
                "name": d["name"],
                "properties": d["properties"] if "properties" in d else None,
                "project_id": d["projectId"] if "projectId" in d else None,
            }
            for d in json
        ]

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

        device = json_or_raise(response)

        return {
            "id": device["id"],
            "name": device["name"],
            "properties": device["properties"] if "properties" in device else None,
            "project_id": device["projectId"] if "projectId" in device else None,
        }

    def update_device(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        new_name: Optional[str] = None,
        properties: Optional[Dict[str, Union[str, bool, float, int]]] = None,
    ):
        """
        Updates a device.

        :param device_id: The id of the device to retrieve.
        :param device_name: The name of the device to retrieve.
        :param new_name: Optional new name to assign to the device.
        :param properties: Optional custom properties to add to or edit on the device.
            Each key must be defined as a custom property for your organization
            and each value must be of the appropriate type.
        """
        if device_name and device_id:
            raise RuntimeError("device_id and device_name are mutually exclusive")
        if device_name is None and device_id is None:
            raise RuntimeError("device_id or device_name must be provided")

        response = self.__session.patch(
            self.__url__(f"/v1/devices/{device_name or device_id}"),
            json=without_nulls({"name": new_name, "properties": properties}),
        )

        device = json_or_raise(response)

        return {
            "id": device["id"],
            "name": device["name"],
            "properties": device["properties"] if "properties" in device else None,
            "project_id": device["projectId"] if "projectId" in device else None,
        }

    def delete_device(
        self, *, device_id: Optional[str] = None, device_name: Optional[str] = None
    ):
        """
        Deletes an existing device.

        Note: you must first delete all imports from the device; see `delete_import`.

        :param device_id: The id of the device.
        :param device_name: The name of the device.
        """
        if device_name and device_id:
            raise RuntimeError("device_id and device_name are mutually exclusive")
        if device_name is None and device_id is None:
            raise RuntimeError("device_id or device_name must be provided")
        response = self.__session.delete(
            self.__url__(f"/v1/devices/{device_name or device_id}"),
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
        return _download_stream_with_progress(
            self.__url__(f"/v1/recording-attachments/{id}/download"),
            self.__session,
            callback=callback,
        )

    def get_topics(
        self,
        *,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        start: datetime.datetime,
        end: datetime.datetime,
        include_schemas: bool = False,
        project_id: Optional[str] = None,
    ):
        """
        List topics.

        :param device_id: Optionally filter topics by this device ID.
        :param device_name: Optionally filter topics by this device name.
        :param start: Filter topics by this start time.
        :param end: Filter topics by this end time.
        :param include_schemas: Optionally include the schema in the response.
        :param project_id: Optional Project to filter topics by.
        """
        response = self.__session.get(
            self.__url__("/v1/data/topics"),
            params={
                "deviceId": device_id,
                "deviceName": device_name,
                "start": start.astimezone().isoformat(),
                "end": end.astimezone().isoformat(),
                "includeSchemas": "true" if include_schemas else "false",
                "projectId": project_id,
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
        """
        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "filename": filename,
            "key": key,
            "projectId": project_id,
        }
        link_response = self.__session.post(
            self.__url__("/v1/data/upload"),
            json={k: v for k, v in params.items() if v is not None},
        )

        json = json_or_raise(link_response)

        link = json["link"]
        buffer = ProgressBufferReader(data, callback=callback)
        upload_request = self.__session.put(
            link,
            data=buffer,
            headers={"Content-Type": "application/octet-stream"},
        )
        return {
            "link": link,
            "text": upload_request.text,
            "code": upload_request.status_code,
        }


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
    }


__all__ = ["Client", "FoxgloveException", "OutputFormat"]
