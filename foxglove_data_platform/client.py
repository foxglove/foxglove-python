import datetime
import os
from enum import Enum
from io import BytesIO
import json
import warnings
from typing import IO, Any, Dict, List, Optional, Union
import base64

import arrow
import requests
from typing_extensions import Protocol


try:
    from mcap.records import Schema as McapSchema
    from mcap.reader import make_reader
except ModuleNotFoundError:
    McapSchema = None
    make_reader = None


def _err_on_construction(err):
    def construct():
        raise RuntimeError(f"Error importing decoder implementation: {err}")

    return construct


try:
    from mcap_ros1.decoder import Decoder as Ros1Decoder
except ModuleNotFoundError as err:
    Ros1Decoder = _err_on_construction(err)

try:
    from mcap_protobuf.decoder import Decoder as ProtobufDecoder
except ModuleNotFoundError as err:
    ProtobufDecoder = _err_on_construction(err)

try:
    from mcap_ros2.decoder import Decoder as Ros2Decoder
except ModuleNotFoundError as err:
    Ros2Decoder = _err_on_construction(err)


class JsonDecoder:
    def decode(self, schema_, message):
        return json.loads(message.data.decode("utf-8"))


def decoder_for_schema_encoding(encoding_string):
    if encoding_string == "ros1msg":
        return Ros1Decoder()
    if encoding_string == "ros2msg":
        return Ros2Decoder()
    if encoding_string == "protobuf":
        return ProtobufDecoder()
    if encoding_string == "jsonschema":
        return JsonDecoder()
    raise RuntimeError(f"No known decoder class for encoding {encoding_string}")


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
    headers: Optional[dict] = None,
    callback: Optional[ProgressCallback] = None,
):
    response = requests.get(url, headers=headers, stream=True)
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
        self.__headers = {
            "Content-type": "application/json",
            "Authorization": "Bearer " + self.__token,
        }
        self.__host = host

    def __url__(self, path: str):
        return f"https://{self.__host}{path}"

    def create_event(
        self,
        *,
        device_id: str,
        start: datetime.datetime,
        end: Optional[datetime.datetime],
        metadata: Optional[Dict[str, str]] = {},
    ):
        """
        Creates a new event.

        device_id: The unique of the device associated with this event.
        start: The event start time.
        end: The event end time. If not provided, an instantaneous event (with end == start)
            is created.
        metadata: Optional metadata attached to the event.
        """
        if end is None:
            end = start
        response = requests.post(
            self.__url__("/v1/events"),
            headers=self.__headers,
            json={
                "deviceId": device_id,
                "start": start.astimezone().isoformat(),
                "end": end.astimezone().isoformat(),
                "metadata": metadata,
            },
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
        response = requests.delete(
            self.__url__(f"/v1/events/{event_id}"),
            headers=self.__headers,
        )
        return json_or_raise(response)

    def get_events(
        self,
        *,
        device_id: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        query: Optional[str] = None,
    ):
        """
        Retrieves events.

        device_id: Id of the device associated with the events.
        sort_by: Optionally sort records by this field name (e.g. "device_id").
        sort_order: Optionally specify the sort order, either "asc" or "desc".
        limit: Optionally limit the number of records returned.
        offset: Optionally offset the results by this many records.
        start: Optionally exclude records before this time.
        end: Optionally exclude records after this time.
        query: optional query string to filter events by metadata.
            See https://foxglove.dev/docs/api#tag/Events/paths/~1events/get for a syntax definition
            of `query`.
        """
        params = {
            "deviceId": device_id,
            "sortBy": camelize(sort_by),
            "sortOrder": sort_order,
            "limit": limit,
            "offset": offset,
            "start": start.astimezone().isoformat() if start else None,
            "end": end.astimezone().isoformat() if end else None,
            "query": query,
        }
        response = requests.get(
            self.__url__("/v1/events"),
            headers=self.__headers,
            params={k: v for k, v in params.items() if v is not None},
        )

        return [_event_dict(event) for event in json_or_raise(response)]

    def get_messages(
        self,
        *,
        device_id: str,
        start: datetime.datetime,
        end: datetime.datetime,
        topics: List[str] = [],
    ):
        """
        Returns a list of tuples of (topic, raw mcap record, decoded message).

        device_id: The id of the device that originated the desired data.
        start: The earliest time from which to retrieve data.
        end: The latest time from which to retrieve data.
        topics: An optional list of topics to retrieve.
            All topics will be retrieved if this is omitted.
        """
        if not McapSchema or not make_reader:
            raise RuntimeError(
                "Mcap library not found. Please install the mcap library."
            )
        data = self.download_data(
            device_id=device_id, start=start, end=end, topics=topics
        )
        reader = make_reader(BytesIO(data))
        decoders = {}
        output_messages = []
        for schema, channel, message in reader.iter_messages():
            if schema.encoding not in decoders:
                decoder = decoder_for_schema_encoding(schema.encoding)
                decoders[schema.encoding] = decoder
            else:
                decoder = decoders[schema.encoding]
            output_messages.append(
                (channel.topic, message, decoder.decode(schema, message))
            )
        return output_messages

    def download_recording_data(
        self,
        *,
        id: str,
        output_format: OutputFormat = OutputFormat.mcap0,
        include_attachments: bool = False,
        callback: Optional[ProgressCallback] = None,
    ):
        """
        Returns raw data bytes for a recording.

        :param id: the ID of the recording.
        :param include_attachments: whether to include MCAP attachments in the returned data.
        :param output_format: The output format of the data, defaulting to .mcap.
            Note: You can only export a .bag file if you originally uploaded a .bag file.
        :param callback: an optional callback to report download progress.
        """
        params = {
            "recordingId": id,
            "includeAttachments": include_attachments,
            "outputFormat": output_format.value,
        }
        link_response = requests.post(
            self.__url__("/v1/data/stream"),
            headers=self.__headers,
            json={k: v for k, v in params.items() if v is not None},
        )

        json = json_or_raise(link_response)

        return _download_stream_with_progress(json["link"], callback=callback)

    def download_data(
        self,
        *,
        device_id: str,
        start: datetime.datetime,
        end: datetime.datetime,
        topics: List[str] = [],
        output_format: OutputFormat = OutputFormat.mcap0,
        callback: Optional[ProgressCallback] = None,
    ) -> bytes:
        """
        Returns raw data bytes for a device and time range.

        device_id: The id of the device that originated the desired data.
        start: The earliest time from which to retrieve data.
        end: The latest time from which to retrieve data.
        topics: An optional list of topics to retrieve.
            All topics will be retrieved if this is omitted.
        output_format: The output format of the data, either .bag or .mcap, defaulting to .mcap.
        """
        params = {
            "deviceId": device_id,
            "end": end.astimezone().isoformat(),
            "outputFormat": output_format.value,
            "start": start.astimezone().isoformat(),
            "topics": topics,
        }
        link_response = requests.post(
            self.__url__("/v1/data/stream"),
            headers=self.__headers,
            json={k: v for k, v in params.items() if v is not None},
        )

        json = json_or_raise(link_response)

        return _download_stream_with_progress(json["link"], callback=callback)

    def get_coverage(
        self,
        *,
        start: datetime.datetime,
        end: datetime.datetime,
        device_id: Optional[str] = None,
        tolerance: Optional[int] = None,
    ):
        """
        List coverage ranges for data.

        :param start: The earliest time after which to retrieve data.
        :param end: The latest time before which to retrieve data.
        :param device_id: Optional device id to limit data by.
        :param tolerance: Minimum interval (in seconds) that ranges must be separated by
            to be considered discrete.
        """
        params = {
            "deviceId": device_id,
            "tolerance": tolerance,
            "start": start.astimezone().isoformat(),
            "end": end.astimezone().isoformat(),
        }
        response = requests.get(
            self.__url__("/v1/data/coverage"),
            headers=self.__headers,
            params={k: v for k, v in params.items() if v is not None},
        )
        json = json_or_raise(response)

        return [
            {
                "device_id": c["deviceId"],
                "start": arrow.get(c["start"]).datetime,
                "end": arrow.get(c["end"]).datetime,
            }
            for c in json
        ]

    def get_device(self, *, device_id: str):
        """
        Gets a single device by id.

        :param device_id: The id of the device to retrieve.
        """
        response = requests.get(
            self.__url__(f"/v1/devices/{device_id}"),
            headers=self.__headers,
        )

        device = json_or_raise(response)

        return {
            "id": device["id"],
            "name": device["name"],
        }

    def get_devices(self):
        """
        Returns a list of all devices.
        """
        response = requests.get(
            self.__url__("/v1/devices"),
            headers=self.__headers,
        )

        json = json_or_raise(response)

        return [
            {
                "id": d["id"],
                "name": d["name"],
            }
            for d in json
        ]

    def create_device(
        self,
        *,
        name: str,
        serial_number: Optional[str] = None,
    ):
        """
        Creates a new device.

        name: The name of the devicee.
        serial_number: DEPRECATED: a serial number for the device. This argument has no effect.
        """
        if serial_number is not None:
            warnings.warn(
                "serial number argument is deprecated and will be removed in the next release"
            )
        response = requests.post(
            self.__url__("/v1/devices"), headers=self.__headers, json={"name": name}
        )

        device = json_or_raise(response)

        return {
            "id": device["id"],
            "name": device["name"],
        }

    def delete_device(self, *, device_id: str):
        """
        Deletes an existing device.

        Note: you must first delete all imports from the device; see `delete_import`.

        :param device_id: The id of the device.
        """
        response = requests.delete(
            self.__url__(f"/v1/devices/{device_id}"),
            headers=self.__headers,
        )
        json_or_raise(response)

    def delete_import(self, *, device_id: Optional[str] = None, import_id: str):
        """
        Deletes an existing import.

        :param device_id: The id of the device associated with the import. (Deprecated; ignored.)
        :param import_id: The id of the import to delete.
        """
        response = requests.delete(
            self.__url__(f"/v1/data/imports/{import_id}"),
            headers=self.__headers,
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
        response = requests.get(
            self.__url__("/v1/data/imports"),
            params={k: v for k, v in all_params.items() if v is not None},
            headers=self.__headers,
        )
        json = json_or_raise(response)

        return [
            {
                "import_id": i["importId"],
                "device_id": i["deviceId"],
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
    ):
        """Fetches recordings.

        :param device_id: Optionally filter responses by this device ID.
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
        """
        all_params = {
            "device.id": device_id,
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
        }
        response = requests.get(
            self.__url__("/v1/recordings"),
            params={k: v for k, v in all_params.items() if v is not None},
            headers=self.__headers,
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
                }
            )

        return out

    def get_attachments(
        self,
        *,
        device_id: Optional[str] = None,
        recording_id: Optional[str] = None,
        site_id: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
    ):
        """List recording attachments.

        :param device_id: Optionally filter responses by this device ID.
        :param recording_id: Optionally filter responses by this recording ID.
        :param site_id: Optionally filter responses by this site ID.
        :param sort_by: Optionally sort responses by this field name.
            currently only "log_time" is supported.
        :param sort_order: Optionally specify the sort order, either "asc" or "desc".
        :param limit: Optionally limit the number of records returned.
        :param offset: Optionally offset the results by this many records.
        """
        all_params = {
            "deviceId": device_id,
            "siteId": site_id,
            "recordingId": recording_id,
            "sortBy": camelize(sort_by),
            "sortOrder": sort_order,
            "limit": limit,
            "offset": offset,
        }
        response = requests.get(
            self.__url__("/v1/recording-attachments"),
            params={k: v for k, v in all_params.items() if v is not None},
            headers=self.__headers,
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
            headers=self.__headers,
            callback=callback,
        )

    def get_topics(
        self,
        *,
        device_id: str,
        start: datetime.datetime,
        end: datetime.datetime,
        include_schemas: bool = False,
    ):
        response = requests.get(
            self.__url__("/v1/data/topics"),
            headers=self.__headers,
            params={
                "deviceId": device_id,
                "start": start.astimezone().isoformat(),
                "end": end.astimezone().isoformat(),
                "includeSchemas": "true" if include_schemas else "false",
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
        device_id: str,
        filename: str,
        data: Union[bytes, IO[Any]],
        callback: Optional[SizeProgressCallback] = None,
    ):
        """
        Uploads data in bytes.

        device_id: Device id of the device from which this data originated.
        filename: A filename to associate with the data. The data format will be
            inferred from the file extension.
        data: The raw data in .bag or .mcap format.
        callback: An optional callback to report progress on the upload.
        """
        link_response = requests.post(
            self.__url__("/v1/data/upload"),
            headers=self.__headers,
            json={
                "deviceId": device_id,
                "filename": filename,
            },
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


def _event_dict(json_event):
    return {
        "id": json_event["id"],
        "device_id": json_event["deviceId"],
        "start": arrow.get(json_event["start"]).datetime,
        "end": arrow.get(json_event["end"]).datetime,
        "metadata": json_event["metadata"],
        "created_at": arrow.get(json_event["createdAt"]).datetime,
        "updated_at": arrow.get(json_event["updatedAt"]).datetime,
    }


__all__ = ["Client", "FoxgloveException", "OutputFormat"]
