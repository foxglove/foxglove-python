import datetime
import os
from enum import Enum
from io import BytesIO
from typing import IO, Any, Dict, List, Optional, Union
from typing_extensions import Protocol

import arrow
import requests

from .to_curl import to_curl


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

    def read(self, n: Optional[int]) -> bytes:
        chunk = self.__buf.read(n or -1) or bytes()
        self.__progress += int(len(chunk))
        if self.__callback:
            self.__callback(size=self.__length or 0, progress=self.__progress)
        return chunk

    def tell(self) -> int:
        return self.__progress


class Client:
    def __init__(self, token: str):
        self.__token = token
        self.__headers = {
            "Content-type": "application/json",
            "Authorization": "Bearer " + self.__token,
        }

    def __url__(self, path: str):
        return f"https://api.foxglove.dev{path}"

    def create_event(
        self,
        device_id: str,
        time: datetime.datetime,
        duration: int,
        metadata: Optional[Dict[str, str]] = {},
    ):
        """
        Creates a new event.

        device_id: The unique of the device associated with this event.
        time: The time at which the event occurred.
        duration: The duration of the event. Zero for an instantaneous event.
        metadata: Optional metadata attached to the event.
        """
        request = requests.post(
            self.__url__("/beta/device-events"),
            headers=self.__headers,
            json={
                "deviceId": device_id,
                "durationNanos": str(duration),
                "metadata": metadata,
                "timestamp": time.astimezone().isoformat(),
            },
        )
        request.raise_for_status()
        return request.json()

    def get_events(
        self,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        key: Optional[str] = None,
        value: Optional[str] = None,
    ):
        """
        Retrieves events.

        device_id: Id of the device associated with the events.
        device_name: Name of the device associated with events. Either device_id or device_name is required.
        sort_by: Optionally sort records by this field name.
        sort_order: Optionally specify the sort order, either "asc" or "desc".
        limit: Optionally limit the number of records return.
        offset: Optionally offset the results by this many records.
        start: Optionally exclude records before this time.
        end: Optionally exclude records after this time.
        key: Optionally only return records having this key = this value.
        value: Optionally only return records having this key = this value.
        """
        if not device_id and not device_name:
            raise FoxgloveException("One of device_id or device_name is required.")

        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "sortBy": sort_by,
            "sortOrder": sort_order,
            "limit": limit,
            "offset": offset,
            "start": start.astimezone().isoformat() if start else None,
            "end": end.astimezone().isoformat() if end else None,
            "key": key,
            "value": value,
        }
        response = requests.get(
            self.__url__("/beta/device-events"),
            headers=self.__headers,
            params={k: v for k, v in params.items() if v},
        )
        response.raise_for_status()
        return [
            {
                "id": e["id"],
                "device_id": e["deviceId"],
                "duration": int(e["durationNanos"]),
                "metadata": e["metadata"],
                # datetime doesn't support nanoseconds so we have to divide by 1e9 first.
                "timestamp": arrow.get(int(e["timestampNanos"]) / 1e9).datetime,
                "timestamp_nanos": int(e["timestampNanos"]),
                "created_at": arrow.get(e["createdAt"]).datetime,
                "updated_at": arrow.get(e["updatedAt"]).datetime,
            }
            for e in response.json()
        ]

    def download_data(
        self,
        device_id: str,
        start: datetime.datetime,
        end: datetime.datetime,
        topics: List[str] = [],
        output_format: OutputFormat = OutputFormat.mcap0,
        callback: Optional[ProgressCallback] = None,
    ) -> bytes:
        """
        Returns a readable data stream.

        device_id: The id of the device that originated the desired data.
        start: The earliest time from which to retrieve data.
        end: The latest time from which to retrieve data.
        topics: An optional list of topics to retrieve. All topics will be retrieved if this is omitted.
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
            json={k: v for k, v in params.items() if v},
        )
        link_response.raise_for_status()
        json = link_response.json()
        link = json["link"]
        response = requests.get(link, stream=True)
        data = bytes()
        for chunk in response.iter_content(chunk_size=32 * 1024):
            data += chunk
            if callback:
                callback(progress=len(data))
        return data

    def get_coverage(self, start: datetime.datetime, end: datetime.datetime):
        response = requests.get(
            self.__url__("/v1/data/coverage"),
            headers=self.__headers,
            params={
                "start": start.astimezone().isoformat(),
                "end": end.astimezone().isoformat(),
            },
        )
        response.raise_for_status()
        return response.json()

    def get_devices(self):
        response = requests.get(
            self.__url__("/v1/devices"),
            headers=self.__headers,
        )
        response.raise_for_status()
        return [
            {
                "id": d["id"],
                "name": d["name"],
                "serial_number": d["serialNumber"],
                "created_at": arrow.get(d["createdAt"]).datetime,
                "updated_at": arrow.get(d["updatedAt"]).datetime,
            }
            for d in response.json()
        ]

    def get_imports(self):
        response = requests.get(
            self.__url__("/v1/data/imports"),
            headers=self.__headers,
        )
        response.raise_for_status()
        return [
            {
                "import_id": i["importId"],
                "device_id": i["deviceId"],
                "import_time": arrow.get(i["importTime"]).datetime,
                "start": arrow.get(i["start"]).datetime,
                "end": arrow.get(i["end"]).datetime,
                "metadata": i["metadata"],
                "input_type": i["inputType"],
                "output_type": i["outputType"],
                "filename": i["filename"],
                "input_size": i["inputSize"],
                "total_output_size": i["totalOutputSize"],
            }
            for i in response.json()
        ]

    def get_topics(
        self,
        device_id: str,
        start: datetime.datetime,
        end: datetime.datetime,
        include_schemas: Optional[bool] = False,
    ):
        response = requests.get(
            self.__url__("/v1/data/topics"),
            headers=self.__headers,
            params={
                "deviceId": device_id,
                "start": start.astimezone().isoformat(),
                "end": end.astimezone().isoformat(),
                "includeSchemas": include_schemas,
            },
        )
        print(to_curl(response.request))
        response.raise_for_status()
        return response.json()

    def upload_data(
        self,
        device_id: str,
        filename: str,
        data: Union[bytes, IO[Any]],
        callback: Optional[SizeProgressCallback] = None,
    ):
        """
        Uploads data in bytes.

        device_id: Device id of the device from which this data originated.
        filename: A filename to associate with the data. The data format will be inferred from the file extension.
        data: The raw data in .bag or .mcap format.
        callback: An optional callback to report progress on the upload.
        """
        link_request = requests.post(
            self.__url__("/v1/data/upload"),
            headers=self.__headers,
            json={
                "deviceId": device_id,
                "filename": filename,
            },
        )
        link_request.raise_for_status()
        json = link_request.json()
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


__all__ = ["Client", "FoxgloveException", "OutputFormat"]
