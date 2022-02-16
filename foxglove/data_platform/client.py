import datetime
from enum import Enum
from io import BytesIO
from typing import Dict, List, Optional, Protocol

import requests


class FoxgloveException(Exception):
    pass


class ProgressCallback(Protocol):
    def __call__(self, size: int, progress: int) -> None:
        pass


class OutputFormat(Enum):
    bag = "bag1"
    mcap = "mcap"


class ProgressBufferReader(BytesIO):
    def __init__(self, buf: bytes = b"", callback: Optional[ProgressCallback] = None):
        self.__callback = callback
        self.__progress = 0
        self.__length = len(buf)
        BytesIO.__init__(self, buf)

    def __len__(self):
        return self.__length

    def read(self, n: Optional[int]) -> bytes:
        chunk = BytesIO.read(self, n)
        self.__progress += int(len(chunk))
        if self.__callback:
            self.__callback(size=self.__length, progress=self.__progress)
        return chunk


class Client:
    def __init__(self, token: str):
        self.__token = token
        self.__headers = {
            "Content-type": "application/json",
            "Authorization": "Bearer " + self.__token,
        }

    def __url__(self, path: str):
        return f"https://api-dev.foxglove.dev{path}"

    def create_event(
        self,
        device_id: str,
        time: datetime.datetime,
        duration: Optional[int] = 0,
        metadata: Optional[Dict[str, str]] = {},
    ):
        """
        Creates a new event.

        device_id: The unique of the device associated with this event.
        time: The time at which the event occurred.
        duration: The optional duration of the event, defaulting to 0.
        metadata: Optional metadata attached to the event.
        """
        r = requests.post(
            self.__url__("/beta/device-events"),
            headers=self.__headers,
            json={
                "deviceId": device_id,
                "durationNanos": str(duration),
                "metadata": metadata,
                "timestamp": time.astimezone().isoformat(),
            },
        )
        return r.json()

    def list_events(
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

        r = requests.get(
            self.__url__("/beta/device-events"),
            headers=self.__headers,
            json={
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
            },
        )
        return r.json()

    def download_data(
        self,
        device_id: str,
        start: datetime.datetime,
        end: datetime.datetime,
        topics: List[str] = [],
        output_format: OutputFormat = OutputFormat.mcap,
    ) -> BytesIO:
        """
        Returns a readable data stream.

        device_id: The id of the device that originated the desired data.
        start: The earliest time from which to retrieve data.
        end: The latest time from which to retrieve data.
        topics: An optional list of topics to retrieve. All topics will be retrieved if this is omitted.
        output_format: The output format of the data, either .bag or .mcap, defaulting to .mcap.
        """

        link_request = requests.post(
            self.__url__("/v1/data/stream"),
            headers=self.__headers,
            json={
                "deviceId": device_id,
                "end": end.astimezone().isoformat(),
                "outputFormat": output_format.value,
                "start": start.astimezone().isoformat(),
                "topics": topics,
            },
        )
        json = link_request.json()
        link = json["link"]
        response = requests.get(link, stream=True)
        return response.raw

    def upload_data(
        self,
        device_id: str,
        filename: str,
        data: bytes,
        callback: Optional[ProgressCallback] = None,
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
