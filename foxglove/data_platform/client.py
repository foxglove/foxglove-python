import datetime
from typing import Dict

import requests


class FoxgloveException(Exception):
    pass


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
        duration: int,
        metadata: Dict[str, str],
    ):
        r = requests.post(
            self.__url__("/beta/device-events"),
            headers=self.__headers,
            json={
                "deviceId": device_id,
                "timestamp": time.isoformat(),
                "durationNanos": str(duration),
                "metadata": metadata,
            },
        )
        return r.json()

    def list_events(self):
        r = requests.get(
            self.__url__("/beta/device-events"),
            headers=self.__headers,
        )
        return r.json()

    def upload_data(self, device_id: str, filename: str, data: bytes):
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
        upload_request = requests.put(
            link, headers={"Content-Type": "application/octet-stream"}, data=data
        )
        return {
            "link": link,
            "text": upload_request.text,
            "code": upload_request.status_code,
        }


__all__ = ["Client", "FoxgloveException"]
