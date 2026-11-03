from datetime import datetime

import responses
from dateutil.tz import tzoffset
from responses.matchers import query_param_matcher

from foxglove.client import Client

from .api_url import api_url

now = datetime.now(tzoffset(None, 0))


@responses.activate
def test_get_lake_files_by_device_and_time_range():
    responses.add(
        responses.GET,
        api_url("/v1/lake-files"),
        match=[
            query_param_matcher(
                {
                    "siteId": "site_1",
                    "deviceName": "robot",
                    "start": now.astimezone().isoformat(),
                    "end": now.astimezone().isoformat(),
                    "topic": "/camera",
                }
            )
        ],
        json=[{"path": "lake/robot/camera.mcap", "topics": ["/camera"]}],
    )
    lake_files = Client("test").get_lake_files(
        site_id="site_1", device_name="robot", start=now, end=now, topic="/camera"
    )
    assert lake_files == [{"path": "lake/robot/camera.mcap", "topics": ["/camera"]}]


@responses.activate
def test_get_lake_files_by_recording_and_session():
    responses.add(
        responses.GET,
        api_url("/v1/lake-files"),
        match=[
            query_param_matcher(
                {
                    "siteId": "site_1",
                    "recordingId": "rec_1",
                    "recordingKey": "key_1",
                    "sessionId": "ses_1",
                    "sessionKey": "ses_key",
                    "deviceId": "dev_1",
                }
            )
        ],
        json=[],
    )
    lake_files = Client("test").get_lake_files(
        site_id="site_1",
        recording_id="rec_1",
        recording_key="key_1",
        session_id="ses_1",
        session_key="ses_key",
        device_id="dev_1",
    )
    assert lake_files == []
