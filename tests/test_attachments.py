import responses
from faker import Faker
from foxglove_data_platform.client import Client
from datetime import datetime
from dateutil.tz import tzoffset

from .api_url import api_url

fake = Faker()


@responses.activate
def test_get_attachments():
    device_id = fake.uuid4()
    attachment_id = fake.uuid4()
    recording_id = fake.uuid4()
    path = fake.file_name(extension="txt")
    media_type = fake.mime_type()
    size = fake.random_number()
    crc = fake.random_number()
    fingerprint = fake.uuid4()
    site_id = fake.uuid4()
    now = datetime.now(tzoffset(None, 0))

    responses.add(
        responses.GET,
        api_url(f"/v1/recording-attachments"),
        json=[
            {
                "id": attachment_id,
                "recordingId": recording_id,
                "siteId": site_id,
                "name": path,
                "mediaType": media_type,
                "size": size,
                "crc": crc,
                "fingerprint": fingerprint,
                "logTime": now.isoformat(),
                "createTime": now.isoformat(),
            },
        ],
        match=[responses.matchers.query_string_matcher("sortBy=logTime")],
    )
    client = Client("test")
    attachments = client.get_attachments(
        sort_by="log_time",
    )
    assert attachments == [
        {
            "id": attachment_id,
            "recording_id": recording_id,
            "site_id": site_id,
            "name": path,
            "media_type": media_type,
            "size": size,
            "crc": crc,
            "fingerprint": fingerprint,
            "log_time": now,
            "create_time": now,
        },
    ]


@responses.activate
def test_download_attachment():
    id = "abcde"
    data = fake.binary(4096)
    responses.add(
        responses.GET,
        api_url(f"/v1/recording-attachments/{id}/download"),
        body=data,
    )
    client = Client("test")
    response_data = client.download_attachment(id=id)
    assert data == response_data
