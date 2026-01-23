from datetime import datetime, timezone
from tempfile import TemporaryFile

import responses
from faker import Faker
from foxglove.client import Client, CompressionFormat
from responses.matchers import json_params_matcher

from .api_url import api_url

fake = Faker()


@responses.activate
def test_download():
    download_link = fake.url()
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        json={
            "link": download_link,
        },
    )
    data = fake.binary(4096)
    responses.add(responses.GET, download_link, body=data)
    client = Client("test")
    response_data = client.download_data(
        device_id="test_id", start=datetime.now(), end=datetime.now()
    )
    assert data == response_data


@responses.activate
def test_download_with_compression_format():
    download_link = fake.url()
    start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        match=[
            json_params_matcher(
                {
                    "deviceId": "test_id",
                    "start": start.astimezone().isoformat(),
                    "end": end.astimezone().isoformat(),
                    "outputFormat": "mcap",
                    "compressionFormat": "lz4",
                    "topics": [],
                },
            )
        ],
        json={
            "link": download_link,
        },
    )
    data = fake.binary(4096)
    responses.add(responses.GET, download_link, body=data)
    client = Client("test")
    response_data = client.download_data(
        device_id="test_id",
        start=start,
        end=end,
        compression_format=CompressionFormat.lz4,
    )
    assert data == response_data


@responses.activate
def test_download_with_compression_format_none():
    download_link = fake.url()
    start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        match=[
            json_params_matcher(
                {
                    "deviceId": "test_id",
                    "start": start.astimezone().isoformat(),
                    "end": end.astimezone().isoformat(),
                    "outputFormat": "mcap",
                    "compressionFormat": "",
                    "topics": [],
                },
            )
        ],
        json={
            "link": download_link,
        },
    )
    data = fake.binary(4096)
    responses.add(responses.GET, download_link, body=data)
    client = Client("test")
    response_data = client.download_data(
        device_id="test_id",
        start=start,
        end=end,
        compression_format=CompressionFormat.none,
    )
    assert data == response_data


@responses.activate
def test_download_without_compression_format():
    """Test that compressionFormat is not sent when not specified."""
    download_link = fake.url()
    start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        match=[
            json_params_matcher(
                {
                    "deviceId": "test_id",
                    "start": start.astimezone().isoformat(),
                    "end": end.astimezone().isoformat(),
                    "outputFormat": "mcap",
                    "topics": [],
                },
            )
        ],
        json={
            "link": download_link,
        },
    )
    data = fake.binary(4096)
    responses.add(responses.GET, download_link, body=data)
    client = Client("test")
    response_data = client.download_data(
        device_id="test_id",
        start=start,
        end=end,
    )
    assert data == response_data


@responses.activate
def test_download_recording_data():
    download_link = fake.url()
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        json={
            "link": download_link,
        },
    )
    data = fake.binary(4096)
    responses.add(responses.GET, download_link, body=data)
    client = Client("test")
    response_data = client.download_recording_data(key="test_key")
    assert data == response_data


@responses.activate
def test_streaming_upload():
    upload_link = fake.url()
    responses.add(
        responses.POST,
        api_url("/v1/data/upload"),
        json={
            "link": upload_link,
        },
    )
    responses.add(responses.PUT, upload_link)
    client = Client("test")
    data = fake.binary(4096)
    file = TemporaryFile()
    file.write(data)
    file.seek(0)
    upload_response = client.upload_data(
        device_id="test_device_id",
        filename="test_file.mcap",
        data=file,
        callback=lambda size, progress: print(".", end=""),
    )
    assert upload_response["link"] == upload_link


@responses.activate
def test_upload():
    upload_link = fake.url()
    device_id = "test_device_id"
    filename = "test_file.mcap"
    responses.add(
        responses.POST,
        api_url("/v1/data/upload"),
        match=[
            json_params_matcher(
                {
                    "deviceId": device_id,
                    "filename": filename,
                },
            )
        ],
        json={
            "link": upload_link,
        },
    )
    responses.add(responses.PUT, upload_link)
    client = Client("test")
    data = fake.binary(4096)
    upload_response = client.upload_data(
        device_id=device_id, filename=filename, data=data
    )
    assert upload_response["link"] == upload_link


@responses.activate
def test_upload_deviceless():
    upload_link = fake.url()
    key = "abc123"
    filename = "test_file.mcap"
    responses.add(
        responses.POST,
        api_url("/v1/data/upload"),
        match=[
            json_params_matcher(
                {
                    "key": key,
                    "filename": filename,
                },
            )
        ],
        json={
            "link": upload_link,
        },
    )
    responses.add(responses.PUT, upload_link)
    client = Client("test")
    data = fake.binary(4096)
    upload_response = client.upload_data(data=data, filename=filename, key=key)
    assert upload_response["link"] == upload_link
