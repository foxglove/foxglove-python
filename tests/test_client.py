from datetime import datetime
from tempfile import TemporaryFile

import responses
from faker import Faker
from foxglove_data_platform.client import Client

fake = Faker()


@responses.activate
def test_get_events():
    device_id = "my_device_id"
    responses.add(
        responses.GET,
        f"https://api.foxglove.dev/beta/device-events?deviceId={device_id}",
        json=[
            {
                "id": "1",
                "createdAt": datetime.now().isoformat(),
                "deviceId": device_id,
                "durationNanos": fake.pyint(),
                "metadata": {},
                "timestampNanos": fake.pyint(),
                "updatedAt": datetime.now().isoformat(),
            }
        ],
    )
    client = Client("test")
    events = client.get_events(device_id=device_id)
    assert len(events) == 1
    assert events[0]["device_id"] == device_id


@responses.activate
def test_get_devices():
    id = fake.uuid4()
    responses.add(
        responses.GET,
        "https://api.foxglove.dev/v1/devices",
        json=[
            {
                "id": id,
                "name": fake.sentence(2),
                "serialNumber": fake.pyint(),
                "createdAt": datetime.now().isoformat(),
                "updatedAt": datetime.now().isoformat(),
            }
        ],
    )
    client = Client("test")
    devices = client.get_devices()
    assert len(devices) == 1
    assert devices[0]["id"] == id


@responses.activate
def test_download():
    download_link = fake.url()
    responses.add(
        responses.POST,
        "https://api.foxglove.dev/v1/data/stream",
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
def test_upload():
    upload_link = fake.url()
    responses.add(
        responses.POST,
        "https://api.foxglove.dev/v1/data/upload",
        json={
            "link": upload_link,
        },
    )
    responses.add(responses.PUT, upload_link)
    client = Client("test")
    data = fake.binary(4096)
    upload_response = client.upload_data(
        device_id="test_device_id", filename="test_file.mcap", data=data
    )
    assert upload_response["link"] == upload_link


@responses.activate
def test_streaming_upload():
    upload_link = fake.url()
    responses.add(
        responses.POST,
        "https://api.foxglove.dev/v1/data/upload",
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
