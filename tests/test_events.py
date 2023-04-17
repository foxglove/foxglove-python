import time
import datetime

import responses
from faker import Faker
from foxglove_data_platform.client import Client

from .api_url import api_url

fake = Faker()


@responses.activate
def test_create_event():
    id = fake.uuid4()
    device_id = fake.uuid4()
    start = datetime.datetime.now().astimezone()
    end = start + datetime.timedelta(seconds=10)
    now = datetime.datetime.now().astimezone()
    responses.add(
        responses.POST,
        api_url("/v1/events"),
        json={
            "id": id,
            "start": start.astimezone().isoformat(),
            "end": end.astimezone().isoformat(),
            "metadata": {"foo": "bar"},
            "createdAt": now.astimezone().isoformat(),
            "updatedAt": now.astimezone().isoformat(),
        },
    )
    client = Client("test")
    event = client.create_event(device_id=device_id, start=start, end=end)
    assert event["start"] == start
    assert event["end"] == end
    assert event["id"] == id
    assert event["created_at"] == now
    assert event["updated_at"] == now


@responses.activate
def test_delete_event():
    id = fake.uuid4()
    responses.add(
        responses.DELETE, api_url(f"/v1/events/{id}"), json={"id": id}
    )
    client = Client("test")
    try:
        client.delete_event(event_id=id)
    except:
        assert False


@responses.activate
def test_get_events():
    device_id = "my_device_id"
    responses.add(
        responses.GET,
        api_url(f"/beta/device-events?deviceId={device_id}"),
        json=[
            {
                "id": "1",
                "createdAt": datetime.datetime.now().isoformat(),
                "deviceId": device_id,
                "durationNanos": fake.pyint(),
                "metadata": {},
                "timestampNanos": fake.pyint(),
                "updatedAt": datetime.datetime.now().isoformat(),
            }
        ],
    )
    client = Client("test")
    events = client.get_events(device_id=device_id)
    assert len(events) == 1
    assert events[0]["device_id"] == device_id
