import time
import datetime

import responses
from faker import Faker
from foxglove.client import Client
from responses.matchers import json_params_matcher

from .api_url import api_url

fake = Faker()


@responses.activate
def test_create_event():
    id = fake.uuid4()
    device_id = fake.uuid4()
    device_name = fake.name()
    start = datetime.datetime.now().astimezone()
    end = start + datetime.timedelta(seconds=10)
    now = datetime.datetime.now().astimezone()
    responses.add(
        responses.POST,
        api_url("/v1/events"),
        match=[
            json_params_matcher(
                {
                    "deviceId": device_id,
                    "start": start.astimezone().isoformat(),
                    "end": end.astimezone().isoformat(),
                    "metadata": {},
                },
            )
        ],
        json={
            "id": id,
            "deviceId": device_id,
            "device": {"id": device_id, "name": device_name},
            "start": start.astimezone().isoformat(),
            "end": end.astimezone().isoformat(),
            "createdAt": now.astimezone().isoformat(),
            "updatedAt": now.astimezone().isoformat(),
            "metadata": {},
        },
    )
    client = Client("test")
    event = client.create_event(device_id=device_id, start=start, end=end)
    assert event["start"] == start
    assert event["device_id"] == device_id
    assert event["device"] == {"id": device_id, "name": device_name}
    assert event["end"] == end
    assert event["id"] == id
    assert event["created_at"] == now
    assert event["updated_at"] == now
    assert event["metadata"] == {}


@responses.activate
def test_create_event_with_properties():
    id = fake.uuid4()
    device_id = fake.uuid4()
    device_name = fake.name()
    start = datetime.datetime.now().astimezone()
    end = start + datetime.timedelta(seconds=10)
    now = datetime.datetime.now().astimezone()
    event_type_id = "evtt_123"
    responses.add(
        responses.POST,
        api_url("/v1/events"),
        match=[
            json_params_matcher(
                {
                    "deviceId": device_id,
                    "start": start.astimezone().isoformat(),
                    "end": end.astimezone().isoformat(),
                    "metadata": {},
                    "eventTypeId": event_type_id,
                    "properties": {"key": "value"},
                },
            )
        ],
        json={
            "id": id,
            "deviceId": device_id,
            "device": {"id": device_id, "name": device_name},
            "start": start.astimezone().isoformat(),
            "end": end.astimezone().isoformat(),
            "metadata": {},
            "properties": {"key": "value"},
            "eventTypeId": event_type_id,
            "createdAt": now.astimezone().isoformat(),
            "updatedAt": now.astimezone().isoformat(),
            "eventTypeId": event_type_id,
        },
    )
    client = Client("test")
    event = client.create_event(
        device_id=device_id,
        start=start,
        end=end,
        properties={"key": "value"},
        event_type_id=event_type_id,
    )
    assert event["start"] == start
    assert event["device_id"] == device_id
    assert event["device"] == {"id": device_id, "name": device_name}
    assert event["end"] == end
    assert event["id"] == id
    assert event["metadata"] == {}
    assert event["created_at"] == now
    assert event["updated_at"] == now
    assert event["properties"] == {"key": "value"}
    assert event["event_type_id"] == event_type_id


@responses.activate
def test_delete_event():
    id = fake.uuid4()
    responses.add(responses.DELETE, api_url(f"/v1/events/{id}"), json={"id": id})
    client = Client("test")
    try:
        client.delete_event(event_id=id)
    except:
        assert False


@responses.activate
def test_update_event():
    event_id = fake.uuid4()
    device_id = fake.uuid4()
    device_name = fake.name()
    start = datetime.datetime.now().astimezone()
    end = start + datetime.timedelta(seconds=20)
    updated_at = datetime.datetime.now().astimezone()
    created_at = updated_at - datetime.timedelta(minutes=2)
    properties = {"key": "value", "number": 42, "to_remove": None}
    # does not contain the removed property
    new_properties = {"key": "value", "number": 42}
    event_type_id = "evtt_789"
    responses.add(
        responses.PATCH,
        api_url(f"/v1/events/{event_id}"),
        match=[
            json_params_matcher(
                {
                    "start": start.astimezone().isoformat(),
                    "end": end.astimezone().isoformat(),
                    "properties": properties,
                    "eventTypeId": event_type_id,
                },
            )
        ],
        json={
            "id": event_id,
            "deviceId": device_id,
            "device": {"id": device_id, "name": device_name},
            "start": start.astimezone().isoformat(),
            "end": end.astimezone().isoformat(),
            "createdAt": created_at.astimezone().isoformat(),
            "updatedAt": updated_at.astimezone().isoformat(),
            "metadata": {},
            "properties": new_properties,
            "eventTypeId": event_type_id,
        },
    )
    client = Client("test")
    event = client.update_event(
        event_id=event_id,
        start=start,
        end=end,
        properties=properties,
        event_type_id=event_type_id,
    )
    assert event["id"] == event_id
    assert event["start"] == start
    assert event["end"] == end
    assert event["metadata"] == {}
    assert event["properties"] == new_properties
    assert event["event_type_id"] == event_type_id
    assert event["created_at"] == created_at
    assert event["updated_at"] == updated_at


@responses.activate
def test_get_events():
    device_id = "my_device_id"
    device_name = "device_name"
    start = datetime.datetime.now().astimezone()
    end = start + datetime.timedelta(seconds=10)
    now = datetime.datetime.now().astimezone()
    project_id = "prj_123"
    responses.add(
        responses.GET,
        api_url(f"/v1/events?deviceId={device_id}&projectId={project_id}"),
        json=[
            {
                "id": "1",
                "deviceId": device_id,
                "device": {
                    "id": device_id,
                    "name": device_name,
                },
                "metadata": {},
                "start": start.astimezone().isoformat(),
                "end": end.astimezone().isoformat(),
                "createdAt": now.astimezone().isoformat(),
                "updatedAt": now.astimezone().isoformat(),
                "projectId": project_id,
            }
        ],
    )
    client = Client("test")
    [event] = client.get_events(device_id=device_id, project_id=project_id)
    assert event["id"] == "1"
    assert event["device_id"] == device_id
    assert event["device"] == {"id": device_id, "name": device_name}
    assert event["start"] == start
    assert event["end"] == end
    assert event["created_at"] == now
    assert event["updated_at"] == now
    assert event["metadata"] == {}
    assert event["properties"] is None
    assert event["event_type_id"] is None

    responses.add(
        responses.GET,
        api_url(f"/v1/events?deviceName={device_name}&projectId={project_id}"),
        json=[
            {
                "id": "2",
                "deviceId": device_id,
                "device": {
                    "id": device_id,
                    "name": device_name,
                },
                "metadata": {},
                "properties": {"key": "value"},
                "eventTypeId": "evtt_123",
                "start": start.astimezone().isoformat(),
                "end": end.astimezone().isoformat(),
                "createdAt": now.astimezone().isoformat(),
                "updatedAt": now.astimezone().isoformat(),
                "projectId": project_id,
            }
        ],
    )
    client = Client("test")
    [event] = client.get_events(device_name=device_name, project_id=project_id)
    assert event["id"] == "2"
    assert event["device_id"] == device_id
    assert event["device"] == {"id": device_id, "name": device_name}
    assert event["start"] == start
    assert event["end"] == end
    assert event["created_at"] == now
    assert event["updated_at"] == now
    assert event["metadata"] == {}
    assert event["properties"] == {"key": "value"}
    assert event["event_type_id"] == "evtt_123"
