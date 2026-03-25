import json
from datetime import datetime
from urllib.parse import parse_qs, quote, urlparse

import pytest
import responses
from faker import Faker

from foxglove.client import Client

from .api_url import api_url

fake = Faker()


@responses.activate
def test_get_device_custom_property_time_interval_quotes_path_and_passes_project_id():
    device_name = "Device / Name"
    time_interval_id = "dcph/id"
    now = datetime.now()
    responses.add(
        responses.GET,
        api_url(
            f"/v1/devices/{quote(device_name, safe='')}/property-time-intervals/"
            f"{quote(time_interval_id, safe='')}"
        ),
        json={
            "id": time_interval_id,
            "deviceId": fake.uuid4(),
            "key": "env",
            "value": "prod",
            "start": now.isoformat(),
            "end": now.isoformat(),
        },
    )

    client = Client("test")
    response = client.get_device_custom_property_time_interval(
        device_name=device_name,
        project_id="project-id",
        id=time_interval_id,
    )

    assert response["id"] == time_interval_id
    assert parse_qs(urlparse(responses.calls[0].request.url).query) == {
        "projectId": ["project-id"]
    }


def test_get_device_custom_property_time_interval_rejects_multiple_device_selectors():
    client = Client("test")

    with pytest.raises(RuntimeError) as exception:
        client.get_device_custom_property_time_interval(
            device_id="device-id",
            device_name="device-name",
            id="time-interval-id",
        )

    assert str(exception.value) == "device_id and device_name are mutually exclusive"


@responses.activate
def test_get_device_custom_property_time_intervals_uses_path_selector_only():
    device_name = "Device / Name"
    responses.add(
        responses.GET,
        api_url(f"/v1/devices/{quote(device_name, safe='')}/property-time-intervals"),
        json=[],
    )

    client = Client("test")
    client.get_device_custom_property_time_intervals(
        device_name=device_name,
        project_id="project-id",
        key="env",
        limit=5,
        offset=10,
    )

    assert parse_qs(urlparse(responses.calls[0].request.url).query) == {
        "key": ["env"],
        "limit": ["5"],
        "offset": ["10"],
        "projectId": ["project-id"],
    }


@responses.activate
def test_get_device_custom_property_time_intervals_supports_multiple_keys():
    responses.add(
        responses.GET,
        api_url("/v1/devices/device-id/property-time-intervals"),
        json=[],
    )

    client = Client("test")
    client.get_device_custom_property_time_intervals(
        device_id="device-id",
        project_id="project-id",
        key=["env", "region"],
    )

    assert parse_qs(urlparse(responses.calls[0].request.url).query) == {
        "key": ["env,region"],
        "projectId": ["project-id"],
    }


@responses.activate
def test_update_device_custom_property_time_intervals_omits_value_for_clear():
    device_name = "Device / Name"
    responses.add(
        responses.POST,
        api_url(
            f"/v1/actions/devices/{quote(device_name, safe='')}/update-property-time-interval"
        ),
        status=204,
        body="",
    )

    client = Client("test")
    client.update_device_custom_property_time_interval(
        device_name=device_name,
        project_id="project-id",
        key="env",
        start=datetime.now(),
        end=datetime.now(),
    )

    request_body = json.loads(responses.calls[0].request.body)
    assert request_body["projectId"] == "project-id"
    assert request_body["key"] == "env"
    assert "value" not in request_body
    assert "deviceName" not in request_body
    assert "deviceId" not in request_body


@responses.activate
def test_update_device_custom_property_time_intervals_supports_array_values():
    responses.add(
        responses.POST,
        api_url("/v1/actions/devices/device-id/update-property-time-interval"),
        status=204,
        body="",
    )

    client = Client("test")
    client.update_device_custom_property_time_interval(
        device_id="device-id",
        key="labels",
        value=["one", "two"],
        start=datetime.now(),
        end=datetime.now(),
    )

    request_body = json.loads(responses.calls[0].request.body)
    assert request_body["value"] == ["one", "two"]
    assert "deviceName" not in request_body
    assert "deviceId" not in request_body
