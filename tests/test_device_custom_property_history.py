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
def test_get_device_custom_property_history_quotes_path_and_passes_project_id():
    device_name = "Device / Name"
    property_history_id = "dcph/id"
    now = datetime.now()
    responses.add(
        responses.GET,
        api_url(
            f"/v1/devices/{quote(device_name, safe='')}/property-history/"
            f"{quote(property_history_id, safe='')}"
        ),
        json={
            "id": property_history_id,
            "deviceId": fake.uuid4(),
            "key": "env",
            "value": "prod",
            "start": now.isoformat(),
            "end": now.isoformat(),
        },
    )

    client = Client("test")
    response = client.get_device_custom_property_history(
        device_name=device_name,
        project_id="project-id",
        id=property_history_id,
    )

    assert response["id"] == property_history_id
    assert parse_qs(urlparse(responses.calls[0].request.url).query) == {
        "projectId": ["project-id"]
    }


def test_get_device_custom_property_history_rejects_multiple_device_selectors():
    client = Client("test")

    with pytest.raises(RuntimeError) as exception:
        client.get_device_custom_property_history(
            device_id="device-id",
            device_name="device-name",
            id="history-id",
        )

    assert str(exception.value) == "device_id and device_name are mutually exclusive"


@responses.activate
def test_get_device_custom_property_history_records_uses_path_selector_only():
    device_name = "Device / Name"
    responses.add(
        responses.GET,
        api_url(f"/v1/devices/{quote(device_name, safe='')}/property-history"),
        json=[],
    )

    client = Client("test")
    client.get_device_custom_property_history_records(
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
def test_update_device_custom_property_history_omits_value_for_clear():
    responses.add(
        responses.POST,
        api_url("/v1/actions/devices/update-device-property-history"),
        status=204,
        body="",
    )

    client = Client("test")
    client.update_device_custom_property_history(
        device_name="Device / Name",
        project_id="project-id",
        key="env",
        start=datetime.now(),
        end=datetime.now(),
    )

    request_body = json.loads(responses.calls[0].request.body)
    assert request_body["deviceName"] == "Device / Name"
    assert request_body["projectId"] == "project-id"
    assert request_body["key"] == "env"
    assert "value" not in request_body
    assert "deviceId" not in request_body


@responses.activate
def test_update_device_custom_property_history_supports_array_values():
    responses.add(
        responses.POST,
        api_url("/v1/actions/devices/update-device-property-history"),
        status=204,
        body="",
    )

    client = Client("test")
    client.update_device_custom_property_history(
        device_id="device-id",
        key="labels",
        value=["one", "two"],
        start=datetime.now(),
        end=datetime.now(),
    )

    request_body = json.loads(responses.calls[0].request.body)
    assert request_body["deviceId"] == "device-id"
    assert request_body["value"] == ["one", "two"]
    assert "deviceName" not in request_body
