from datetime import datetime

import responses
from dateutil.tz import tzoffset
from responses.matchers import json_params_matcher

from foxglove.client import Client

from .api_url import api_url

now = datetime.now(tzoffset(None, 0))
custom_properties = [{"id": "cpp_1", "required": True}]


def _event_type_json(**overrides):
    return {
        "id": "evtt_1",
        "name": "Collision",
        "colorName": "red",
        "customProperties": custom_properties,
        "createdAt": now.isoformat(),
        "updatedAt": now.isoformat(),
        **overrides,
    }


@responses.activate
def test_get_event_types():
    responses.add(
        responses.GET,
        api_url("/v1/event-types"),
        json=[_event_type_json(), _event_type_json(id="evtt_2", colorName=None)],
    )
    event_types = Client("test").get_event_types()
    assert event_types[0] == {
        "id": "evtt_1",
        "name": "Collision",
        "color_name": "red",
        "custom_properties": custom_properties,
        "created_at": now,
        "updated_at": now,
    }
    assert event_types[1]["color_name"] is None


@responses.activate
def test_get_event_type():
    responses.add(
        responses.GET, api_url("/v1/event-types/evtt_1"), json=_event_type_json()
    )
    assert Client("test").get_event_type(event_type_id="evtt_1")["name"] == "Collision"


@responses.activate
def test_create_event_type():
    responses.add(
        responses.POST,
        api_url("/v1/event-types"),
        match=[
            json_params_matcher(
                {"name": "Collision", "customProperties": custom_properties},
                strict_match=True,
            )
        ],
        json=_event_type_json(colorName=None),
    )
    event_type = Client("test").create_event_type(
        name="Collision", custom_properties=custom_properties
    )
    assert event_type["id"] == "evtt_1"
    assert event_type["custom_properties"] == custom_properties


@responses.activate
def test_update_event_type_sends_only_set_fields():
    responses.add(
        responses.PATCH,
        api_url("/v1/event-types/evtt_1"),
        match=[json_params_matcher({"colorName": "blue"}, strict_match=True)],
        json=_event_type_json(colorName="blue"),
    )
    event_type = Client("test").update_event_type(
        event_type_id="evtt_1", color_name="blue"
    )
    assert event_type["color_name"] == "blue"


@responses.activate
def test_delete_event_type():
    responses.add(
        responses.DELETE, api_url("/v1/event-types/evtt_1"), json={"id": "evtt_1"}
    )
    Client("test").delete_event_type(event_type_id="evtt_1")
    assert len(responses.calls) == 1
