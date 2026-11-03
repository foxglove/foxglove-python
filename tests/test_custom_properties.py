import responses
from responses.matchers import json_params_matcher, query_param_matcher

from foxglove.client import Client

from .api_url import api_url


def _custom_property_json(**overrides):
    return {
        "id": "cp_1",
        "key": "partNumber",
        "label": "Part number",
        "resourceType": "device",
        "valueType": "enum",
        "enumValues": ["a", "b"],
        "hasAssociatedData": False,
        **overrides,
    }


@responses.activate
def test_get_custom_properties():
    responses.add(
        responses.GET,
        api_url("/v1/custom-properties"),
        match=[query_param_matcher({"resourceType": "device"})],
        json=[
            _custom_property_json(),
            {
                "id": "cp_2",
                "key": "notes",
                "label": "Notes",
                "resourceType": "device",
                "valueType": "string",
            },
        ],
    )
    custom_properties = Client("test").get_custom_properties(resource_type="device")
    assert custom_properties[0] == {
        "id": "cp_1",
        "key": "partNumber",
        "label": "Part number",
        "resource_type": "device",
        "value_type": "enum",
        "enum_values": ["a", "b"],
        "has_associated_data": False,
    }
    assert custom_properties[1]["enum_values"] is None
    assert custom_properties[1]["has_associated_data"] is None


@responses.activate
def test_get_custom_property():
    responses.add(
        responses.GET,
        api_url("/v1/custom-properties/cp_1"),
        json=_custom_property_json(),
    )
    custom_property = Client("test").get_custom_property(custom_property_id="cp_1")
    assert custom_property["key"] == "partNumber"


@responses.activate
def test_create_custom_property():
    responses.add(
        responses.POST,
        api_url("/v1/custom-properties"),
        match=[
            json_params_matcher(
                {
                    "key": "partNumber",
                    "label": "Part number",
                    "resourceType": "device",
                    "valueType": "enum",
                    "enumValues": ["a", "b"],
                },
                strict_match=True,
            )
        ],
        json=_custom_property_json(),
    )
    custom_property = Client("test").create_custom_property(
        key="partNumber",
        label="Part number",
        resource_type="device",
        value_type="enum",
        enum_values=["a", "b"],
    )
    assert custom_property["id"] == "cp_1"
    assert custom_property["value_type"] == "enum"


@responses.activate
def test_update_custom_property_sends_only_set_fields():
    responses.add(
        responses.PATCH,
        api_url("/v1/custom-properties/cp_1"),
        match=[json_params_matcher({"label": "Part"}, strict_match=True)],
        json=_custom_property_json(label="Part"),
    )
    custom_property = Client("test").update_custom_property(
        custom_property_id="cp_1", label="Part"
    )
    assert custom_property["label"] == "Part"


@responses.activate
def test_delete_custom_property():
    responses.add(
        responses.DELETE,
        api_url("/v1/custom-properties/cp_1"),
        json={"id": "cp_1"},
    )
    Client("test").delete_custom_property(custom_property_id="cp_1")
    assert len(responses.calls) == 1
