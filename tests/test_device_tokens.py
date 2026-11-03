from datetime import datetime

import responses
from dateutil.tz import tzoffset
from responses.matchers import json_params_matcher, query_param_matcher

from foxglove.client import Client

from .api_url import api_url

now = datetime.now(tzoffset(None, 0))


def _device_token_json(**overrides):
    return {
        "id": "dt_1",
        "createdAt": now.isoformat(),
        "deviceId": "dev_1",
        "projectId": "prj_1",
        "enabled": True,
        "preapproved": False,
        **overrides,
    }


@responses.activate
def test_get_device_tokens():
    responses.add(
        responses.GET,
        api_url("/v1/device-tokens"),
        match=[query_param_matcher({"deviceId": "dev_1"})],
        json=[_device_token_json(), _device_token_json(id="dt_2", deviceId=None)],
    )
    device_tokens = Client("test").get_device_tokens(device_id="dev_1")
    assert device_tokens[0] == {
        "id": "dt_1",
        "created_at": now,
        "device_id": "dev_1",
        "project_id": "prj_1",
        "enabled": True,
        "preapproved": False,
    }
    assert device_tokens[1]["device_id"] is None


@responses.activate
def test_get_device_token():
    responses.add(
        responses.GET, api_url("/v1/device-tokens/dt_1"), json=_device_token_json()
    )
    device_token = Client("test").get_device_token(device_token_id="dt_1")
    assert device_token["id"] == "dt_1"
    assert "token" not in device_token


@responses.activate
def test_create_device_token_returns_secret():
    responses.add(
        responses.POST,
        api_url("/v1/device-tokens"),
        match=[
            json_params_matcher(
                {"projectId": "prj_1", "preapproved": True}, strict_match=True
            )
        ],
        json=_device_token_json(deviceId=None, preapproved=True, token="fox_dt_secret"),
    )
    device_token = Client("test").create_device_token(
        project_id="prj_1", preapproved=True
    )
    assert device_token == {
        "id": "dt_1",
        "created_at": now,
        "device_id": None,
        "project_id": "prj_1",
        "enabled": True,
        "preapproved": True,
        "token": "fox_dt_secret",
    }


@responses.activate
def test_update_device_token():
    responses.add(
        responses.PATCH,
        api_url("/v1/device-tokens/dt_1"),
        match=[json_params_matcher({"enabled": False}, strict_match=True)],
        json=_device_token_json(enabled=False),
    )
    device_token = Client("test").update_device_token(
        device_token_id="dt_1", enabled=False
    )
    assert device_token["enabled"] is False


@responses.activate
def test_delete_device_token():
    responses.add(
        responses.DELETE, api_url("/v1/device-tokens/dt_1"), json={"id": "dt_1"}
    )
    Client("test").delete_device_token(device_token_id="dt_1")
    assert len(responses.calls) == 1
