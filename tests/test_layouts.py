from datetime import datetime

import responses
from dateutil.tz import tzoffset
from responses.matchers import json_params_matcher, query_param_matcher

from foxglove.client import Client

from .api_url import api_url

now = datetime.now(tzoffset(None, 0))
data = {"configById": {"3D!a": {}}, "layout": "3D!a"}


def _layout_json(**overrides):
    return {
        "id": "lyt_1",
        "name": "Debug",
        "createdAt": now.isoformat(),
        "updatedAt": now.isoformat(),
        "savedAt": now.isoformat(),
        "folderName": "Team",
        "permission": "ORG_WRITE",
        "data": data,
        **overrides,
    }


@responses.activate
def test_get_layouts():
    updated_since = datetime(2026, 1, 1, tzinfo=tzoffset(None, 0))
    responses.add(
        responses.GET,
        api_url("/v1/layouts"),
        match=[
            query_param_matcher(
                {
                    "updatedSince": updated_since.astimezone().isoformat(),
                    "includeData": "false",
                }
            )
        ],
        json=[
            {
                "id": "lyt_1",
                "name": "Debug",
                "createdAt": now.isoformat(),
                "updatedAt": now.isoformat(),
                "permission": "ORG_READ",
            }
        ],
    )
    layouts = Client("test").get_layouts(
        updated_since=updated_since, include_data=False
    )
    assert layouts == [
        {
            "id": "lyt_1",
            "name": "Debug",
            "created_at": now,
            "updated_at": now,
            "saved_at": None,
            "folder_name": None,
            "permission": "ORG_READ",
            "data": None,
        }
    ]


@responses.activate
def test_get_layout():
    responses.add(
        responses.GET,
        api_url("/v1/layouts/lyt_1"),
        match=[query_param_matcher({"includeData": "true"})],
        json=_layout_json(),
    )
    assert Client("test").get_layout(layout_id="lyt_1", include_data=True) == {
        "id": "lyt_1",
        "name": "Debug",
        "created_at": now,
        "updated_at": now,
        "saved_at": now,
        "folder_name": "Team",
        "permission": "ORG_WRITE",
        "data": data,
    }


@responses.activate
def test_create_layout():
    responses.add(
        responses.POST,
        api_url("/v1/layouts"),
        match=[
            json_params_matcher(
                {
                    "id": "lyt_1",
                    "name": "Debug",
                    "savedAt": now.astimezone().isoformat(),
                    "folderName": "Team",
                    "permission": "ORG_WRITE",
                    "data": data,
                },
                strict_match=True,
            )
        ],
        json=_layout_json(),
    )
    layout = Client("test").create_layout(
        layout_id="lyt_1",
        name="Debug",
        saved_at=now,
        folder_name="Team",
        permission="ORG_WRITE",
        data=data,
    )
    assert layout["id"] == "lyt_1"
    assert layout["data"] == data


@responses.activate
def test_update_layout_sends_only_set_fields_for_org_api_key():
    responses.add(
        responses.PATCH,
        api_url("/v1/layouts/lyt_1"),
        match=[
            json_params_matcher(
                {"folderName": "", "permission": "ORG_WRITE"}, strict_match=True
            )
        ],
        json=_layout_json(permission="ORG_WRITE", folderName=None),
    )
    layout = Client("test-org-api-key").update_layout(
        layout_id="lyt_1", folder_name="", permission="ORG_WRITE"
    )
    assert layout["permission"] == "ORG_WRITE"
    assert layout["folder_name"] is None


@responses.activate
def test_delete_layout():
    responses.add(
        responses.DELETE, api_url("/v1/layouts/lyt_1"), json={"success": True}
    )
    Client("test").delete_layout(layout_id="lyt_1")
    assert len(responses.calls) == 1


@responses.activate
def test_restore_layout():
    responses.add(
        responses.POST, api_url("/v1/layouts/lyt_1/restore"), json={"success": True}
    )
    Client("test").restore_layout(layout_id="lyt_1")
    assert len(responses.calls) == 1
