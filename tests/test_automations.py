from datetime import datetime, timezone

import pytest
import responses
from responses.matchers import json_params_matcher, query_param_matcher

from foxglove.client import Client

from .api_url import api_url

NOW = datetime(2026, 10, 5, 9, tzinfo=timezone.utc)
PERMISSIONS = {"canEdit": True, "canRun": True, "canDelete": False}


def _automation_json(**overrides):
    return {
        "id": "aut_1",
        "projectId": "prj_1",
        "name": "Triage new recordings",
        "actionPrompt": "Summarize anomalies.",
        "isOwner": True,
        "permissions": PERMISSIONS,
        "createdAt": NOW.isoformat(),
        "updatedAt": NOW.isoformat(),
        **overrides,
    }


def _automation_result(**overrides):
    return {
        "id": "aut_1",
        "project_id": "prj_1",
        "name": "Triage new recordings",
        "action_prompt": "Summarize anomalies.",
        "is_owner": True,
        "permissions": PERMISSIONS,
        "created_at": NOW,
        "updated_at": NOW,
        **overrides,
    }


@responses.activate
def test_get_automations():
    responses.add(
        responses.GET,
        api_url("/v1/automations"),
        match=[
            query_param_matcher({"projectId": "prj_1", "limit": "10", "offset": "20"})
        ],
        json={
            "automations": [
                _automation_json(
                    lifetimeRunCount=3,
                    triggerEnabled=True,
                    lastRunAt=NOW.isoformat(),
                    lastRunStatus="succeeded",
                ),
                _automation_json(
                    id="aut_2",
                    isOwner=False,
                    lifetimeRunCount=0,
                    triggerEnabled=None,
                    lastRunAt=None,
                    lastRunStatus=None,
                ),
            ]
        },
    )
    automations = Client("test").get_automations(
        project_id="prj_1", limit=10, offset=20
    )
    assert automations == [
        _automation_result(
            lifetime_run_count=3,
            trigger_enabled=True,
            last_run_at=NOW,
            last_run_status="succeeded",
        ),
        _automation_result(
            id="aut_2",
            is_owner=False,
            lifetime_run_count=0,
            trigger_enabled=None,
            last_run_at=None,
            last_run_status=None,
        ),
    ]


@responses.activate
def test_get_automation_encodes_id():
    responses.add(
        responses.GET,
        api_url("/v1/automations/aut%2F1"),
        json=_automation_json(id="aut/1"),
    )
    assert Client("test").get_automation(automation_id="aut/1") == _automation_result(
        id="aut/1"
    )


@responses.activate
def test_create_automation():
    responses.add(
        responses.POST,
        api_url("/v1/automations"),
        status=201,
        match=[
            json_params_matcher(
                {
                    "projectId": "prj_1",
                    "name": "Triage new recordings",
                    "actionPrompt": "Summarize anomalies.",
                },
                strict_match=True,
            )
        ],
        json=_automation_json(),
    )
    automation = Client("test").create_automation(
        project_id="prj_1",
        name="Triage new recordings",
        action_prompt="Summarize anomalies.",
    )
    assert automation == _automation_result()


@responses.activate
def test_update_automation_sends_only_given_fields():
    responses.add(
        responses.PATCH,
        api_url("/v1/automations/aut_1"),
        match=[json_params_matcher({"name": "Renamed"}, strict_match=True)],
        json=_automation_json(name="Renamed"),
    )
    automation = Client("test").update_automation(automation_id="aut_1", name="Renamed")
    assert automation == _automation_result(name="Renamed")


def test_update_automation_requires_a_change():
    with pytest.raises(ValueError):
        Client("test").update_automation(automation_id="aut_1")


@responses.activate
def test_delete_automation():
    responses.add(
        responses.DELETE,
        api_url("/v1/automations/aut_1"),
        json={"success": True},
    )
    assert Client("test").delete_automation(automation_id="aut_1") is None
