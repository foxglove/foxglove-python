from datetime import datetime, timedelta, timezone

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


def _trigger_json(**overrides):
    return {
        "id": "trg_1",
        "automationId": "aut_1",
        "eventType": "clock.updated",
        "enabled": True,
        "configurationVersion": 1,
        "configuration": {
            "schedule": {"cadence": "daily", "anchor": "2026-10-05T09:00:00Z"}
        },
        "createdAt": NOW.isoformat(),
        "updatedAt": NOW.isoformat(),
        "deletedAt": None,
        **overrides,
    }


def _trigger_result(**overrides):
    return {
        "id": "trg_1",
        "automation_id": "aut_1",
        "event_type": "clock.updated",
        "enabled": True,
        "configuration_version": 1,
        "configuration": {
            "schedule": {"cadence": "daily", "anchor": "2026-10-05T09:00:00Z"}
        },
        "created_at": NOW,
        "updated_at": NOW,
        "deleted_at": None,
        **overrides,
    }


@responses.activate
def test_get_automation_trigger():
    responses.add(
        responses.GET,
        api_url("/v1/automations/aut%2F1/trigger"),
        json=_trigger_json(),
    )
    trigger = Client("test").get_automation_trigger(automation_id="aut/1")
    assert trigger == _trigger_result()


@responses.activate
def test_set_automation_trigger_converts_schedule_anchor_to_utc():
    responses.add(
        responses.PUT,
        api_url("/v1/automations/aut_1/trigger"),
        status=201,
        match=[
            json_params_matcher(
                {
                    "eventType": "clock.updated",
                    "enabled": False,
                    "configuration": {
                        "schedule": {
                            "cadence": "daily",
                            "anchor": "2026-10-05T09:00:00Z",
                        }
                    },
                },
                strict_match=True,
            )
        ],
        json=_trigger_json(enabled=False),
    )
    anchor = datetime(2026, 10, 5, 11, tzinfo=timezone(timedelta(hours=2)))
    trigger = Client("test").set_automation_trigger(
        automation_id="aut_1",
        event_type="clock.updated",
        enabled=False,
        schedule={"cadence": "daily", "anchor": anchor},
    )
    assert trigger == _trigger_result(enabled=False)


@responses.activate
def test_set_automation_trigger_for_event_type():
    configuration = {"eventTypeId": "evt_type_1"}
    responses.add(
        responses.PUT,
        api_url("/v1/automations/aut_1/trigger"),
        status=201,
        match=[
            json_params_matcher(
                {"eventType": "event.created", "configuration": configuration},
                strict_match=True,
            )
        ],
        json=_trigger_json(eventType="event.created", configuration=configuration),
    )
    trigger = Client("test").set_automation_trigger(
        automation_id="aut_1",
        event_type="event.created",
        event_type_id="evt_type_1",
    )
    assert trigger == _trigger_result(
        event_type="event.created", configuration=configuration
    )


@responses.activate
def test_set_automation_trigger_without_configuration():
    responses.add(
        responses.PUT,
        api_url("/v1/automations/aut_1/trigger"),
        status=201,
        match=[
            json_params_matcher({"eventType": "recording.created"}, strict_match=True)
        ],
        json=_trigger_json(eventType="recording.created", configuration={}),
    )
    trigger = Client("test").set_automation_trigger(
        automation_id="aut_1", event_type="recording.created"
    )
    assert trigger["event_type"] == "recording.created"
    assert trigger["configuration"] == {}


@responses.activate
def test_set_automation_trigger_accepts_anchor_string():
    schedule = {"cadence": "monthly", "anchor": "2026-10-28T00:00:00Z"}
    responses.add(
        responses.PUT,
        api_url("/v1/automations/aut_1/trigger"),
        status=201,
        match=[
            json_params_matcher(
                {"eventType": "clock.updated", "configuration": {"schedule": schedule}},
                strict_match=True,
            )
        ],
        json=_trigger_json(configuration={"schedule": schedule}),
    )
    trigger = Client("test").set_automation_trigger(
        automation_id="aut_1", event_type="clock.updated", schedule=schedule
    )
    assert trigger["configuration"] == {"schedule": schedule}


@responses.activate
def test_delete_automation_trigger():
    responses.add(
        responses.DELETE,
        api_url("/v1/automations/aut_1/trigger"),
        json={"success": True},
    )
    assert Client("test").delete_automation_trigger(automation_id="aut_1") is None
