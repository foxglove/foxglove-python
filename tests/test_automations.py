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


TOKEN_USAGE = {
    "inputTokens": 120,
    "outputTokens": 40,
    "cacheReadTokens": 0,
    "cacheWriteTokens": 8,
}
TRIGGER_SOURCE = {
    "kind": "recording",
    "id": "rec_1",
    "projectId": "prj_1",
    "available": True,
}


def _run_json(**overrides):
    return {
        "id": "run_1",
        "automationId": "aut_1",
        "actionPrompt": "Summarize anomalies.",
        "runPrompt": "Look at rec_1.",
        "status": "pending",
        "createdAt": NOW.isoformat(),
        "updatedAt": NOW.isoformat(),
        "startedAt": None,
        "completedAt": None,
        "errorMessage": None,
        **overrides,
    }


def _run_result(**overrides):
    return {
        "id": "run_1",
        "automation_id": "aut_1",
        "action_prompt": "Summarize anomalies.",
        "run_prompt": "Look at rec_1.",
        "status": "pending",
        "created_at": NOW,
        "updated_at": NOW,
        "started_at": None,
        "completed_at": None,
        "error_message": None,
        **overrides,
    }


@responses.activate
def test_create_automation_run():
    responses.add(
        responses.POST,
        api_url("/v1/automations/runs"),
        status=201,
        match=[
            json_params_matcher(
                {"automationId": "aut_1", "runPrompt": "Look at rec_1."},
                strict_match=True,
            )
        ],
        json=_run_json(),
    )
    run = Client("test").create_automation_run(
        automation_id="aut_1", run_prompt="Look at rec_1."
    )
    assert run == _run_result()


@responses.activate
def test_create_automation_run_without_run_prompt():
    responses.add(
        responses.POST,
        api_url("/v1/automations/runs"),
        status=201,
        match=[json_params_matcher({"automationId": "aut_1"}, strict_match=True)],
        json=_run_json(runPrompt=""),
    )
    run = Client("test").create_automation_run(automation_id="aut_1")
    assert run == _run_result(run_prompt="")


@responses.activate
def test_get_automation_runs():
    created_after = datetime(2026, 10, 1, tzinfo=timezone.utc)
    created_before = datetime(2026, 10, 6, tzinfo=timezone.utc)
    schedule = {"cadence": "daily", "anchor": "2026-10-05T09:00:00Z"}
    responses.add(
        responses.GET,
        api_url("/v1/automations/runs"),
        match=[
            query_param_matcher(
                {
                    "projectId": "prj_1",
                    "automationId": "aut_1,aut_2",
                    "status": "succeeded,failed",
                    "createdAfter": created_after.astimezone().isoformat(),
                    "createdBefore": created_before.astimezone().isoformat(),
                    "sortBy": "createdAt",
                    "sortOrder": "asc",
                    "ownerMemberId": "me",
                    "limit": "5",
                    "offset": "10",
                }
            )
        ],
        json={
            "runs": [
                {
                    "id": "run_1",
                    "automationId": "aut_1",
                    "status": "succeeded",
                    "createdAt": NOW.isoformat(),
                    "updatedAt": NOW.isoformat(),
                    "startedAt": NOW.isoformat(),
                    "completedAt": NOW.isoformat(),
                    "errorMessage": None,
                    "automationName": "Triage new recordings",
                    "triggerEventType": "clock.updated",
                    "triggerSource": None,
                    "triggerSchedule": schedule,
                    "tokenUsage": TOKEN_USAGE,
                },
                {
                    "id": "run_2",
                    "automationId": "aut_2",
                    "status": "cancelled",
                    "createdAt": NOW.isoformat(),
                    "updatedAt": NOW.isoformat(),
                    "startedAt": None,
                    "completedAt": None,
                    "errorMessage": "Stopped",
                    "automationName": "Tag events",
                    "triggerEventType": "recording.created",
                    "triggerSource": TRIGGER_SOURCE,
                    "triggerSchedule": None,
                    "tokenUsage": None,
                },
            ]
        },
    )
    runs = Client("test").get_automation_runs(
        project_id="prj_1",
        automation_ids=["aut_1", "aut_2"],
        statuses=["succeeded", "failed"],
        created_after=created_after,
        created_before=created_before,
        sort_by="created_at",
        sort_order="asc",
        owner_member_id="me",
        limit=5,
        offset=10,
    )
    assert runs == [
        {
            "id": "run_1",
            "automation_id": "aut_1",
            "status": "succeeded",
            "created_at": NOW,
            "updated_at": NOW,
            "started_at": NOW,
            "completed_at": NOW,
            "error_message": None,
            "automation_name": "Triage new recordings",
            "trigger_event_type": "clock.updated",
            "trigger_source": None,
            "trigger_schedule": schedule,
            "token_usage": TOKEN_USAGE,
        },
        {
            "id": "run_2",
            "automation_id": "aut_2",
            "status": "cancelled",
            "created_at": NOW,
            "updated_at": NOW,
            "started_at": None,
            "completed_at": None,
            "error_message": "Stopped",
            "automation_name": "Tag events",
            "trigger_event_type": "recording.created",
            "trigger_source": TRIGGER_SOURCE,
            "trigger_schedule": None,
            "token_usage": None,
        },
    ]


@responses.activate
def test_get_automation_runs_sends_only_project_by_default():
    responses.add(
        responses.GET,
        api_url("/v1/automations/runs"),
        match=[query_param_matcher({"projectId": "prj_1"})],
        json={"runs": []},
    )
    assert Client("test").get_automation_runs(project_id="prj_1") == []


@responses.activate
def test_get_automation_run():
    responses.add(
        responses.GET,
        api_url("/v1/automations/runs/run%2F1"),
        json=_run_json(
            id="run/1",
            status="succeeded",
            startedAt=NOW.isoformat(),
            completedAt=NOW.isoformat(),
            automationName="Triage new recordings",
            finalAnswer="No anomalies found.",
            trigger=_trigger_json(
                eventType="recording.created",
                configuration={},
                deletedAt=NOW.isoformat(),
            ),
            triggerSource=TRIGGER_SOURCE,
            tokenUsage=TOKEN_USAGE,
        ),
    )
    run = Client("test").get_automation_run(run_id="run/1")
    assert run == _run_result(
        id="run/1",
        status="succeeded",
        started_at=NOW,
        completed_at=NOW,
        automation_name="Triage new recordings",
        final_answer="No anomalies found.",
        trigger=_trigger_result(
            event_type="recording.created", configuration={}, deleted_at=NOW
        ),
        trigger_source=TRIGGER_SOURCE,
        token_usage=TOKEN_USAGE,
    )


@responses.activate
def test_get_manual_automation_run_in_progress():
    responses.add(
        responses.GET,
        api_url("/v1/automations/runs/run_1"),
        json=_run_json(
            status="running",
            startedAt=NOW.isoformat(),
            automationName="Triage new recordings",
            finalAnswer=None,
            trigger=None,
            triggerSource=None,
            tokenUsage=None,
        ),
    )
    run = Client("test").get_automation_run(run_id="run_1")
    assert run == _run_result(
        status="running",
        started_at=NOW,
        automation_name="Triage new recordings",
        final_answer=None,
        trigger=None,
        trigger_source=None,
        token_usage=None,
    )
