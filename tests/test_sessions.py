import datetime

import arrow
import responses
from faker import Faker
from foxglove.client import Client
from responses.matchers import json_params_matcher, query_string_matcher

from .api_url import api_url

fake = Faker()

NOW = datetime.datetime.now().astimezone()


def _make_session_json(
    *,
    session_id=None,
    project_id=None,
    device_id=None,
    device_name=None,
    key=None,
    recording_ids=None,
):
    session_id = session_id or fake.uuid4()
    project_id = project_id or fake.uuid4()
    device_id = device_id or fake.uuid4()
    device_name = device_name or fake.name()
    key = key or fake.slug()
    return {
        "id": session_id,
        "projectId": project_id,
        "device": {"id": device_id, "name": device_name},
        "key": key,
        "createdAt": NOW.isoformat(),
        "updatedAt": NOW.isoformat(),
        "recordings": [{"id": rid} for rid in (recording_ids or [])],
    }


@responses.activate
def test_get_sessions():
    project_id = fake.uuid4()
    s1 = _make_session_json()
    s2 = _make_session_json()
    responses.add(
        responses.GET,
        api_url("/v1/sessions"),
        match=[
            query_string_matcher(f"projectId={project_id}"),
        ],
        json=[s1, s2],
    )
    client = Client("test")
    result = client.get_sessions(project_id=project_id)
    assert len(result) == 2
    assert result[0]["id"] == s1["id"]
    assert result[0]["project_id"] == s1["projectId"]
    assert result[0]["device"] == s1["device"]
    assert result[0]["key"] == s1["key"]
    assert result[0]["created_at"] == arrow.get(s1["createdAt"]).datetime
    assert result[0]["updated_at"] == arrow.get(s1["updatedAt"]).datetime
    assert result[0]["recordings"] == s1["recordings"]


@responses.activate
def test_get_session():
    session_id = fake.uuid4()
    project_id = fake.uuid4()
    s = _make_session_json(session_id=session_id, project_id=project_id)
    responses.add(
        responses.GET,
        api_url(f"/v1/sessions/{session_id}"),
        match=[
            query_string_matcher(f"projectId={project_id}"),
        ],
        json=s,
    )
    client = Client("test")
    result = client.get_session(session_id=session_id, project_id=project_id)
    assert result["id"] == session_id
    assert result["project_id"] == project_id


@responses.activate
def test_get_session_by_key():
    session_key = fake.slug()
    project_id = fake.uuid4()
    s = _make_session_json(key=session_key, project_id=project_id)
    responses.add(
        responses.GET,
        api_url(f"/v1/sessions/{session_key}"),
        match=[
            query_string_matcher(f"projectId={project_id}"),
        ],
        json=s,
    )
    client = Client("test")
    result = client.get_session(session_key=session_key, project_id=project_id)
    assert result["key"] == session_key
    assert result["project_id"] == project_id


@responses.activate
def test_create_session():
    device_id = fake.uuid4()
    key = "test-session"
    s = _make_session_json(device_id=device_id, key=key)
    responses.add(
        responses.POST,
        api_url("/v1/sessions"),
        match=[
            json_params_matcher({"deviceId": device_id, "key": key}),
        ],
        json=s,
    )
    client = Client("test")
    result = client.create_session(device_id=device_id, key=key)
    assert result["key"] == key


@responses.activate
def test_update_session():
    session_id = fake.uuid4()
    project_id = fake.uuid4()
    add_ids = [fake.uuid4()]
    s = _make_session_json(
        session_id=session_id,
        project_id=project_id,
        recording_ids=add_ids,
    )
    responses.add(
        responses.PATCH,
        api_url(f"/v1/sessions/{session_id}"),
        match=[
            query_string_matcher(f"projectId={project_id}"),
            json_params_matcher({"addRecordingIds": add_ids}),
        ],
        json=s,
    )
    client = Client("test")
    result = client.update_session(
        session_id=session_id,
        project_id=project_id,
        add_recording_ids=add_ids,
    )
    assert result["id"] == session_id


@responses.activate
def test_delete_session():
    session_id = fake.uuid4()
    project_id = fake.uuid4()
    responses.add(
        responses.DELETE,
        api_url(f"/v1/sessions/{session_id}"),
        match=[
            query_string_matcher(f"projectId={project_id}"),
        ],
        json={"id": session_id},
    )
    client = Client("test")
    result = client.delete_session(session_id=session_id, project_id=project_id)
    assert result["id"] == session_id
