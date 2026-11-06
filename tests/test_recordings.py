from datetime import datetime
from dateutil.tz import tzoffset

import pytest
import responses
from faker import Faker
from foxglove.client import Client
from responses.matchers import query_param_matcher

from .api_url import api_url

fake = Faker()
now = datetime.now(tzoffset(None, 0))


@responses.activate
def test_delete_recording():
    recording_id = fake.uuid4()
    responses.add(
        responses.DELETE,
        api_url(f"/v1/recordings/{recording_id}"),
        json={"success": True, "id": recording_id},
    )
    client = Client("test")
    try:
        client.delete_recording(recording_id=recording_id)
    except:
        assert False


@responses.activate
def test_delete_recording_by_key_quotes_path():
    responses.add(
        responses.DELETE,
        api_url("/v1/recordings/runs%2Fday%201.mcap"),
        match=[query_param_matcher({})],
        json={"recordingId": "rec_1"},
    )
    assert Client("test").delete_recording(recording_key="runs/day 1.mcap") is None


def test_delete_recording_requires_one_identifier():
    client = Client("test")
    with pytest.raises(RuntimeError, match="mutually exclusive"):
        client.delete_recording(recording_id="rec_1", recording_key="run.mcap")
    with pytest.raises(RuntimeError, match="must be provided"):
        client.delete_recording()


@responses.activate
def test_get_recordings():
    device_id = fake.uuid4()
    recording_id_a = fake.uuid4()
    recording_id_b = fake.uuid4()
    session_id = fake.uuid4()
    path = fake.file_name(extension="mcap")
    size = fake.random_number()
    message_count = fake.random_number()
    site_id = fake.uuid4()
    edge_site_id = fake.uuid4()
    now = datetime.now(tzoffset(None, 0))
    project_id = "prj_123"
    location = {"scheme": "s3", "path": path, "bucket": "my-bucket"}

    responses.add(
        responses.GET,
        api_url("/v1/recordings"),
        json=[
            {
                "id": recording_id_a,
                "path": path,
                "size": size,
                "messageCount": message_count,
                "createdAt": now.isoformat(),
                "importedAt": now.isoformat(),
                "start": now.isoformat(),
                "end": now.isoformat(),
                "importStatus": "complete",
                "site": {"id": site_id, "name": "primarySite"},
                "device": {"id": device_id, "name": "deviceName"},
                "metadata": {"hey": "now", "brown": "cow"},
                "key": "recording_key",
                "projectId": project_id,
                "sessionId": session_id,
                "location": location,
            },
            {
                "id": recording_id_b,
                "path": path,
                "size": size,
                "messageCount": message_count,
                "createdAt": now.isoformat(),
                "start": now.isoformat(),
                "end": now.isoformat(),
                "importStatus": "none",
                "edgeSite": {"id": edge_site_id, "name": "edgeSite"},
                "device": {"id": device_id, "name": "deviceName"},
                "projectId": project_id,
            },
        ],
    )
    client = Client("test")
    recordings = client.get_recordings(
        start=datetime.now(),
        end=datetime.now(),
        device_id=device_id,
        project_id=project_id,
    )
    assert recordings == [
        {
            "id": recording_id_a,
            "path": path,
            "size": size,
            "message_count": message_count,
            "created_at": now,
            "imported_at": now,
            "start": now,
            "end": now,
            "import_status": "complete",
            "site": {"id": site_id, "name": "primarySite"},
            "edge_site": None,
            "device": {"id": device_id, "name": "deviceName"},
            "metadata": {"hey": "now", "brown": "cow"},
            "key": "recording_key",
            "project_id": project_id,
            "session_id": session_id,
            "session_key": None,
            "location": location,
        },
        {
            "id": recording_id_b,
            "path": path,
            "size": size,
            "message_count": message_count,
            "created_at": now,
            "start": now,
            "end": now,
            "import_status": "none",
            "imported_at": None,
            "site": None,
            "edge_site": {"id": edge_site_id, "name": "edgeSite"},
            "device": {"id": device_id, "name": "deviceName"},
            "metadata": None,
            "key": None,
            "project_id": project_id,
            "session_id": None,
            "session_key": None,
            "location": None,
        },
    ]


def _recording_json(**overrides):
    return {
        "id": "rec_1",
        "projectId": "prj_1",
        "path": "a b.mcap",
        "size": 10,
        "createdAt": now.isoformat(),
        "start": now.isoformat(),
        "end": now.isoformat(),
        "importStatus": "complete",
        **overrides,
    }


@responses.activate
def test_get_recordings_topic_and_metadata_query():
    responses.add(
        responses.GET,
        api_url("/v1/recordings"),
        match=[
            query_param_matcher(
                {"topic": "/camera", "metadataQuery": "robot:alpha"},
            )
        ],
        json=[_recording_json()],
    )
    recordings = Client("test").get_recordings(
        topic="/camera", metadata_query="robot:alpha"
    )
    assert [r["id"] for r in recordings] == ["rec_1"]


@responses.activate
def test_get_recording_by_id():
    location = {"scheme": "gs", "path": "a b.mcap", "bucket": "bucket"}
    responses.add(
        responses.GET,
        api_url("/v1/recordings/rec_1"),
        json=_recording_json(location=location, sessionId="ses_1"),
    )
    recording = Client("test").get_recording(recording_id="rec_1")
    assert recording["id"] == "rec_1"
    assert recording["project_id"] == "prj_1"
    assert recording["imported_at"] is None
    assert recording["location"] == location
    assert recording["session_id"] == "ses_1"


@responses.activate
def test_get_recording_by_key_quotes_path():
    responses.add(
        responses.GET,
        api_url("/v1/recordings/my%2Fkey"),
        json=_recording_json(key="my/key"),
    )
    recording = Client("test").get_recording(recording_key="my/key")
    assert recording["key"] == "my/key"


def test_get_recording_requires_exactly_one_identifier():
    client = Client("test")
    with pytest.raises(RuntimeError):
        client.get_recording()
    with pytest.raises(RuntimeError):
        client.get_recording(recording_id="rec_1", recording_key="key")


@responses.activate
def test_import_recording_from_edge():
    responses.add(
        responses.POST,
        api_url("/v1/recordings/rec_1/import"),
        json={"id": "rec_1", "importStatus": "pending"},
    )
    result = Client("test").import_recording_from_edge(recording_id="rec_1")
    assert result == {"id": "rec_1", "import_status": "pending"}


def _pending_import_json(**overrides):
    return {
        "requestId": "req_1",
        "orgId": "org_1",
        "siteId": "site_1",
        "filename": "a.mcap",
        "pipelineStage": "indexing",
        "createdAt": now.isoformat(),
        "updatedAt": now.isoformat(),
        **overrides,
    }


@responses.activate
def test_get_pending_imports():
    responses.add(
        responses.GET,
        api_url("/v1/data/pending-imports"),
        match=[
            query_param_matcher(
                {
                    "deviceName": "robot",
                    "updatedSince": now.astimezone().isoformat(),
                    "showCompleted": "true",
                    "showQuarantined": "false",
                    "sortBy": "updatedAt",
                    "limit": "5",
                },
            )
        ],
        json=[
            _pending_import_json(
                importId="imp_1",
                deviceId="dev_1",
                deviceName="robot",
                projectId="prj_1",
                status="complete",
                quarantinedAt=now.isoformat(),
            ),
            _pending_import_json(requestId="req_2"),
        ],
    )
    pending = Client("test").get_pending_imports(
        device_name="robot",
        updated_since=now,
        show_completed=True,
        sort_by="updated_at",
        limit=5,
    )
    assert pending[0] == {
        "request_id": "req_1",
        "import_id": "imp_1",
        "org_id": "org_1",
        "project_id": "prj_1",
        "site_id": "site_1",
        "device_id": "dev_1",
        "device_name": "robot",
        "filename": "a.mcap",
        "pipeline_stage": "indexing",
        "status": "complete",
        "error": None,
        "created_at": now,
        "updated_at": now,
        "quarantined_at": now,
    }
    assert pending[1]["request_id"] == "req_2"
    assert pending[1]["import_id"] is None
    assert pending[1]["quarantined_at"] is None


@responses.activate
def test_get_import_errors():
    responses.add(
        responses.GET,
        api_url("/v1/data/import-errors"),
        match=[
            query_param_matcher(
                {
                    "filenameMatches": "*.mcap",
                    "error": "corrupt",
                    "siteBackfillJobId": "sbj_1",
                    "sortOrder": "desc",
                },
            )
        ],
        json=[_pending_import_json(status="error", error="corrupt file")],
    )
    errors = Client("test").get_import_errors(
        filename_matches="*.mcap",
        error="corrupt",
        site_backfill_job_id="sbj_1",
        sort_order="desc",
    )
    assert errors[0]["error"] == "corrupt file"
    assert errors[0]["status"] == "error"
