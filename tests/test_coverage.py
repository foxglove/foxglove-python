from datetime import datetime

import responses
from responses.matchers import query_param_matcher
from faker import Faker
from foxglove.client import Client

from .api_url import api_url

fake = Faker()


@responses.activate
def test_get_coverage():
    device_id = fake.uuid4()
    device_name = fake.name()
    project_id = "prj_123"
    responses.add(
        responses.GET,
        api_url(f"/v1/data/coverage"),
        json=[
            {
                "deviceId": device_id,
                "device": {"id": device_id, "name": device_name},
                "start": datetime.now().isoformat(),
                "end": datetime.now().isoformat(),
                "projectId": project_id,
            },
            {
                "deviceId": device_id,
                "device": {"id": device_id, "name": device_name},
                "start": datetime.now().isoformat(),
                "end": datetime.now().isoformat(),
                "projectId": project_id,
            },
        ],
    )
    client = Client("test")
    coverage = client.get_coverage(
        start=datetime.now(),
        end=datetime.now(),
        device_id=device_id,
        project_id=project_id,
    )
    assert len(coverage) == 2
    assert coverage[0]["device_id"] == device_id


@responses.activate
def test_get_coverage_by_recording_with_edge_recordings():
    responses.add(
        responses.GET,
        api_url("/v1/data/coverage"),
        match=[
            query_param_matcher(
                {"recordingKey": "run.mcap", "includeEdgeRecordings": "true"}
            )
        ],
        json=[
            {
                "deviceId": "dev_1",
                "device": {"id": "dev_1", "name": "robot"},
                "start": datetime(2026, 1, 1).isoformat(),
                "end": datetime(2026, 1, 2).isoformat(),
                "importStatus": "pending",
            }
        ],
    )
    coverage = Client("test").get_coverage(
        recording_key="run.mcap", include_edge_recordings=True
    )
    assert coverage[0]["import_status"] == "pending"


@responses.activate
def test_get_coverage_with_fractional_tolerance():
    responses.add(
        responses.GET,
        api_url("/v1/data/coverage"),
        match=[query_param_matcher({"recordingId": "rec_1", "tolerance": "15.5"})],
        json=[],
    )
    coverage = Client("test").get_coverage(recording_id="rec_1", tolerance=15.5)
    assert coverage == []
