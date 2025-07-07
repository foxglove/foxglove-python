from datetime import datetime

import responses
from faker import Faker
from foxglove.client import Client
import arrow

from .api_url import api_url

fake = Faker()


@responses.activate
def test_get_projects():
    project1_id = fake.uuid4()
    project1_name = "Test Project"
    project1_org_member_count = 5
    project1_last_seen_at = datetime(2025, 1, 1, 12, 0, 0)

    project2_id = fake.uuid4()

    responses.add(
        responses.GET,
        api_url("/v1/projects"),
        json=[
            {
                "id": project1_id,
                "name": project1_name,
                "orgMemberCount": project1_org_member_count,
                "lastSeenAt": project1_last_seen_at.isoformat(),
            },
            {
                "id": project2_id,
            },
        ],
    )
    client = Client("test")
    projects = client.get_projects()
    assert len(projects) == 2

    project1 = next(p for p in projects if p["id"] == project1_id)
    assert project1["id"] == project1_id
    assert project1["name"] == project1_name
    assert project1["org_member_count"] == project1_org_member_count
    assert project1["last_seen_at"] == arrow.get(project1_last_seen_at).datetime

    # Handles optional values
    project2 = next(p for p in projects if p["id"] == project2_id)
    assert project2["id"] == project2_id
    assert project2["name"] is None
    assert project2["org_member_count"] == 0
    assert project2["last_seen_at"] is None


@responses.activate
def test_get_projects_empty():
    responses.add(
        responses.GET,
        api_url("/v1/projects"),
        json=[],
    )

    client = Client("test")
    projects = client.get_projects()

    assert len(projects) == 0
