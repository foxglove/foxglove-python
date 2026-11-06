from datetime import datetime, timezone

import responses
from foxglove.client import Client
from responses.matchers import query_param_matcher

from .api_url import api_url

CREATED = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
UPDATED = datetime(2025, 2, 1, 12, 0, tzinfo=timezone.utc)
LAST_SEEN = datetime(2025, 3, 1, 12, 0, tzinfo=timezone.utc)


@responses.activate
def test_get_org_members():
    responses.add(
        responses.GET,
        api_url("/v1/org-members"),
        match=[
            query_param_matcher(
                {
                    "sortBy": "lastSeenAt",
                    "sortOrder": "desc",
                    "search": "ada",
                    "role": "admin",
                    "status": "active",
                    "projectId": "prj_1",
                    "limit": "10",
                    "offset": "20",
                }
            )
        ],
        json=[
            {
                "id": "mbr_1",
                "orgId": "org_1",
                "email": "ada@example.com",
                "displayName": "Ada",
                "avatarImageUrl": "https://example.com/ada.png",
                "role": "admin",
                "status": "active",
                "projectIds": ["prj_1"],
                "createdAt": CREATED.isoformat(),
                "updatedAt": UPDATED.isoformat(),
                "lastSeenAt": LAST_SEEN.isoformat(),
            },
            {
                "id": "mbr_2",
                "orgId": "org_1",
                "email": "invitee@example.com",
                "role": "user",
                "status": "invited",
                "createdAt": CREATED.isoformat(),
                "updatedAt": UPDATED.isoformat(),
            },
        ],
    )

    members = Client("test").get_org_members(
        sort_by="last_seen_at",
        sort_order="desc",
        search="ada",
        role="admin",
        status="active",
        project_id="prj_1",
        limit=10,
        offset=20,
    )

    assert members == [
        {
            "id": "mbr_1",
            "org_id": "org_1",
            "email": "ada@example.com",
            "display_name": "Ada",
            "avatar_image_url": "https://example.com/ada.png",
            "role": "admin",
            "status": "active",
            "project_ids": ["prj_1"],
            "created_at": CREATED,
            "updated_at": UPDATED,
            "last_seen_at": LAST_SEEN,
        },
        {
            "id": "mbr_2",
            "org_id": "org_1",
            "email": "invitee@example.com",
            "display_name": None,
            "avatar_image_url": None,
            "role": "user",
            "status": "invited",
            "project_ids": None,
            "created_at": CREATED,
            "updated_at": UPDATED,
            "last_seen_at": None,
        },
    ]


@responses.activate
def test_get_org_members_without_filters():
    responses.add(
        responses.GET,
        api_url("/v1/org-members"),
        match=[query_param_matcher({})],
        json=[],
    )
    assert Client("test").get_org_members() == []
