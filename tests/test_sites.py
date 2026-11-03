from datetime import datetime

import responses
from dateutil.tz import tzoffset
from responses.matchers import json_params_matcher

from foxglove.client import Client

from .api_url import api_url

now = datetime.now(tzoffset(None, 0))
rules = [
    {"pattern": "^tmp/", "action": {"type": "ignore"}},
    {
        "pattern": "^robots/([^/]+)/",
        "action": {
            "type": "import",
            "device": {"type": "nameTemplate", "deviceName": "$1"},
            "projectId": "prj_1",
        },
    },
]


def _site_json(**overrides):
    return {
        "id": "site_1",
        "name": "Edge",
        "type": "edge",
        "retainRecordingsSeconds": 3600,
        **overrides,
    }


def _policy_json(**overrides):
    return {
        "revision": 2,
        "createdAt": now.isoformat(),
        "policy": {"schemaVersion": 1, "rules": rules},
        **overrides,
    }


@responses.activate
def test_get_sites():
    responses.add(
        responses.GET,
        api_url("/v1/sites"),
        json=[
            _site_json(),
            {
                "id": "site_2",
                "name": "Self",
                "type": "self-hosted",
                "url": "https://site.example.com",
            },
        ],
    )
    sites = Client("test").get_sites()
    assert sites[0] == {
        "id": "site_1",
        "name": "Edge",
        "type": "edge",
        "url": None,
        "retain_recordings_seconds": 3600,
    }
    assert sites[1]["url"] == "https://site.example.com"
    assert sites[1]["retain_recordings_seconds"] is None


@responses.activate
def test_get_site():
    responses.add(responses.GET, api_url("/v1/sites/site_1"), json=_site_json())
    assert Client("test").get_site(site_id="site_1")["type"] == "edge"


@responses.activate
def test_create_site():
    responses.add(
        responses.POST,
        api_url("/v1/sites"),
        match=[
            json_params_matcher(
                {"name": "Edge", "type": "edge", "retainRecordingsSeconds": 3600},
                strict_match=True,
            )
        ],
        json=_site_json(),
    )
    site = Client("test").create_site(
        name="Edge", site_type="edge", retain_recordings_seconds=3600
    )
    assert site["id"] == "site_1"


@responses.activate
def test_update_site_sends_only_set_fields():
    responses.add(
        responses.PATCH,
        api_url("/v1/sites/site_1"),
        match=[json_params_matcher({"retainRecordingsSeconds": 0}, strict_match=True)],
        json=_site_json(retainRecordingsSeconds=0),
    )
    site = Client("test").update_site(site_id="site_1", retain_recordings_seconds=0)
    assert site["retain_recordings_seconds"] == 0


@responses.activate
def test_delete_site():
    responses.add(responses.DELETE, api_url("/v1/sites/site_1"), json={"id": "site_1"})
    Client("test").delete_site(site_id="site_1")
    assert len(responses.calls) == 1


@responses.activate
def test_get_site_ingest_path_policy():
    responses.add(
        responses.GET,
        api_url("/v1/sites/site_1/ingest-path-policy"),
        json=_policy_json(),
    )
    assert Client("test").get_site_ingest_path_policy(site_id="site_1") == {
        "revision": 2,
        "created_at": now,
        "policy": {"schemaVersion": 1, "rules": rules},
    }


@responses.activate
def test_get_site_ingest_path_policy_unconfigured():
    responses.add(
        responses.GET, api_url("/v1/sites/site_1/ingest-path-policy"), status=204
    )
    assert Client("test").get_site_ingest_path_policy(site_id="site_1") is None


@responses.activate
def test_save_site_ingest_path_policy():
    responses.add(
        responses.POST,
        api_url("/v1/sites/site_1/ingest-path-policy"),
        match=[
            json_params_matcher(
                {"schemaVersion": 1, "rules": rules, "previousRevision": 0},
                strict_match=True,
            )
        ],
        json=_policy_json(revision=1),
    )
    policy = Client("test").save_site_ingest_path_policy(
        site_id="site_1", rules=rules, previous_revision=0
    )
    assert policy["revision"] == 1
    assert policy["policy"]["rules"] == rules


@responses.activate
def test_notify_site_bucket():
    responses.add(
        responses.POST,
        api_url("/v1/site-bucket-notifications"),
        match=[
            json_params_matcher(
                {
                    "messageId": "msg_1",
                    "fileName": "robots/r1/log.mcap",
                    "bucketName": "inbox",
                    "storageProvider": "azure",
                    "azureStorageAccountName": "acct",
                    "deviceName": "r1",
                    "projectId": "prj_1",
                },
                strict_match=True,
            )
        ],
        json={"success": True},
    )
    Client("test").notify_site_bucket(
        message_id="msg_1",
        file_name="robots/r1/log.mcap",
        bucket_name="inbox",
        storage_provider="azure",
        azure_storage_account_name="acct",
        device_name="r1",
        project_id="prj_1",
    )
    assert len(responses.calls) == 1
