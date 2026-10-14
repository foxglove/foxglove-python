import json
from datetime import datetime, timedelta, timezone

import pytest
import requests
import responses
from foxglove.client import Client

from .api_url import api_url

NOW = datetime(2026, 9, 15, tzinfo=timezone.utc)


def _episode_json(episode_id="ep_1", *, include_recordings=False):
    episode = {
        "id": episode_id,
        "version": 1,
        "latestVersion": 1,
        "projectId": "prj_1",
        "startTime": NOW.isoformat(),
        "endTime": NOW.replace(minute=1).isoformat(),
        "metadata": {"result": "success", "nestedValue": {"keepMe": True}},
        "creatorApiKeyId": "key_1",
        "createdAt": NOW.isoformat(),
    }
    if include_recordings:
        episode["recordings"] = [
            {
                "id": "rec_1",
                "path": "run.mcap",
                "location": {"bucket": "robot-logs", "path": "fleet/run.mcap"},
                "start": NOW.isoformat(),
                "end": NOW.replace(minute=1).isoformat(),
                "deviceId": "dev_1",
                "available": True,
                "resolvable": True,
            }
        ]
        episode["hasMissingRecordings"] = False
    return episode


@responses.activate
def test_create_episodes_serializes_inputs():
    responses.add(
        responses.POST,
        api_url("/v1/episodes"),
        json={"episodes": [{"id": "ep_1", "created": True}]},
    )

    result = Client("test").create_episodes(
        project_id="prj_1",
        episodes=[
            {
                "recordings": ["rec_1"],
                "start_time": NOW,
                "end_time": NOW.replace(minute=1),
                "metadata": {"result": "success"},
            }
        ],
    )

    assert result == [{"id": "ep_1", "created": True}]
    assert json.loads(responses.calls[0].request.body) == {
        "projectId": "prj_1",
        "episodes": [
            {
                "recordings": ["rec_1"],
                "startTime": NOW.astimezone().isoformat(),
                "endTime": NOW.replace(minute=1).astimezone().isoformat(),
                "metadata": {"result": "success"},
            }
        ],
    }


@responses.activate
def test_get_episodes_maps_response_and_filters():
    responses.add(
        responses.GET,
        api_url("/v1/episodes"),
        json={"episodes": [_episode_json(include_recordings=True)]},
    )

    episodes = Client("test").get_episodes(
        project_id="prj_1",
        start=NOW - timedelta(minutes=1),
        end=NOW,
        has_missing_recordings=False,
        recording_id="rec_1",
        sort_by="start_time",
        sort_order="asc",
        limit=10,
        cursor="next",
        include_recordings=True,
    )

    assert episodes.items[0]["start_time"] == NOW
    assert episodes.items[0]["metadata"]["nestedValue"] == {"keepMe": True}
    assert episodes.items[0]["recordings"][0]["device_id"] == "dev_1"
    assert episodes.items[0]["recordings"][0]["location"] == {
        "bucket": "robot-logs",
        "path": "fleet/run.mcap",
    }
    assert episodes.items[0]["recordings"][0]["resolvable"] is True
    assert episodes.items[0]["has_missing_recordings"] is False
    assert responses.calls[0].request.params == {
        "projectId": "prj_1",
        "start": (NOW - timedelta(minutes=1)).astimezone().isoformat(),
        "end": NOW.astimezone().isoformat(),
        "hasMissingRecordings": "false",
        "recordingId": "rec_1",
        "sortBy": "startTime",
        "sortOrder": "asc",
        "limit": "10",
        "cursor": "next",
        "include": "recordings",
    }


@responses.activate
def test_get_and_delete_episode():
    responses.add(
        responses.GET,
        api_url("/v1/episodes/ep_1"),
        json=_episode_json(include_recordings=True),
    )
    responses.add(
        responses.DELETE,
        api_url("/v1/episodes/ep_1"),
        json={"success": True},
    )
    client = Client("test")

    episode = client.get_episode(episode_id="ep_1", include_recordings=True)
    client.delete_episode(episode_id="ep_1")

    assert episode["id"] == "ep_1"
    assert responses.calls[0].request.params == {"include": "recordings"}


@responses.activate
def test_download_data_by_episode_uses_episode_bounds():
    download_link = "https://storage.example/episode.mcap"
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        json={"link": download_link},
    )
    responses.add(responses.GET, download_link, body=b"mcap")

    data = Client("test").download_data(episode_id="ep_1")

    assert data == b"mcap"
    assert json.loads(responses.calls[0].request.body) == {
        "episodeId": "ep_1",
        "outputFormat": "mcap",
        "topics": [],
    }
    assert "Authorization" not in responses.calls[1].request.headers


@responses.activate
def test_get_topics_by_episode_uses_episode_bounds():
    responses.add(responses.GET, api_url("/v1/data/topics"), json=[])

    topics = Client("test").get_topics(episode_id="ep_1")

    assert topics == []
    assert responses.calls[0].request.params == {
        "includeSchemas": "false",
        "episodeId": "ep_1",
    }


def test_episode_download_rejects_another_identifier():
    client = Client("test")

    with pytest.raises(RuntimeError) as raised:
        client.download_data(episode_id="ep_1", device_id="dev_1")

    assert str(raised.value) == "episode_id cannot be combined with another identifier"


def test_download_without_episode_requires_time_range():
    client = Client("test")

    with pytest.raises(RuntimeError) as raised:
        client.download_data(device_id="dev_1")

    assert str(raised.value) == "start and end must be provided unless using episode_id"


def test_get_topics_without_episode_requires_time_range():
    client = Client("test")

    with pytest.raises(RuntimeError) as raised:
        client.get_topics(session_id="ses_1")

    assert str(raised.value) == "start and end must be provided unless using episode_id"


@responses.activate
def test_episode_version_reads_and_label_filters():
    historical = {
        **_episode_json(),
        "version": 1,
        "latestVersion": 2,
        "labels": {
            "note": {"type": "TEXT", "content": "mike"},
            "asset": {
                "type": "URI",
                "content": {
                    "uri": "s3://bucket/item",
                    "checksum": "sha256:abc",
                    "contentType": "image/png",
                },
            },
        },
    }
    responses.add(responses.GET, api_url("/v1/episodes/ep_1"), json=historical)
    responses.add(
        responses.GET,
        api_url("/v1/episodes"),
        json={"episodes": [historical]},
    )
    responses.add(
        responses.GET,
        api_url("/v1/episodes/ep_1/versions"),
        json={
            "versions": [
                {
                    "version": 2,
                    "createdAt": NOW.isoformat(),
                    "changedFields": ["labels"],
                }
            ]
        },
        headers={"fg-pagination-next-cursor": "next"},
    )
    responses.add(
        responses.GET,
        api_url("/v1/episodes/ep_1/versions"),
        json={
            "versions": [
                {"version": 1, "createdAt": NOW.isoformat(), "changedFields": []}
            ]
        },
    )
    client = Client("test")

    episode = client.get_episode(episode_id="ep_1", version=1, include_labels=True)
    listed = client.get_episodes(include_labels=True, label_query="note:mike")
    versions = list(
        client.get_episode_versions(episode_id="ep_1", limit=1).auto_paging_iter()
    )

    assert episode["version"] == 1
    assert episode["latest_version"] == 2
    assert episode["labels"]["asset"]["content"]["content_type"] == "image/png"
    assert listed.items[0]["labels"]["note"]["content"] == "mike"
    assert responses.calls[0].request.params == {"version": "1", "include": "labels"}
    assert responses.calls[1].request.params["labelQuery"] == "note:mike"
    assert [version["version"] for version in versions] == [2, 1]
    assert responses.calls[3].request.params == {"limit": "1", "cursor": "next"}


@responses.activate
def test_update_episode_serializes_labels_and_preserves_conflict():
    responses.add(
        responses.PATCH,
        api_url("/v1/episodes/ep_1"),
        json={**_episode_json(), "version": 2, "latestVersion": 2},
    )
    responses.add(
        responses.PATCH,
        api_url("/v1/episodes/ep_1"),
        json={
            "error": "Version conflict",
            "conflicts": [{"id": "ep_1", "currentVersion": 2}],
        },
        status=409,
    )
    client = Client("test")
    labels = {
        "note": {"type": "TEXT", "content": "brook"},
        "old": None,
        "asset": {
            "type": "URI",
            "content": {
                "uri": "s3://bucket/item",
                "checksum": "sha256:abc",
                "content_type": "image/png",
            },
        },
    }

    result = client.update_episode(
        episode_id="ep_1",
        applies_to_version=1,
        start_time=NOW,
        recordings={"add": ["rec_2"], "remove": ["rec_1"]},
        labels=labels,
    )
    assert result["version"] == 2
    assert json.loads(responses.calls[0].request.body) == {
        "appliesToVersion": 1,
        "startTime": NOW.astimezone().isoformat(),
        "recordings": {"add": ["rec_2"], "remove": ["rec_1"]},
        "labels": {
            "note": labels["note"],
            "old": None,
            "asset": {
                "type": "URI",
                "content": {
                    "uri": "s3://bucket/item",
                    "checksum": "sha256:abc",
                    "contentType": "image/png",
                },
            },
        },
    }

    with pytest.raises(requests.HTTPError) as raised:
        client.update_episode(
            episode_id="ep_1", applies_to_version=1, labels={"old": None}
        )
    assert raised.value.response.status_code == 409
    assert raised.value.response.json()["conflicts"][0]["currentVersion"] == 2


@responses.activate
def test_bulk_episode_updates_preserve_order_and_atomic_error():
    responses.add(
        responses.PATCH,
        api_url("/v1/episodes"),
        json={"episodes": [{"id": "ep_2", "version": 4}, {"id": "ep_1", "version": 2}]},
    )
    responses.add(
        responses.PATCH,
        api_url("/v1/episodes"),
        json={
            "error": "Version conflict",
            "conflicts": [{"id": "ep_1", "currentVersion": 2}],
        },
        status=409,
    )
    client = Client("test")
    updates = [
        {"id": "ep_2", "labels": {"note": {"type": "TEXT", "content": "ready"}}},
        {"id": "ep_1", "applies_to_version": 1, "end_time": NOW.replace(minute=2)},
    ]

    assert client.update_episodes(episodes=updates) == [
        {"id": "ep_2", "version": 4},
        {"id": "ep_1", "version": 2},
    ]
    body = json.loads(responses.calls[0].request.body)
    assert body["episodes"][0]["id"] == "ep_2"
    assert body["episodes"][1]["appliesToVersion"] == 1
    with pytest.raises(requests.HTTPError) as raised:
        client.update_episodes(episodes=updates)
    assert raised.value.response.json()["conflicts"][0]["id"] == "ep_1"


@responses.activate
def test_episode_stream_and_topics_use_explicit_version():
    download_link = "https://storage.example/episode.mcap"
    responses.add(
        responses.POST, api_url("/v1/data/stream"), json={"link": download_link}
    )
    responses.add(responses.GET, download_link, body=b"version one")
    responses.add(responses.GET, api_url("/v1/data/topics"), json=[])
    client = Client("test")

    assert client.download_data(episode_id="ep_1", episode_version=1) == b"version one"
    assert client.get_topics(episode_id="ep_1", episode_version=1) == []
    assert json.loads(responses.calls[0].request.body)["episodeVersion"] == 1
    assert responses.calls[2].request.params["episodeVersion"] == "1"

    with pytest.raises(ValueError, match="requires episode_id"):
        client.download_data(episode_version=1)
    with pytest.raises(ValueError, match="episode version"):
        client.get_topics(episode_id="ep_1", episode_version=0)
