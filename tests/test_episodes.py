import json
from datetime import datetime, timedelta, timezone

import pytest
import responses
from foxglove.client import Client

from .api_url import api_url

NOW = datetime(2026, 9, 15, tzinfo=timezone.utc)


def _episode_json(episode_id="ep_1", *, include_recordings=False):
    episode = {
        "id": episode_id,
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
        offset=20,
        include_recordings=True,
    )

    assert episodes.items[0]["start_time"] == NOW
    assert episodes.items[0]["metadata"]["nestedValue"] == {"keepMe": True}
    assert episodes.items[0]["recordings"][0]["device_id"] == "dev_1"
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
        "offset": "20",
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
