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
        "version": 2,
        "latestVersion": 3,
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
                "properties": {"stage": "review", "tags": []},
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
                "properties": {"stage": "review", "tags": []},
            }
        ],
    }


@responses.activate
def test_update_episodes_serializes_inputs():
    responses.add(
        responses.PATCH,
        api_url("/v1/episodes"),
        json={"episodes": [{"id": "ep_1", "version": 3}, {"id": "ep_2", "version": 1}]},
    )

    result = Client("test").update_episodes(
        episodes=[
            {
                "id": "ep_1",
                "applies_to_version": 2,
                "start_time": NOW,
                "end_time": NOW.replace(minute=1),
                "recordings": {"add": ["rec_2"], "remove": ["rec_1"]},
                "properties": {"stage": "done", "reviewer": None},
            },
            {"id": "ep_2"},
        ]
    )

    assert result == [{"id": "ep_1", "version": 3}, {"id": "ep_2", "version": 1}]
    assert json.loads(responses.calls[0].request.body) == {
        "episodes": [
            {
                "id": "ep_1",
                "appliesToVersion": 2,
                "startTime": NOW.astimezone().isoformat(),
                "endTime": NOW.replace(minute=1).astimezone().isoformat(),
                "recordings": {"add": ["rec_2"], "remove": ["rec_1"]},
                "properties": {"stage": "done", "reviewer": None},
            },
            {"id": "ep_2"},
        ]
    }


@responses.activate
def test_create_episodes_serializes_event_inputs():
    responses.add(
        responses.POST,
        api_url("/v1/episodes"),
        json={"episodes": [{"id": "ep_1", "created": False}]},
    )

    result = Client("test").create_episodes(
        project_id="prj_1",
        episodes=[{"event_id": "evt_1", "metadata": {"label": "merge"}}],
    )

    assert result == [{"id": "ep_1", "created": False}]
    assert json.loads(responses.calls[0].request.body) == {
        "projectId": "prj_1",
        "episodes": [{"eventId": "evt_1", "metadata": {"label": "merge"}}],
    }


@pytest.mark.parametrize(
    "episode,message",
    [
        ({}, "episodes[1]: provide exactly one of recordings or event_id"),
        (
            {"event_id": "evt_1", "recordings": ["rec_1"]},
            "episodes[1]: provide exactly one of recordings or event_id",
        ),
        (
            {"event_id": "evt_1", "start_time": NOW},
            "episodes[1]: event_id cannot be combined with start_time or end_time",
        ),
        (
            {"event_id": "evt_1", "end_time": NOW},
            "episodes[1]: event_id cannot be combined with start_time or end_time",
        ),
    ],
)
def test_create_episodes_rejects_incompatible_inputs(episode, message):
    with pytest.raises(ValueError) as raised:
        Client("test").create_episodes(
            project_id="prj_1", episodes=[{"event_id": "evt_0"}, episode]
        )

    assert str(raised.value) == message


@responses.activate
def test_get_episodes_maps_response_and_filters():
    episode_json = _episode_json(include_recordings=True)
    episode_json["properties"] = {"stage": "review", "tags": ["night"]}
    responses.add(
        responses.GET,
        api_url("/v1/episodes"),
        json={"episodes": [episode_json]},
    )

    episodes = Client("test").get_episodes(
        project_id="prj_1",
        start=NOW - timedelta(minutes=1),
        end=NOW,
        has_missing_recordings=False,
        recording_id="rec_1",
        properties_query="stage:review",
        sort_by="start_time",
        sort_order="asc",
        limit=10,
        cursor="next",
        include_recordings=True,
        include_properties=True,
    )

    assert episodes.items[0]["start_time"] == NOW
    assert episodes.items[0]["version"] == 2
    assert episodes.items[0]["latest_version"] == 3
    assert episodes.items[0]["properties"] == {"stage": "review", "tags": ["night"]}
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
        "propertiesQuery": "stage:review",
        "sortBy": "startTime",
        "sortOrder": "asc",
        "limit": "10",
        "cursor": "next",
        "include": "recordings,properties",
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


@pytest.mark.parametrize("episode_version", [None, 2])
@pytest.mark.parametrize("method", ["iter_messages", "download_data", "get_topics"])
@responses.activate
def test_episode_version_requests(method, episode_version):
    from .generate import generate_json_data

    link = "https://storage.example/episode.mcap"
    if method == "get_topics":
        responses.add(responses.GET, api_url("/v1/data/topics"), json=[])
    else:
        responses.add(responses.POST, api_url("/v1/data/stream"), json={"link": link})
        responses.add(responses.GET, link, body=generate_json_data())
    result = getattr(Client("test"), method)(
        episode_id="ep_1",
        episode_version=episode_version,
        start=NOW - timedelta(seconds=5),
        end=NOW,
    )
    if method == "iter_messages":
        assert len(list(result)) == 10
    request = responses.calls[0].request
    params = request.params if method == "get_topics" else json.loads(request.body)
    assert params["episodeId"] == "ep_1"
    assert params["start"] == (NOW - timedelta(seconds=5)).astimezone().isoformat()
    assert params["end"] == NOW.astimezone().isoformat()
    if episode_version is None:
        assert "episodeVersion" not in params
    else:
        assert params["episodeVersion"] == (
            str(episode_version) if method == "get_topics" else episode_version
        )


@pytest.mark.parametrize("method", ["iter_messages", "download_data", "get_topics"])
def test_episode_version_requires_episode_id(method):
    with pytest.raises(RuntimeError, match="episode_version requires episode_id"):
        getattr(Client("test"), method)(
            device_id="device", start=NOW, end=NOW, episode_version=2
        )
