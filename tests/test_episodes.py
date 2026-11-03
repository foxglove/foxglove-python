import json
from datetime import datetime, timedelta, timezone

import pytest
import responses
from responses.matchers import json_params_matcher, query_param_matcher
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
def test_get_episodes_rejects_empty_exact_recording_ids():
    with pytest.raises(ValueError, match="exact_recording_ids"):
        Client("test").get_episodes(exact_recording_ids=[])

    assert len(responses.calls) == 0


@responses.activate
def test_get_episodes_omits_none_exact_recording_ids():
    responses.add(
        responses.GET,
        api_url("/v1/episodes"),
        match=[query_param_matcher({}, strict_match=True)],
        json={"episodes": [_episode_json()]},
    )

    episodes = Client("test").get_episodes(exact_recording_ids=None)

    assert episodes.items[0]["id"] == "ep_1"


@responses.activate
def test_get_episodes_filters_exact_recording_ids():
    responses.add(
        responses.GET,
        api_url("/v1/episodes"),
        match=[query_param_matcher({"exactRecordingIds": "rec_1,rec_2"})],
        json={"episodes": [_episode_json()]},
    )

    episodes = Client("test").get_episodes(exact_recording_ids=["rec_1", "rec_2"])

    assert episodes.items[0]["id"] == "ep_1"


@responses.activate
def test_get_episode_at_version():
    responses.add(
        responses.GET,
        api_url("/v1/episodes/ep_1"),
        match=[query_param_matcher({"version": "2", "include": "properties"})],
        json={**_episode_json(), "properties": {"stage": "review"}},
    )

    episode = Client("test").get_episode(
        episode_id="ep_1", version=2, include_properties=True
    )

    assert episode["version"] == 2
    assert episode["latest_version"] == 3
    assert episode["properties"] == {"stage": "review"}


@responses.activate
def test_get_episode_versions():
    responses.add(
        responses.GET,
        api_url("/v1/episodes/ep_1/versions"),
        match=[query_param_matcher({"limit": "2"})],
        headers={"fg-pagination-next-cursor": "next"},
        json={
            "versions": [
                {
                    "version": 2,
                    "createdAt": NOW.isoformat(),
                    "creatorApiKeyId": "key_1",
                    "changedFields": ["startTime", "properties"],
                },
                {"version": 1, "createdAt": NOW.isoformat(), "changedFields": []},
            ]
        },
    )

    versions = Client("test").get_episode_versions(episode_id="ep_1", limit=2)

    assert versions.items[0] == {
        "version": 2,
        "created_at": NOW,
        "creator_org_member_id": None,
        "creator_api_key_id": "key_1",
        "changed_fields": ["start_time", "properties"],
    }
    assert versions.items[1]["creator_api_key_id"] is None
    assert versions.next_cursor == "next"


@responses.activate
def test_update_episode_serializes_inputs():
    responses.add(
        responses.PATCH,
        api_url("/v1/episodes/ep_1"),
        match=[
            json_params_matcher(
                {
                    "appliesToVersion": 2,
                    "startTime": NOW.astimezone().isoformat(),
                    "endTime": NOW.replace(minute=1).astimezone().isoformat(),
                    "recordings": {"add": ["rec_2"], "remove": ["rec_1"]},
                    "properties": {"stage": "done", "reviewer": None},
                },
                strict_match=True,
            )
        ],
        json={**_episode_json(), "version": 3, "properties": {"stage": "done"}},
    )

    episode = Client("test").update_episode(
        episode_id="ep_1",
        applies_to_version=2,
        start_time=NOW,
        end_time=NOW.replace(minute=1),
        recordings={"add": ["rec_2"], "remove": ["rec_1"]},
        properties={"stage": "done", "reviewer": None},
    )

    assert episode["id"] == "ep_1"
    assert episode["version"] == 3
    assert episode["properties"] == {"stage": "done"}


@responses.activate
def test_update_episode_sends_only_set_fields():
    responses.add(
        responses.PATCH,
        api_url("/v1/episodes/ep_1"),
        match=[
            json_params_matcher({"properties": {"stage": "done"}}, strict_match=True)
        ],
        json=_episode_json(),
    )

    Client("test").update_episode(episode_id="ep_1", properties={"stage": "done"})

    assert len(responses.calls) == 1


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
def test_download_data_at_episode_version_with_attachments():
    download_link = "https://storage.example/episode.mcap"
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        match=[
            json_params_matcher(
                {
                    "episodeId": "ep_1",
                    "episodeVersion": 2,
                    "includeAttachments": True,
                    "outputFormat": "mcap",
                    "topics": [],
                },
                strict_match=True,
            )
        ],
        json={"link": download_link},
    )
    responses.add(responses.GET, download_link, body=b"mcap")

    data = Client("test").download_data(
        episode_id="ep_1", episode_version=2, include_attachments=True
    )

    assert data == b"mcap"


@responses.activate
def test_get_topics_by_episode_uses_episode_bounds():
    responses.add(responses.GET, api_url("/v1/data/topics"), json=[])

    topics = Client("test").get_topics(episode_id="ep_1")

    assert topics == []
    assert responses.calls[0].request.params == {
        "includeSchemas": "false",
        "episodeId": "ep_1",
    }


def test_download_rejects_episode_version_without_episode():
    with pytest.raises(RuntimeError, match="episode_version requires episode_id"):
        Client("test").download_data(recording_id="rec_1", episode_version=2)


def test_download_rejects_recording_with_session():
    with pytest.raises(RuntimeError, match="cannot be combined with a session"):
        Client("test").download_data(recording_id="rec_1", session_id="ses_1")


def test_download_by_recording_and_device_requires_time_range():
    with pytest.raises(RuntimeError, match="start and end must be provided"):
        Client("test").download_data(recording_id="rec_1", device_id="dev_1")


def test_episode_download_rejects_another_identifier():
    client = Client("test")

    with pytest.raises(RuntimeError) as raised:
        client.download_data(episode_id="ep_1", device_id="dev_1")

    assert str(raised.value) == "episode_id cannot be combined with another identifier"


def test_download_without_episode_requires_time_range():
    client = Client("test")

    with pytest.raises(RuntimeError) as raised:
        client.download_data(device_id="dev_1")

    assert str(raised.value) == (
        "start and end must be provided unless using episode_id, "
        "recording_id, or recording_key"
    )


def test_get_topics_without_episode_requires_time_range():
    client = Client("test")

    with pytest.raises(RuntimeError) as raised:
        client.get_topics(session_id="ses_1")

    assert str(raised.value) == "start and end must be provided unless using episode_id"


CREATION_CONTEXT = {
    "version": 1,
    "basis": {"type": "recordings", "recordingIds": ["rec_1"]},
}


@responses.activate
def test_get_episodes_includes_creation_context():
    responses.add(
        responses.GET,
        api_url("/v1/episodes"),
        match=[query_param_matcher({"include": "recordings,creationContext"})],
        json={
            "episodes": [
                {
                    **_episode_json(include_recordings=True),
                    "creationContext": CREATION_CONTEXT,
                },
                _episode_json("ep_2"),
            ]
        },
    )

    episodes = Client("test").get_episodes(
        include_recordings=True, include_creation_context=True
    )

    assert episodes.items[0]["creation_context"] == CREATION_CONTEXT
    assert "creation_context" not in episodes.items[1]


@responses.activate
def test_get_episode_keeps_null_creation_context():
    responses.add(
        responses.GET,
        api_url("/v1/episodes/ep_1"),
        json={**_episode_json(), "creationContext": None},
    )

    episode = Client("test").get_episode(episode_id="ep_1")

    assert episode["creation_context"] is None
