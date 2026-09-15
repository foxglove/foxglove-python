import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import requests
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
        "metadata": {},
        "createdAt": NOW.isoformat(),
    }
    if include_recordings:
        episode["recordings"] = [
            {
                "id": "rec_1",
                "path": "run.mcap",
                "start": NOW.isoformat(),
                "end": NOW.replace(minute=1).isoformat(),
                "available": True,
            }
        ]
    return episode


def _dataset_json():
    return {
        "id": "ds_1",
        "projectId": "prj_1",
        "name": "Successful runs",
        "description": "Training candidates",
        "createdAt": NOW.isoformat(),
        "updatedAt": NOW.isoformat(),
        "episodeCount": 1,
    }


def _version_json(version_number=1, *, committed=True, missing=False):
    result = {
        "versionNumber": version_number,
        "createdAt": NOW.isoformat(),
        "episodeCount": 1,
        "addedEpisodeCount": 1,
        "removedEpisodeCount": 0,
        "hasMissingRecordings": missing,
    }
    if committed:
        result["committedAt"] = NOW.isoformat()
    return result


def _dataset_episode_json(episode_id="ep_1", *, missing=False):
    return {
        "addedAt": NOW.isoformat(),
        "addedInVersion": 1,
        "hasMissingRecordings": missing,
        "episode": _episode_json(episode_id, include_recordings=True),
    }


@responses.activate
def test_dataset_metadata_methods():
    responses.add(responses.POST, api_url("/v1/datasets"), json=_dataset_json())
    responses.add(responses.GET, api_url("/v1/datasets"), json=[_dataset_json()])
    responses.add(responses.GET, api_url("/v1/datasets/ds_1"), json=_dataset_json())
    responses.add(responses.PATCH, api_url("/v1/datasets/ds_1"), json=_dataset_json())
    responses.add(
        responses.DELETE, api_url("/v1/datasets/ds_1"), json={"success": True}
    )
    client = Client("test")

    created = client.create_dataset(
        project_id="prj_1",
        name="Successful runs",
        description="Training candidates",
        episode_ids=["ep_1"],
    )
    datasets = client.get_datasets(
        project_id="prj_1", sort_by="updated_at", limit=10, offset=2
    )
    fetched = client.get_dataset(dataset_id="ds_1")
    updated = client.update_dataset(dataset_id="ds_1", description=None)
    client.delete_dataset(dataset_id="ds_1")

    assert created["project_id"] == "prj_1"
    assert created["created_at"] == NOW
    assert datasets[0]["episode_count"] == 1
    assert fetched["name"] == "Successful runs"
    assert updated["description"] == "Training candidates"
    assert json.loads(responses.calls[3].request.body) == {"description": None}


@responses.activate
def test_dataset_episode_methods():
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/episodes"),
        json={"episodes": [_dataset_episode_json()]},
    )
    responses.add(
        responses.PATCH,
        api_url("/v1/datasets/ds_1/episodes"),
        json={"added": 1, "removed": 0, "alreadyPresent": 0},
    )
    client = Client("test")

    episodes = client.get_dataset_episodes(dataset_id="ds_1", include_recordings=True)
    result = client.update_dataset_episodes(dataset_id="ds_1", add=["ep_1"])

    assert episodes[0]["episode"]["id"] == "ep_1"
    assert episodes[0]["added_at"] == NOW
    assert result == {"added": 1, "removed": 0, "already_present": 0}


@responses.activate
def test_dataset_version_methods():
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions"),
        json={"versions": [_version_json()]},
    )
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/1"),
        json=_version_json(),
    )
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/1/episodes"),
        json={"episodes": [_dataset_episode_json()]},
    )
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/2/compare"),
        json={
            "changes": [{**_dataset_episode_json(), "change": "added"}],
            "addedCount": 1,
            "removedCount": 0,
            "nextCursor": "next",
        },
    )
    client = Client("test")

    versions = client.get_dataset_versions(dataset_id="ds_1", sort_order="asc")
    version = client.get_dataset_version(dataset_id="ds_1", version_number=1)
    episodes = client.get_dataset_version_episodes(dataset_id="ds_1", version_number=1)
    comparison = client.compare_dataset_versions(
        dataset_id="ds_1",
        version_number=2,
        base_version=1,
        limit=5,
        cursor="cursor",
    )

    assert versions[0]["committed_at"] == NOW
    assert version["version_number"] == 1
    assert episodes[0]["episode"]["id"] == "ep_1"
    assert comparison["changes"][0]["change"] == "added"
    assert comparison["next_cursor"] == "next"


@responses.activate
def test_dataset_version_actions():
    responses.add(
        responses.POST,
        api_url("/v1/datasets/ds_1/commit"),
        json={"committed": _version_json(), "editableVersionNumber": 2},
    )
    responses.add(
        responses.POST,
        api_url("/v1/datasets/ds_1/discard"),
        json={"discardedAdds": 1, "discardedRemoves": 2},
    )
    responses.add(
        responses.POST,
        api_url("/v1/datasets/ds_1/versions/1/restore"),
        json={"added": 1, "removed": 2, "discardedAdds": 3, "discardedRemoves": 4},
    )
    client = Client("test")

    committed = client.commit_dataset(dataset_id="ds_1")
    discarded = client.discard_dataset(dataset_id="ds_1")
    restored = client.restore_dataset_version(
        dataset_id="ds_1", version_number=1, force=True
    )

    assert committed["editable_version_number"] == 2
    assert discarded == {"discarded_adds": 1, "discarded_removes": 2}
    assert restored["discarded_removes"] == 4
    assert responses.calls[2].request.params == {"force": "true"}


@responses.activate
def test_download_dataset(tmp_path):
    output = tmp_path / "dataset"
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/1"),
        json=_version_json(),
    )
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/1/episodes"),
        json={"episodes": [_dataset_episode_json("ep_1")]},
    )
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        json={"link": "https://storage.example/ep_1.mcap"},
    )
    responses.add(
        responses.GET, "https://storage.example/ep_1.mcap", body=b"episode data"
    )

    result = Client("test").download_dataset(
        dataset_id="ds_1", version_number=1, output_directory=output
    )

    assert result == output
    assert (output / "ep_1.mcap").read_bytes() == b"episode data"
    assert not (output / ".ep_1.mcap.part").exists()
    assert "Authorization" not in responses.calls[3].request.headers


@responses.activate
def test_download_dataset_rejects_editable_version(tmp_path):
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/2"),
        json=_version_json(2, committed=False),
    )

    with pytest.raises(RuntimeError) as raised:
        Client("test").download_dataset(
            dataset_id="ds_1",
            version_number=2,
            output_directory=tmp_path / "dataset",
        )

    assert str(raised.value) == "Cannot download an editable dataset version"


@responses.activate
def test_download_dataset_removes_partial_file_and_keeps_completed_files(tmp_path):
    output = tmp_path / "dataset"
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/1"),
        json=_version_json(),
    )
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/1/episodes"),
        json={
            "episodes": [
                _dataset_episode_json("ep_1"),
                _dataset_episode_json("ep_2"),
            ]
        },
    )
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        json={"link": "https://storage.example/ep_1.mcap"},
    )
    responses.add(responses.GET, "https://storage.example/ep_1.mcap", body=b"complete")
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        json={"link": "https://storage.example/ep_2.mcap"},
    )
    responses.add(
        responses.GET,
        "https://storage.example/ep_2.mcap",
        body=requests.ConnectionError("interrupted"),
    )

    with pytest.raises(RuntimeError) as raised:
        Client("test").download_dataset(
            dataset_id="ds_1", version_number=1, output_directory=output
        )

    assert str(raised.value) == "Failed to download dataset episode ep_2"
    assert isinstance(raised.value.__cause__, requests.ConnectionError)

    assert (output / "ep_1.mcap").read_bytes() == b"complete"
    assert not (output / "ep_2.mcap").exists()
    assert not (output / ".ep_2.mcap.part").exists()


def test_download_dataset_paginates(tmp_path):
    client = Client("test")
    client.get_dataset_version = MagicMock(
        return_value={"committed_at": NOW, "has_missing_recordings": False}
    )
    first_page = [
        {
            "episode": {"id": f"ep_{index}"},
            "has_missing_recordings": False,
        }
        for index in range(2000)
    ]
    client.get_dataset_version_episodes = MagicMock(side_effect=[first_page, []])
    client._download_episode_to_file = MagicMock()

    client.download_dataset(
        dataset_id="ds_1",
        version_number=1,
        output_directory=tmp_path / "dataset",
    )

    assert client.get_dataset_version_episodes.call_count == 2
    assert client.get_dataset_version_episodes.call_args_list[0].kwargs["offset"] == 0
    assert (
        client.get_dataset_version_episodes.call_args_list[1].kwargs["offset"] == 2000
    )
    assert client._download_episode_to_file.call_count == 2000
