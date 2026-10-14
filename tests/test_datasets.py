import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import requests
import responses
from foxglove.client import Client, DatasetDownloadWarning

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
                "resolvable": True,
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
    created_dataset = {
        **_dataset_json(),
        "added": 1,
        "removed": 0,
        "alreadyPresent": 0,
    }
    updated_dataset = _dataset_json()
    del updated_dataset["episodeCount"]
    responses.add(responses.POST, api_url("/v1/datasets"), json=created_dataset)
    responses.add(responses.GET, api_url("/v1/datasets"), json=[_dataset_json()])
    responses.add(responses.GET, api_url("/v1/datasets/ds_1"), json=_dataset_json())
    responses.add(responses.PATCH, api_url("/v1/datasets/ds_1"), json=updated_dataset)
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
        project_id="prj_1", sort_by="updated_at", limit=10, cursor="next"
    )
    fetched = client.get_dataset(dataset_id="ds_1")
    updated = client.update_dataset(dataset_id="ds_1", description=None)
    client.delete_dataset(dataset_id="ds_1")

    assert created["project_id"] == "prj_1"
    assert created["created_at"] == NOW
    assert created["already_present"] == 0
    assert datasets.items[0]["episode_count"] == 1
    assert "added" not in datasets.items[0]
    assert fetched["name"] == "Successful runs"
    assert updated["description"] == "Training candidates"
    assert "episode_count" not in updated
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

    assert episodes.items[0]["episode"]["id"] == "ep_1"
    assert episodes.items[0]["added_at"] == NOW
    assert result == {"added": 1, "removed": 0, "already_present": 0}


@responses.activate
def test_dataset_version_methods():
    listed_version = _version_json()
    listed_version["changedEpisodeCount"] = 1
    del listed_version["hasMissingRecordings"]
    dataset_episode_change = _dataset_episode_json()
    del dataset_episode_change["hasMissingRecordings"]
    previous_episode = _episode_json("ep_2")
    previous_episode["labels"] = {"note": {"type": "TEXT", "content": "mike"}}
    changed_episode = _dataset_episode_json("ep_2")
    changed_episode["episode"].update(
        version=2,
        latestVersion=2,
        labels={"note": {"type": "TEXT", "content": "brook"}},
    )
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions"),
        json={"versions": [listed_version]},
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
            "changes": [
                {**dataset_episode_change, "change": "added"},
                {
                    **changed_episode,
                    "change": "changed",
                    "previousVersion": 1,
                    "previousEpisode": previous_episode,
                },
            ],
            "addedCount": 1,
            "removedCount": 0,
            "changedCount": 1,
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
        include_labels=True,
    )

    assert versions.items[0]["committed_at"] == NOW
    assert versions.items[0]["changed_episode_count"] == 1
    assert "has_missing_recordings" not in versions.items[0]
    assert version["version_number"] == 1
    assert version["has_missing_recordings"] is False
    assert episodes.items[0]["episode"]["id"] == "ep_1"
    assert episodes.items[0]["episode"]["recordings"][0]["location"] is None
    assert comparison["changes"][0]["change"] == "added"
    assert "has_missing_recordings" not in comparison["changes"][0]
    assert comparison["next_cursor"] == "next"
    assert comparison["changed_count"] == 1
    changed = comparison["changes"][1]
    assert changed["previous_version"] == 1
    assert changed["previous_episode"]["start_time"] == NOW
    assert changed["previous_episode"]["labels"]["note"]["content"] == "mike"
    assert changed["episode"]["version"] == 2
    assert changed["episode"]["labels"]["note"]["content"] == "brook"
    assert responses.calls[3].request.params["include"] == "labels"


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
    responses.add(responses.GET, api_url("/v1/datasets/ds_1"), json=_dataset_json())
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
    assert (output / "episode_0000_ep_1.mcap").read_bytes() == b"episode data"
    assert not (output / ".episode_0000_ep_1.mcap.part").exists()
    assert responses.calls[2].request.params == {
        "sortBy": "startTime",
        "sortOrder": "asc",
        "limit": "2000",
        "include": "labels",
    }
    assert "Authorization" not in responses.calls[4].request.headers
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["formatVersion"] == 1
    assert manifest["dataset"] == {
        "id": "ds_1",
        "name": "Successful runs",
        "projectId": "prj_1",
    }
    assert manifest["episodes"] == [
        {
            "index": 0,
            "id": "ep_1",
            "version": 1,
            "file": "episode_0000_ep_1.mcap",
            "startTime": "2026-09-15T00:00:00.000Z",
            "endTime": "2026-09-15T00:01:00.000Z",
            "metadata": {},
            "byteSize": 12,
            "status": "downloaded",
            "episodeHasMissingRecordings": False,
        }
    ]


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
def test_download_dataset_continues_after_request_failure(tmp_path):
    output = tmp_path / "dataset"
    responses.add(responses.GET, api_url("/v1/datasets/ds_1"), json=_dataset_json())
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

    with pytest.warns(DatasetDownloadWarning):
        Client("test").download_dataset(
            dataset_id="ds_1", version_number=1, output_directory=output
        )
    assert (output / "episode_0000_ep_1.mcap").read_bytes() == b"complete"
    assert not (output / "episode_0001_ep_2.mcap").exists()
    manifest = json.loads((output / "manifest.json").read_text())
    assert [entry["status"] for entry in manifest["episodes"]] == [
        "downloaded",
        "failed",
    ]


def test_download_episode_keeps_partial_bytes(tmp_path, monkeypatch):
    def interrupted_chunks(*, chunk_size):
        assert chunk_size == 32 * 1024
        yield b"partial data"
        raise requests.ConnectionError("interrupted")

    response = MagicMock()
    response.iter_content.return_value = interrupted_chunks(chunk_size=32 * 1024)
    get = MagicMock(return_value=response)
    monkeypatch.setattr(requests, "get", get)

    client = Client("test")
    client._make_stream_link = MagicMock(
        return_value="https://storage.example/ep_1.mcap"
    )
    output = tmp_path / "ep_1.mcap"

    with pytest.raises(requests.ConnectionError):
        client._download_episode_to_file(
            episode_id="ep_1", episode_version=1, output_path=output
        )

    assert not output.exists()
    assert (tmp_path / ".ep_1.mcap.part").read_bytes() == b"partial data"
    response.close.assert_called_once()


@responses.activate
def test_download_dataset_paginates(tmp_path):
    client = Client("test")
    responses.add(responses.GET, api_url("/v1/datasets/ds_1"), json=_dataset_json())
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/1"),
        json={**_version_json(), "episodeCount": 2001},
    )
    url = api_url("/v1/datasets/ds_1/versions/1/episodes")
    responses.add(
        responses.GET,
        url,
        json={"episodes": [_dataset_episode_json(f"ep_{i}") for i in range(2000)]},
        headers={"fg-pagination-next-cursor": "next"},
    )
    responses.add(
        responses.GET, url, json={"episodes": [_dataset_episode_json("ep_last")]}
    )
    client._download_episode_to_file = MagicMock(return_value=0)
    client.download_dataset(
        dataset_id="ds_1",
        version_number=1,
        output_directory=tmp_path / "dataset",
    )

    assert "offset" not in responses.calls[2].request.params
    assert responses.calls[3].request.params["cursor"] == "next"
    assert client._download_episode_to_file.call_count == 2001


@responses.activate
def test_dataset_membership_keeps_string_adds_and_explicit_pins():
    responses.add(
        responses.PATCH,
        api_url("/v1/datasets/ds_1/episodes"),
        json={"added": 1, "updated": 1, "removed": 0, "alreadyPresent": 1},
    )
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/1/episodes"),
        json={"episodes": [_dataset_episode_json()]},
    )
    client = Client("test")

    result = client.update_dataset_episodes(
        dataset_id="ds_1",
        add=["ep_1", {"id": "ep_2", "version": 3}],
        update=[{"id": "ep_1", "version": 2}],
    )
    listed = client.get_dataset_version_episodes(
        dataset_id="ds_1",
        version_number=1,
        include_labels=True,
        label_query="note:mike",
    )

    assert result == {"added": 1, "updated": 1, "removed": 0, "already_present": 1}
    assert json.loads(responses.calls[0].request.body) == {
        "add": ["ep_1", {"id": "ep_2", "version": 3}],
        "update": [{"id": "ep_1", "version": 2}],
    }
    assert listed.items[0]["episode"]["version"] == 1
    assert responses.calls[1].request.params["include"] == "labels"
    assert responses.calls[1].request.params["labelQuery"] == "note:mike"


@responses.activate
def test_dataset_export_keeps_committed_pin_after_episode_changes(tmp_path):
    output = tmp_path / "dataset"
    member = _dataset_episode_json("ep_1")
    member["episode"]["latestVersion"] = 2
    member["episode"]["labels"] = {
        "note": {"type": "TEXT", "content": "mike"},
        "asset": {
            "type": "URI",
            "content": {
                "uri": "s3://bucket/item",
                "checksum": "sha256:abc",
                "contentType": "image/png",
            },
        },
    }
    responses.add(responses.GET, api_url("/v1/datasets/ds_1"), json=_dataset_json())
    responses.add(
        responses.GET, api_url("/v1/datasets/ds_1/versions/1"), json=_version_json()
    )
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/1/episodes"),
        json={"episodes": [member]},
    )
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        json={"link": "https://storage.example/ep_1.mcap"},
    )
    responses.add(responses.GET, "https://storage.example/ep_1.mcap", body=b"E@1")

    Client("test").download_dataset(
        dataset_id="ds_1", version_number=1, output_directory=output
    )

    assert json.loads(responses.calls[3].request.body)["episodeVersion"] == 1
    assert (output / "episode_0000_ep_1.mcap").read_bytes() == b"E@1"
    entry = json.loads((output / "manifest.json").read_text())["episodes"][0]
    assert entry["version"] == 1
    assert entry["labels"] == member["episode"]["labels"]


@responses.activate
def test_dataset_export_rejects_missing_pin_before_transfer(tmp_path):
    output = tmp_path / "dataset"
    member = _dataset_episode_json("ep_1")
    del member["episode"]["version"]
    responses.add(responses.GET, api_url("/v1/datasets/ds_1"), json=_dataset_json())
    responses.add(
        responses.GET, api_url("/v1/datasets/ds_1/versions/1"), json=_version_json()
    )
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/1/episodes"),
        json={"episodes": [member]},
    )

    with pytest.raises(ValueError, match="episode ep_1 has no version pin"):
        Client("test").download_dataset(
            dataset_id="ds_1", version_number=1, output_directory=output
        )
    assert not output.exists()
    assert len(responses.calls) == 3
