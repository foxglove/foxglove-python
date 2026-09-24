import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
import responses

from foxglove.client import Client
from foxglove.client.dataset_download import selection_digest
from .api_url import api_url
from .test_datasets import _dataset_json, _version_json, _dataset_episode_json


def mock_selection(*, count=2, missing=True):
    responses.add(responses.GET, api_url("/v1/datasets/ds_1"), json=_dataset_json())
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/1"),
        json={**_version_json(missing=missing), "episodeCount": count},
    )
    responses.add(
        responses.GET,
        api_url("/v1/datasets/ds_1/versions/1/episodes"),
        json={
            "episodes": [
                _dataset_episode_json(f"ep_{i}", missing=missing) for i in range(count)
            ]
        },
    )


def sign_error(code=None, status=404):
    body = {"error": "unavailable"}
    if code:
        body["code"] = code
    responses.add(responses.POST, api_url("/v1/data/stream"), json=body, status=status)


def stream_success():
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        json={"link": "https://storage.example/episode.mcap"},
    )
    responses.add(responses.GET, "https://storage.example/episode.mcap", body=b"mcap")


def run_download(tmp_path, **kwargs):
    return Client("test").download_dataset(
        dataset_id="ds_1", version_number=1, output_directory=tmp_path, **kwargs
    )


@pytest.mark.parametrize("topics", [None, [], ["/a"]])
@responses.activate
def test_partial_and_skipped_episodes(tmp_path, topics):
    mock_selection()
    sign_error("NoStreamableRecordings")
    stream_success()
    assert run_download(tmp_path, topics=topics) == tmp_path
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    skipped, downloaded = manifest["episodes"]
    assert skipped["status"] == "skipped"
    assert "file" not in skipped
    assert "Import those recordings" in skipped["reason"]
    assert downloaded["status"] == "downloaded"
    assert downloaded["episodeHasMissingRecordings"] is True
    assert (tmp_path / downloaded["file"]).read_bytes() == b"mcap"
    if topics:
        assert manifest["selection"]["topics"] == topics
    else:
        assert "topics" not in manifest["selection"]
    for call in responses.calls:
        if call.request.method == "POST":
            assert json.loads(call.request.body)["topics"] == (topics or [])
    assert not list(tmp_path.glob("*.part"))


@responses.activate
def test_all_skipped_is_success(tmp_path):
    mock_selection()
    sign_error("NoStreamableRecordings")
    run_download(tmp_path)
    assert [path.name for path in tmp_path.iterdir()] == ["manifest.json"]
    assert all(
        entry["status"] == "skipped"
        for entry in json.loads((tmp_path / "manifest.json").read_text())["episodes"]
    )


@pytest.mark.parametrize("status", [404, 403, 503])
@responses.activate
def test_ordinary_errors_are_failed_and_all_failures_raise(tmp_path, status):
    mock_selection()
    sign_error(status=status)
    with pytest.raises(RuntimeError, match="No episodes could be downloaded"):
        run_download(tmp_path)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert [entry["status"] for entry in manifest["episodes"]] == ["failed", "failed"]
    assert all(str(status) in entry["reason"] for entry in manifest["episodes"])


@responses.activate
def test_signed_storage_404_is_not_skipped(tmp_path):
    mock_selection(count=1)
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        json={"link": "https://storage.example/episode.mcap?secret=token"},
    )
    responses.add(
        responses.GET,
        "https://storage.example/episode.mcap?secret=token",
        status=404,
        json={"code": "NoStreamableRecordings"},
    )
    with pytest.raises(RuntimeError, match="No episodes"):
        run_download(tmp_path)
    text = (tmp_path / "manifest.json").read_text()
    assert "secret" not in text
    assert json.loads(text)["episodes"][0]["status"] == "failed"


@responses.activate
def test_non_object_error_response_does_not_stop_download(tmp_path):
    mock_selection()
    responses.add(responses.POST, api_url("/v1/data/stream"), json=[], status=404)
    stream_success()
    run_download(tmp_path)
    entries = json.loads((tmp_path / "manifest.json").read_text())["episodes"]
    assert [entry["status"] for entry in entries] == ["failed", "downloaded"]


@responses.activate
def test_failed_request_does_not_prevent_later_success(tmp_path):
    mock_selection()
    sign_error(status=503)
    stream_success()
    run_download(tmp_path)
    entries = json.loads((tmp_path / "manifest.json").read_text())["episodes"]
    assert [entry["status"] for entry in entries] == ["failed", "downloaded"]


@responses.activate
def test_interrupted_transfer_preserves_files_and_manifest(tmp_path, monkeypatch):
    mock_selection(count=3)
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        json={"link": "https://storage.example/episode.mcap"},
    )
    original = requests.ConnectionError("interrupted")

    def interrupted(*, chunk_size):
        yield b"partial"
        raise original

    complete, partial = MagicMock(), MagicMock()
    complete.iter_content.return_value = [b"complete"]
    partial.iter_content.side_effect = interrupted
    monkeypatch.setattr(requests, "get", MagicMock(side_effect=[complete, partial]))
    with pytest.raises(requests.ConnectionError) as raised:
        run_download(tmp_path)
    assert raised.value is original
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["selection"]["episodeCount"] == 3
    assert [entry["status"] for entry in manifest["episodes"]] == [
        "downloaded",
        "failed",
    ]
    assert (tmp_path / "episode_0000_ep_0.mcap").read_bytes() == b"complete"
    assert (tmp_path / ".episode_0001_ep_1.mcap.part").read_bytes() == b"partial"
    complete.close.assert_called_once()
    partial.close.assert_called_once()


@responses.activate
def test_filesystem_failure_preserves_original_even_if_manifest_fails(
    tmp_path, monkeypatch
):
    mock_selection()
    stream_success()
    original = OSError("disk full")
    monkeypatch.setattr(Path, "open", MagicMock(side_effect=original))
    with pytest.raises(OSError) as raised:
        run_download(tmp_path)
    assert raised.value is original


@responses.activate
def test_existing_output_is_never_overwritten(tmp_path):
    existing = tmp_path / "keep.txt"
    existing.write_text("keep")
    responses.add(
        responses.GET, api_url("/v1/datasets/ds_1/versions/1"), json=_version_json()
    )
    with pytest.raises(RuntimeError, match="empty"):
        run_download(tmp_path)
    assert existing.read_text() == "keep"


@pytest.mark.parametrize(
    "topics,digest",
    [
        (None, "c65436f9e9c5a225cb738a1cfb0e15a1394b84fd4de890937d871498f47800ae"),
        (
            ["/b", "/a"],
            "cff9c74162c5488d22376749f20e6e4090db6cd21383eb500b848ca4b4ba2d2b",
        ),
        (
            ["/\ue000", "/😀"],
            "830455c1642352826c36ce205b324b1d6a5d54f0de3de9aafe9d64ae4af53fe2",
        ),
    ],
)
def test_selection_digest_matches_app_javascript(topics, digest):
    # Expected values generated with the app's JSON.stringify + UTF-16 sort + SHA-256.
    assert selection_digest("ds_1", 1, ["ep_b", "ep_a"], topics) == digest
