import json
import io
import datetime
import warnings
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
import responses
from mcap.reader import make_reader
from mcap.writer import Writer

from foxglove.client import Client, DatasetDownloadWarning
from foxglove.client.dataset_download import selection_digest, export_dataset
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


@responses.activate
def test_dataset_download_preserves_attachments_and_manifest_paths(tmp_path):
    mock_selection(count=1, missing=False)
    data = io.BytesIO()
    writer = Writer(data)
    writer.start()
    writer.add_attachment(0, 0, "calibration.json", "application/json", b'{"scale": 1}')
    writer.finish()
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        json={"link": "https://storage.example/episode.mcap"},
    )
    responses.add(
        responses.GET, "https://storage.example/episode.mcap", body=data.getvalue()
    )
    output = run_download(tmp_path, topics=["/camera"])
    request = json.loads(responses.calls[3].request.body)
    assert request["includeAttachments"] is True
    assert request["topics"] == ["/camera"]
    manifest = json.loads((output / "manifest.json").read_text())
    path = output / manifest["episodes"][0]["file"]
    assert path.parent == output
    with path.open("rb") as stream:
        attachments = list(make_reader(stream).iter_attachments())
    assert len(attachments) == 1
    assert attachments[0].name == "calibration.json"
    assert attachments[0].data == b'{"scale": 1}'


@pytest.mark.parametrize("include", [None, False, True])
@responses.activate
def test_stream_attachment_option_preserves_default(include):
    responses.add(
        responses.POST,
        api_url("/v1/data/stream"),
        json={"link": "https://storage.example"},
    )
    kwargs = {} if include is None else {"include_attachments": include}
    Client("test")._make_stream_link(episode_id="ep_1", **kwargs)
    body = json.loads(responses.calls[0].request.body)
    if include is None:
        assert "includeAttachments" not in body
    else:
        assert body["includeAttachments"] is include


@pytest.mark.parametrize("topics", [None, [], ["/a"]])
@responses.activate
def test_partial_and_skipped_episodes(tmp_path, topics):
    mock_selection()
    sign_error("NoStreamableRecordings")
    stream_success()
    with pytest.warns(DatasetDownloadWarning):
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
            assert json.loads(call.request.body)["includeAttachments"] is True
    assert not list(tmp_path.glob("*.part"))


@responses.activate
def test_all_skipped_is_success(tmp_path):
    mock_selection()
    sign_error("NoStreamableRecordings")
    with pytest.warns(DatasetDownloadWarning):
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
    with pytest.warns(DatasetDownloadWarning):
        run_download(tmp_path)
    entries = json.loads((tmp_path / "manifest.json").read_text())["episodes"]
    assert [entry["status"] for entry in entries] == ["failed", "downloaded"]


@responses.activate
def test_failed_request_does_not_prevent_later_success(tmp_path):
    mock_selection()
    sign_error(status=503)
    stream_success()
    with pytest.warns(DatasetDownloadWarning):
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


@pytest.mark.parametrize("outcome", ["complete", "partial", "skipped", "failed"])
@responses.activate
def test_export_warning_counts(tmp_path, outcome):
    mock_selection(missing=outcome == "partial")
    if outcome == "skipped":
        sign_error("NoStreamableRecordings")
    elif outcome == "failed":
        sign_error(status=503)
    stream_success()
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", DatasetDownloadWarning)
        assert run_download(tmp_path) == tmp_path
    if outcome == "complete":
        assert not captured
        return
    assert len(captured) == 1
    assert captured[0].category is DatasetDownloadWarning
    expected = {
        "partial": "0 failed, 0 skipped, 2 downloaded with missing recordings",
        "skipped": "0 failed, 1 skipped, 0 downloaded with missing recordings",
        "failed": "1 failed, 0 skipped, 0 downloaded with missing recordings",
    }
    assert expected[outcome] in str(captured[0].message)
    assert str(tmp_path / "manifest.json") in str(captured[0].message)


@responses.activate
def test_warning_as_error_preserves_written_export(tmp_path):
    mock_selection(count=1)
    stream_success()
    with warnings.catch_warnings():
        warnings.simplefilter("error", DatasetDownloadWarning)
        with pytest.raises(DatasetDownloadWarning):
            run_download(tmp_path)
    entry = json.loads((tmp_path / "manifest.json").read_text())["episodes"][0]
    assert entry["status"] == "downloaded"
    assert (tmp_path / entry["file"]).read_bytes() == b"mcap"


@responses.activate
def test_all_skipped_warns_once(tmp_path):
    mock_selection()
    sign_error("NoStreamableRecordings")
    with pytest.warns(DatasetDownloadWarning, match="0 failed, 2 skipped") as captured:
        assert run_download(tmp_path) == tmp_path
    assert len(captured) == 1


@responses.activate
def test_fatal_request_error_not_replaced_by_warning(tmp_path):
    mock_selection()
    sign_error(status=503)
    with warnings.catch_warnings():
        warnings.simplefilter("error", DatasetDownloadWarning)
        with pytest.raises(RuntimeError, match="No episodes could be downloaded"):
            run_download(tmp_path)


def test_manifest_timestamps_match_app(tmp_path, monkeypatch):
    instant = datetime.datetime(
        2024,
        1,
        1,
        5,
        30,
        0,
        123456,
        tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
    )

    class FixedDatetime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return instant.astimezone(tz)

    monkeypatch.setattr(
        "foxglove.client.dataset_download.datetime.datetime", FixedDatetime
    )
    export_dataset(
        dataset={"id": "ds_1", "name": "test", "project_id": "prj_1"},
        version={"version_number": 1, "episode_count": 1, "committed_at": instant},
        episodes=[
            {
                "episode": {
                    "id": "ep_1",
                    "start_time": instant,
                    "end_time": instant.replace(microsecond=0),
                    "metadata": {},
                },
                "has_missing_recordings": False,
            }
        ],
        destination=tmp_path,
        topics=None,
        download=lambda *_: 0,
    )
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["generatedAt"] == "2024-01-01T00:00:00.123Z"
    assert manifest["version"]["committedAt"] == "2024-01-01T00:00:00.123Z"
    assert manifest["episodes"][0]["startTime"] == "2024-01-01T00:00:00.123Z"
    assert manifest["episodes"][0]["endTime"] == "2024-01-01T00:00:00.000Z"
