"""Directory exports using the app's dataset download manifest format."""

import datetime
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests


class EpisodeRequestError(Exception):
    """A stream could not be opened; no output file has been written."""

    def __init__(self, reason: str, *, unavailable: bool = False):
        super().__init__(reason)
        self.unavailable = unavailable


def _request_error(
    error: requests.RequestException, *, signing: bool
) -> EpisodeRequestError:
    response = error.response
    if signing and response is not None and response.status_code == 404:
        try:
            body = response.json()
            if isinstance(body, dict) and body.get("code") == "NoStreamableRecordings":
                return EpisodeRequestError(
                    "No Primary Site holds data for any recording in this episode. "
                    "Import those recordings to include it.",
                    unavailable=True,
                )
        except ValueError:
            pass
    # requests exception strings can contain signed URLs. Keep credentials out of manifests.
    reason = "Stream request failed"
    if response is not None:
        reason += f" with status {response.status_code}"
    else:
        reason += f" ({type(error).__name__})"
    return EpisodeRequestError(reason)


def download_episode(output_path: Path, get_link: Callable[[], str]) -> int:
    try:
        link = get_link()
    except requests.RequestException as error:
        raise _request_error(error, signing=True) from error
    try:
        response = requests.get(link, stream=True)
    except requests.RequestException as error:
        raise _request_error(error, signing=False) from error
    try:
        try:
            response.raise_for_status()
        except requests.RequestException as error:
            raise _request_error(error, signing=False) from error
        temporary_path = output_path.with_name(f".{output_path.name}.part")
        size = 0
        with temporary_path.open("wb") as output:
            for chunk in response.iter_content(chunk_size=32 * 1024):
                output.write(chunk)
                size += len(chunk)
        temporary_path.replace(output_path)
        return size
    finally:
        response.close()


def selection_digest(
    dataset_id: str,
    version_number: int,
    episode_ids: List[str],
    topics: Optional[List[str]],
) -> str:
    # JavaScript Array.sort compares UTF-16 code units, including for topic names.
    def js_sort_key(value: str) -> bytes:
        return value.encode("utf-16-be", errors="surrogatepass")

    selection: Dict[str, Any] = {
        "datasetId": dataset_id,
        "versionNumber": version_number,
        "episodeIds": sorted(episode_ids, key=js_sort_key),
    }
    if topics is not None:
        selection["topics"] = sorted(topics, key=js_sort_key)
    canonical = json.dumps(selection, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _write_manifest(destination: Path, manifest: Dict[str, Any]) -> None:
    temporary_path = destination / ".manifest.json.part"
    with temporary_path.open("w", encoding="utf-8") as output:
        json.dump(manifest, output, ensure_ascii=False, indent=2)
        output.write("\n")
    temporary_path.replace(destination / "manifest.json")


def export_dataset(
    *,
    dataset: Dict[str, Any],
    version: Dict[str, Any],
    episodes: List[Dict[str, Any]],
    destination: Path,
    topics: Optional[List[str]],
    download: Callable[[str, Path], int],
) -> Path:
    entries: List[Dict[str, Any]] = []
    selection: Dict[str, Any] = {
        "digest": selection_digest(
            dataset["id"],
            version["version_number"],
            [item["episode"]["id"] for item in episodes],
            topics,
        ),
        "episodeCount": version["episode_count"],
    }
    if topics is not None:
        selection["topics"] = topics
    manifest = {
        "formatVersion": 1,
        "generatedAt": datetime.datetime.now(datetime.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        "dataset": {
            "id": dataset["id"],
            "name": dataset["name"],
            "projectId": dataset["project_id"],
        },
        "version": {
            "versionNumber": version["version_number"],
            "committedAt": version["committed_at"].isoformat(),
        },
        "selection": selection,
        "episodes": entries,
    }
    downloaded = 0
    failed = 0
    try:
        for index, member in enumerate(episodes):
            episode = member["episode"]
            entry = {
                "index": index,
                "id": episode["id"],
                "startTime": episode["start_time"].isoformat(),
                "endTime": episode["end_time"].isoformat(),
                "metadata": episode["metadata"],
            }
            safe_id = re.sub(r"[^A-Za-z0-9._-]+", "-", episode["id"])
            filename = f"episode_{index:04d}_{safe_id}.mcap"
            try:
                size = download(episode["id"], destination / filename)
            except EpisodeRequestError as error:
                entry.update(
                    status="skipped" if error.unavailable else "failed",
                    reason=str(error),
                )
                if not error.unavailable:
                    failed += 1
            except BaseException as error:
                entry.update(
                    status="failed",
                    reason=f"Download interrupted ({type(error).__name__})",
                )
                entries.append(entry)
                raise
            else:
                downloaded += 1
                entry.update(
                    status="downloaded",
                    file=filename,
                    byteSize=size,
                    episodeHasMissingRecordings=member["has_missing_recordings"],
                )
            entries.append(entry)
    except BaseException:
        try:
            _write_manifest(destination, manifest)
        except Exception:
            pass  # Preserve the transfer/filesystem error that interrupted the export.
        raise
    _write_manifest(destination, manifest)
    if downloaded == 0 and failed:
        raise RuntimeError("No episodes could be downloaded; see manifest.json")
    return destination
