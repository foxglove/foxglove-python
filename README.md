# Python Client Library for the Foxglove API

[![foxglove-client on PyPI](https://img.shields.io/pypi/v/foxglove-client?label=pypi%20%7C%20foxglove-client)](https://pypi.org/project/foxglove-client/)

A convenient python client for [Foxglove](https://foxglove.dev/).

## Getting Started

Install from [PyPI](https://pypi.org/project/foxglove-client/):

```
pip install foxglove-client
```

Create an API token for your organization on your organization's [settings page](https://app.foxglove.dev/~/settings) and read more in the [Foxglove API Docs](https://docs.foxglove.dev/api/).

Examples of various client features can be found in the `examples` directory.

## Datasets and episodes

Create episodes from recordings, collect them into a versioned dataset, and download a committed
version as one MCAP file per episode:

```python
from foxglove.client import Client

client = Client(token="<YOUR API TOKEN HERE>")
episodes = client.create_episodes(
    project_id="prj_example",
    episodes=[{"recordings": ["rec_example"]}],
)
dataset = client.create_dataset(
    project_id="prj_example",
    name="Training candidates",
    episode_ids=[episodes[0]["id"]],
)
committed = client.commit_dataset(dataset_id=dataset["id"])
client.download_dataset(
    dataset_id=dataset["id"],
    version_number=committed["committed"]["version_number"],
    output_directory="./training-candidates",
)
```

The API token needs the relevant `episodes.*` and `datasets.*` capabilities, plus `data.stream` to
download dataset contents. Datasets must also be enabled for the organization.

Dataset, episode, dataset membership, and version list methods return a `Page`:

```python
page = client.get_datasets(project_id="prj_example", name="Training", limit=100)
for dataset in page.items:  # Only the page already fetched
    print(dataset["name"])

for dataset in page.auto_paging_iter():  # This page, then further pages on demand
    print(dataset["name"])

if page.next_cursor is not None:
    next_page = client.get_datasets(
        project_id="prj_example", name="Training", limit=100, cursor=page.next_cursor
    )
```

`limit` sets the page size, not a total cap on automatic iteration. Keep filters and ordering the
same when using `next_cursor` or `previous_cursor`. Cursors are opaque and pagination does not create
a snapshot of a mutable collection. A nonzero `offset` uses deprecated single-page pagination and
cannot be combined with a cursor or `auto_paging_iter()`. Previously released list methods keep their
existing list return values. `get_topics()` supports `limit` and `offset`: request successive pages
with increasing offsets until a page contains fewer items than the requested limit.

Dataset downloads require a new or empty output directory and write `episode_0000_<id>.mcap` files
plus a `manifest.json` using the app's schema and selection digest. MCAP downloads include
recording attachments, matching the app. Pass `topics=["/camera", "/joint_states"]` to select topics;
omitting `topics` or passing `[]` downloads all topics. The exporter gathers episode metadata across
all pages to identify the selection, then streams each MCAP directly to disk.

In Python exports, each manifest `file` path is relative to the returned `output_directory`,
which also contains `manifest.json`: resolve it as `output_directory / entry["file"]`.
The app's ZIP exports instead use ZIP-root-relative paths such as
`dataset-v1/episode_0000_<id>.mcap`, with the manifest also inside `dataset-v1/`.
Do not resolve app ZIP paths relative to the manifest's directory. Python exports do not
add that enclosing archive folder.

Read the manifest to check completeness: successful episodes have `status: "downloaded"`, a relative
`file` path, and `byteSize`. `episodeHasMissingRecordings: true` marks a download containing only the
remaining streamable recordings. An episode with no streamable recordings is `skipped`, with a reason;
an all-skipped export succeeds with only the manifest. Other request failures before streaming are
recorded as `failed` and the export continues. If no download succeeds and any fail, the exporter
raises after writing the manifest.

Incomplete exports emit `DatasetDownloadWarning` (from `foxglove.client`) with counts of
failed, skipped, and downloaded-but-partial episodes and the manifest location. The return
value is still a `Path`. You can suppress this category or promote it to an error with
`warnings.filterwarnings("error", category=DatasetDownloadWarning)`; the files and manifest
have already been written when the warning is emitted. Manifest timestamps use UTC with
millisecond precision and a `Z` suffix, matching the app.

An interrupted transfer or filesystem failure stops the export and re-raises the original error.
Completed MCAPs and the failed episode's `.part` file remain; a manifest is written atomically if
possible. Its episode entries may be incomplete, while `selection.episodeCount` and the selection
digest describe the entire selected version. A `.part` file is incomplete and must not be used as an
MCAP. Retrying requires a new empty directory; automatic resume is not supported.

## Development

### Running Tests

```bash
uv sync --dev
uv run python -m pytest
```

In addition to unit tests, all PRs that change behavior should also be tested against the Foxglove API.

### Release Process

Release numbering follows a major.minor.patch format, abbreviated as "X.Y.Z" below.

CI will build the package and publish to PyPI once tags are pushed, as described below.

1. Update the `version` in pyproject.toml with the new version `X.Y.Z`
2. Draft a [release on GitHub](https://github.com/foxglove/foxglove-python/releases/new) and create a new tag `releases/vX.Y.Z`
3. Generate release notes, review, and publish the release

## Stay in touch

Join our [Discord](https://foxglove.dev/chat) to ask questions, share feedback, and stay up to date on what our team is working on.
