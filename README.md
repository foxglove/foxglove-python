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
