# Python Client Library for the Foxglove API

[![foxglove-client on PyPI](https://img.shields.io/pypi/v/foxglove-client?label=pypi%20%7C%20foxglove-client)](https://pypi.org/project/foxglove-client/)

This library provides a convenient python client for [Foxglove](https://foxglove.dev/).

In order to use the client you will first have to create an API token for your organization on your organization's [settings page](https://app.foxglove.dev/~/settings).

Read more about the API in the [Foxglove API Docs](https://docs.foxglove.dev/api/).

## Sample Usage

Examples of various client features can be found in the `examples` directory.

## Running Tests

```bash
pipenv install --dev
pipenv run python -m pytest
```

## Release Process

Release numbering follows a major.minor.patch format, abbreviated as "X.Y.Z" below.

CI will build the package and publish to PyPI once tags are pushed, as described below.

1. Update the `version` in setup.cfg with the new version `X.Y.Z`
2. Draft a [release on GitHub](https://github.com/foxglove/foxglove-python/releases/new) and create a new tag `releases/vX.Y.Z`
3. Generate release notes, review, and publish the release

## Stay in touch

Join our [Discord](https://foxglove.dev/chat) to ask questions, share feedback, and stay up to date on what our team is working on.
