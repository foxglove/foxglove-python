from datetime import timedelta

import pytest
import requests
import responses

from foxglove.client import Client, Page
from .api_url import api_url
from .test_datasets import (
    NOW,
    _dataset_json,
    _episode_json,
    _dataset_episode_json,
    _version_json,
)


LIST_CASES = [
    (
        "get_datasets",
        {},
        "/v1/datasets",
        None,
        _dataset_json,
        {"name": "runs", "project_id": "prj_1", "sort_by": "episode_count"},
    ),
    (
        "get_episodes",
        {},
        "/v1/episodes",
        "episodes",
        _episode_json,
        {
            "project_id": "prj_1",
            "include_recordings": True,
            "has_missing_recordings": False,
            "recording_id": "rec_1",
            "start": NOW,
            "end": NOW + timedelta(minutes=1),
        },
    ),
    (
        "get_dataset_episodes",
        {"dataset_id": "ds_1"},
        "/v1/datasets/ds_1/episodes",
        "episodes",
        _dataset_episode_json,
        {
            "include_recordings": True,
            "start": NOW,
            "end": NOW + timedelta(minutes=1),
            "has_missing_recordings": False,
            "recording_id": "rec_1",
            "sort_by": "start_time",
        },
    ),
    (
        "get_dataset_version_episodes",
        {"dataset_id": "ds_1", "version_number": 1},
        "/v1/datasets/ds_1/versions/1/episodes",
        "episodes",
        _dataset_episode_json,
        {"sort_by": "added_at"},
    ),
    (
        "get_dataset_versions",
        {"dataset_id": "ds_1"},
        "/v1/datasets/ds_1/versions",
        "versions",
        _version_json,
        {},
    ),
]


@pytest.mark.parametrize("method,kwargs,path,collection,factory,filters", LIST_CASES)
@responses.activate
def test_pages_are_lazy_and_preserve_query(
    method, kwargs, path, collection, factory, filters
):
    body = {collection: [factory()]} if collection else [factory()]
    responses.add(
        responses.GET,
        api_url(path),
        json=body,
        headers={"fg-pagination-next-cursor": "forward"},
    )
    responses.add(
        responses.GET,
        api_url(path),
        json=body,
        headers={"fg-pagination-previous-cursor": "back"},
    )
    get_page = getattr(Client("test"), method)
    page = get_page(**kwargs, **filters, limit=1, sort_order="asc")
    assert isinstance(page, Page)
    assert page.previous_cursor is None
    assert page.next_cursor == "forward"
    iterator = page.auto_paging_iter()
    assert next(iterator) == page.items[0]
    assert len(responses.calls) == 1
    assert list(iterator) == page.items  # one more page even though limit=1
    assert len(responses.calls) == 2
    first_params = responses.calls[0].request.params
    assert responses.calls[1].request.params == {**first_params, "cursor": "forward"}
    if method == "get_datasets":
        assert first_params["name"] == "runs"
        assert first_params["sortBy"] == "episodeCount"

    page = get_page(**kwargs, **filters, limit=1, sort_order="asc", cursor="forward")
    assert page.next_cursor is None
    assert page.previous_cursor == "back"
    get_page(
        **kwargs, **filters, limit=1, sort_order="asc", cursor=page.previous_cursor
    )
    assert responses.calls[-1].request.params["cursor"] == "back"


@pytest.mark.parametrize("method,kwargs,path,collection,factory,filters", LIST_CASES)
@responses.activate
def test_offset_rules(method, kwargs, path, collection, factory, filters):
    get_page = getattr(Client("test"), method)
    with pytest.raises(ValueError, match="nonzero offset"):
        get_page(**kwargs, cursor="cursor", offset=1)
    assert not responses.calls
    responses.add(
        responses.GET, api_url(path), json={collection: []} if collection else []
    )
    page = get_page(**kwargs, offset=1)
    with pytest.raises(ValueError, match="nonzero offset"):
        list(page.auto_paging_iter())
    page = get_page(**kwargs, offset=0, cursor="cursor", limit=0)
    assert list(page.auto_paging_iter()) == []
    assert page.next_cursor is None


@responses.activate
def test_iteration_propagates_next_page_errors():
    responses.add(
        responses.GET,
        api_url("/v1/datasets"),
        json=[_dataset_json()],
        headers={"fg-pagination-next-cursor": "next"},
    )
    responses.add(
        responses.GET,
        api_url("/v1/datasets"),
        json={"error": "unavailable"},
        status=503,
    )
    iterator = Client("test").get_datasets().auto_paging_iter()
    assert next(iterator)["id"] == "ds_1"
    with pytest.raises(requests.HTTPError):
        next(iterator)


@pytest.mark.parametrize(
    "method,kwargs",
    [
        ("get_episodes", {}),
        ("get_dataset_episodes", {"dataset_id": "ds_1"}),
        ("get_dataset_version_episodes", {"dataset_id": "ds_1", "version_number": 1}),
    ],
)
@pytest.mark.parametrize("bound", ["start", "end"])
@responses.activate
def test_episode_time_filters_require_both_bounds(method, kwargs, bound):
    with pytest.raises(ValueError, match="supplied together"):
        getattr(Client("test"), method)(**kwargs, **{bound: NOW})
    assert not responses.calls


@responses.activate
def test_topics_offset_pagination_keeps_list_return():
    topic = {
        "topic": "/one",
        "version": "v1",
        "encoding": "json",
        "schemaEncoding": "jsonschema",
        "schemaName": "Example",
    }
    responses.add(responses.GET, api_url("/v1/data/topics"), json=[topic])
    responses.add(responses.GET, api_url("/v1/data/topics"), json=[])
    client = Client("test")
    assert client.get_topics(episode_id="ep_1", limit=1)[0]["topic"] == "/one"
    assert client.get_topics(episode_id="ep_1", limit=1, offset=1) == []
    assert responses.calls[1].request.params == {
        "episodeId": "ep_1",
        "limit": "1",
        "offset": "1",
        "includeSchemas": "false",
    }
