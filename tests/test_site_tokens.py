from datetime import datetime

import responses
from dateutil.tz import tzoffset
from responses.matchers import json_params_matcher, query_param_matcher

from foxglove.client import Client

from .api_url import api_url

now = datetime.now(tzoffset(None, 0))


def _site_token_json(**overrides):
    return {
        "id": "stkn_1",
        "siteId": "site_1",
        "createdAt": now.isoformat(),
        **overrides,
    }


@responses.activate
def test_get_site_tokens():
    responses.add(
        responses.GET,
        api_url("/v1/site-tokens"),
        match=[query_param_matcher({"siteId": "site_1"})],
        json=[_site_token_json()],
    )
    assert Client("test").get_site_tokens(site_id="site_1") == [
        {"id": "stkn_1", "site_id": "site_1", "created_at": now}
    ]


@responses.activate
def test_create_site_token():
    responses.add(
        responses.POST,
        api_url("/v1/site-tokens"),
        match=[json_params_matcher({"siteId": "site_1"}, strict_match=True)],
        json=_site_token_json(token="fox_st_secret"),
    )
    assert Client("test").create_site_token(site_id="site_1") == {
        "id": "stkn_1",
        "site_id": "site_1",
        "created_at": now,
        "token": "fox_st_secret",
    }


@responses.activate
def test_delete_site_token():
    responses.add(
        responses.DELETE, api_url("/v1/site-tokens/stkn_1"), json={"success": True}
    )
    Client("test").delete_site_token(site_token_id="stkn_1")
    assert len(responses.calls) == 1
