from datetime import datetime

import responses
from dateutil.tz import tzoffset
from responses.matchers import json_params_matcher, query_param_matcher

from foxglove.client import Client

from .api_url import api_url

now = datetime.now(tzoffset(None, 0))


def _token_json(**overrides):
    return {
        "id": "sint_1",
        "orgId": "org_1",
        "siteId": "site_1",
        "createdAt": now.isoformat(),
        **overrides,
    }


@responses.activate
def test_get_site_inbox_notification_tokens():
    responses.add(
        responses.GET,
        api_url("/v1/site-inbox-notification-tokens"),
        match=[query_param_matcher({"siteId": "site_1"})],
        json=[_token_json()],
    )
    assert Client("test").get_site_inbox_notification_tokens(site_id="site_1") == [
        {"id": "sint_1", "org_id": "org_1", "site_id": "site_1", "created_at": now}
    ]


@responses.activate
def test_create_site_inbox_notification_token():
    responses.add(
        responses.POST,
        api_url("/v1/site-inbox-notification-tokens"),
        match=[json_params_matcher({"siteId": "site_1"}, strict_match=True)],
        json=_token_json(token="fox_snt_secret"),
    )
    token = Client("test").create_site_inbox_notification_token(site_id="site_1")
    assert token == {
        "id": "sint_1",
        "org_id": "org_1",
        "site_id": "site_1",
        "created_at": now,
        "token": "fox_snt_secret",
    }


@responses.activate
def test_delete_site_inbox_notification_token():
    responses.add(
        responses.DELETE,
        api_url("/v1/site-inbox-notification-tokens/sint_1"),
        json={"id": "sint_1"},
    )
    Client("test").delete_site_inbox_notification_token(
        site_inbox_notification_token_id="sint_1"
    )
    assert len(responses.calls) == 1
