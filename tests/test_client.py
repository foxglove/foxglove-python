from datetime import datetime

import pytest
import responses
from faker import Faker
from foxglove_data_platform.client import Client, camelize
from requests.exceptions import RequestException

from .api_url import api_url

fake = Faker()


@responses.activate
def test_error_reason():
    reason = fake.text()
    responses.add(
        responses.GET,
        api_url("/v1/data/coverage"),
        status=403,
        json={"error": reason},
    )
    client = Client("test")
    with pytest.raises(RequestException) as exception:
        client.get_coverage(start=datetime.now(), end=datetime.now())
    assert exception.value.response.reason == reason


@responses.activate
def test_get_coverage():
    responses.add(
        responses.GET,
        api_url("/v1/data/coverage"),
        json=[
            {
                "deviceId": "device_id",
                "start": datetime.now().isoformat(),
                "end": datetime.now().isoformat(),
            }
        ],
    )
    client = Client("test")
    coverage_response = client.get_coverage(start=datetime.now(), end=datetime.now())
    assert len(coverage_response) == 1
    assert coverage_response[0]["device_id"] == "device_id"


@responses.activate
def test_get_topics():
    responses.add(
        responses.GET,
        api_url("/v1/data/topics"),
        json=[
            {
                "topic": "/topic",
                "version": "1",
                "encoding": "ros1",
                "schemaEncoding": "ros1msg",
                "schemaName": "std_msgs/String",
            }
        ],
    )
    client = Client("test")
    topics_response = client.get_topics(
        device_id="device", start=datetime.now(), end=datetime.now()
    )
    assert len(topics_response) == 1
    assert topics_response[0]["topic"] == "/topic"


def test_camelize():
    assert camelize("a_field_name") == "aFieldName"
    assert camelize("aFieldName") == "aFieldName"
    assert camelize(None) is None
