from datetime import datetime

import responses
from faker import Faker
from foxglove_data_platform.client import Client

from .api_url import api_url

fake = Faker()


@responses.activate
def test_get_coverage():
    device_id = fake.uuid4()
    responses.add(
        responses.GET,
        api_url(f"/v1/data/coverage"),
        json=[
            {
                "deviceId": device_id,
                "start": datetime.now().isoformat(),
                "end": datetime.now().isoformat(),
            },
            {
                "deviceId": device_id,
                "start": datetime.now().isoformat(),
                "end": datetime.now().isoformat(),
            },
        ],
    )
    client = Client("test")
    coverage = client.get_coverage(
        start=datetime.now(), end=datetime.now(), device_id=device_id
    )
    assert len(coverage) == 2
    assert coverage[0]["device_id"] == device_id
