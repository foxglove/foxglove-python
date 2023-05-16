from datetime import datetime
from dateutil.tz import tzoffset

import responses
from faker import Faker
from foxglove_data_platform.client import Client

from .api_url import api_url

fake = Faker()


@responses.activate
def test_get_recordings():
    device_id = fake.uuid4()
    recording_id_a = fake.uuid4()
    recording_id_b = fake.uuid4()
    path = fake.file_name(extension="mcap")
    size = fake.random_number()
    message_count = fake.random_number()
    site_id = fake.uuid4()
    edge_site_id = fake.uuid4()
    now = datetime.now(tzoffset(None, 0))

    responses.add(
        responses.GET,
        api_url(f"/v1/recordings"),
        json=[
            {
                "id": recording_id_a,
                "path": path,
                "size": size,
                "messageCount": message_count,
                "createdAt": now.isoformat(),
                "importedAt": now.isoformat(),
                "start": now.isoformat(),
                "end": now.isoformat(),
                "importStatus": "complete",
                "site": {"id": site_id, "name": "primarySite"},
                "device": {"id": device_id, "name": "deviceName"},
                "metadata": {"hey": "now", "brown": "cow"},
            },
            {
                "id": recording_id_b,
                "path": path,
                "size": size,
                "messageCount": message_count,
                "createdAt": now.isoformat(),
                "start": now.isoformat(),
                "end": now.isoformat(),
                "importStatus": "none",
                "edgeSite": {"id": edge_site_id, "name": "edgeSite"},
                "device": {"id": device_id, "name": "deviceName"},
            },
        ],
    )
    client = Client("test")
    recordings = client.get_recordings(
        start=datetime.now(),
        end=datetime.now(),
        device_id=device_id,
    )
    assert recordings == [
        {
            "id": recording_id_a,
            "path": path,
            "size": size,
            "message_count": message_count,
            "created_at": now,
            "imported_at": now,
            "start": now,
            "end": now,
            "import_status": "complete",
            "site": {"id": site_id, "name": "primarySite"},
            "edge_site": None,
            "device": {"id": device_id, "name": "deviceName"},
            "metadata": {"hey": "now", "brown": "cow"},
        },
        {
            "id": recording_id_b,
            "path": path,
            "size": size,
            "message_count": message_count,
            "created_at": now,
            "start": now,
            "end": now,
            "import_status": "none",
            "imported_at": None,
            "site": None,
            "edge_site": {"id": edge_site_id, "name": "edgeSite"},
            "device": {"id": device_id, "name": "deviceName"},
            "metadata": None,
        },
    ]
