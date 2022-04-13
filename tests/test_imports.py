from datetime import datetime

import responses
from faker import Faker
from foxglove_data_platform.client import Client

from .api_url import api_url

fake = Faker()


@responses.activate
def test_delete_import():
    device_id = fake.uuid4()
    import_id = fake.uuid4()
    responses.add(
        responses.DELETE,
        api_url(f"/v1/data/imports/{import_id}?deviceId={device_id}"),
    )
    try:
        client = Client("test")
        client.delete_import(device_id=device_id, import_id=import_id)
    except:
        assert False


@responses.activate
def test_get_imports():
    device_id = "my_device_id"
    import_id = "my_device_id"
    responses.add(
        responses.GET,
        api_url(f"/v1/data/imports"),
        json=[
            {
                "importId": import_id,
                "deviceId": device_id,
                "importTime": datetime.now().isoformat(),
                "start": datetime.now().isoformat(),
                "end": datetime.now().isoformat(),
                "metadata": {},
                "inputType": "bag",
                "outputType": "mcap0",
                "filename": "test.bag",
                "inputSize": 1024,
                "totalOutputSize": 1024,
            }
        ],
    )
    client = Client("test")
    imports = client.get_imports(device_id=device_id)
    assert len(imports) == 1
    assert imports[0]["device_id"] == device_id
