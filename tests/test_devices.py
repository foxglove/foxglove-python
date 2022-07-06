from datetime import datetime

import responses
from faker import Faker
from foxglove_data_platform.client import Client

from .api_url import api_url

fake = Faker()


@responses.activate
def test_create_device():
    id = fake.uuid4()
    name = "name"
    serial_number = "serial"
    responses.add(
        responses.POST,
        api_url("/v1/devices"),
        json={
            "id": id,
            "name": name,
            "serialNumber": serial_number,
        },
    )
    client = Client("test")
    device = client.create_device(name=name, serial_number=serial_number)
    assert device["serial_number"] == serial_number


@responses.activate
def test_get_device():
    id = fake.uuid4()
    responses.add(
        responses.GET,
        api_url(f"/v1/devices/{id}"),
        json={
            "id": id,
            "name": fake.sentence(2),
            "serialNumber": fake.pyint(),
            "createdAt": datetime.now().isoformat(),
            "updatedAt": datetime.now().isoformat(),
        },
    )
    client = Client("test")
    device = client.get_device(device_id=id)
    assert device["id"] == id


@responses.activate
def test_get_devices():
    id = fake.uuid4()
    responses.add(
        responses.GET,
        api_url("/v1/devices"),
        json=[
            {
                "id": id,
                "name": fake.sentence(2),
                "serialNumber": fake.pyint(),
                "createdAt": datetime.now().isoformat(),
                "updatedAt": datetime.now().isoformat(),
            }
        ],
    )
    client = Client("test")
    devices = client.get_devices()
    assert len(devices) == 1
    assert devices[0]["id"] == id


@responses.activate
def test_delete_device():
    id = fake.uuid4()
    name = "name"
    serial_number = "serial"
    responses.add(
        responses.DELETE,
        api_url(f"/v1/devices/{id}"),
        json={"success": True},
    )
    client = Client("test")
    try:
        client.delete_device(device_id=id)
    except:
        assert False
