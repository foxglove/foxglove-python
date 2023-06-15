from datetime import datetime

import responses
from faker import Faker
from foxglove_data_platform.client import Client
from responses.matchers import json_params_matcher

from .api_url import api_url

fake = Faker()


@responses.activate
def test_create_device():
    id = fake.uuid4()
    name = "name"
    responses.add(
        responses.POST,
        api_url("/v1/devices"),
        match=[json_params_matcher({"name": name}, strict_match=True)],
        json={
            "id": id,
            "name": name,
        },
    )
    client = Client("test")
    device = client.create_device(name=name)
    assert device["name"] == name


@responses.activate
def test_create_device_with_properties():
    id = fake.uuid4()
    name = "name"
    properties = {"sn": 1}
    responses.add(
        responses.POST,
        api_url("/v1/devices"),
        match=[
            json_params_matcher(
                {"name": name, "properties": properties}, strict_match=True
            )
        ],
        json={
            "id": id,
            "name": name,
            "properties": properties,
        },
    )
    client = Client("test")
    device = client.create_device(name=name, properties=properties)
    assert device["name"] == name
    assert device["properties"] == properties


@responses.activate
def test_get_device():
    id = fake.uuid4()
    name = "name"
    responses.add(
        responses.GET,
        api_url(f"/v1/devices/{id}"),
        json={
            "id": id,
            "name": fake.sentence(2),
        },
    )
    client = Client("test")
    device = client.get_device(device_id=id)
    assert device["id"] == id

    responses.add(
        responses.GET,
        api_url(f"/v1/devices/{name}"),
        json={
            "id": id,
            "name": fake.sentence(2),
        },
    )
    device = client.get_device(device_name=name)
    assert device["id"] == id
    assert device["properties"] is None


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
            }
        ],
    )
    client = Client("test")
    devices = client.get_devices()
    assert len(devices) == 1
    assert devices[0]["id"] == id
    assert devices[0]["properties"] is None


@responses.activate
def test_delete_device():
    id = fake.uuid4()
    name = "name"
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

    responses.add(
        responses.DELETE,
        api_url(f"/v1/devices/{name}"),
        json={"success": True},
    )
    try:
        client.delete_device(device_name=name)
    except:
        assert False


@responses.activate
def test_update_device():
    old_name = "old_name"
    new_name = "new_name"
    properties = {"sn": 1}
    # Patching name alone
    responses.add(
        responses.PATCH,
        api_url(f"/v1/devices/{old_name}"),
        match=[json_params_matcher({"name": new_name}, strict_match=True)],
        json={
            "id": "no-new-properties",
            "name": new_name,
            "properties": properties,
        },
    )
    # Patching name and properties
    responses.add(
        responses.PATCH,
        api_url(f"/v1/devices/{old_name}"),
        match=[
            json_params_matcher(
                {"name": new_name, "properties": properties}, strict_match=True
            )
        ],
        json={
            "id": "with-new-properties",
            "name": new_name,
            "properties": properties,
        },
    )
    client = Client("test")
    device = client.update_device(
        device_name=old_name, new_name=new_name, properties=properties
    )
    assert device["id"] == "with-new-properties"
    assert device["name"] == new_name
    assert device["properties"] == properties

    device = client.update_device(device_name=old_name, new_name=new_name)
    assert device["id"] == "no-new-properties"
    assert device["name"] == new_name
