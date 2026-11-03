import responses
from responses.matchers import header_matcher

from foxglove.client import Client

from .api_url import api_url


def _extension_json(**overrides):
    return {
        "id": "ext_1",
        "name": "custom-panel",
        "publisher": "acme",
        "displayName": "Custom Panel",
        "description": "A custom panel",
        "activeVersion": "1.2.0",
        "sha256Sum": "abc123",
        **overrides,
    }


@responses.activate
def test_get_extensions():
    responses.add(
        responses.GET,
        api_url("/v1/extensions"),
        json=[
            _extension_json(),
            _extension_json(
                id="ext_2", description=None, activeVersion=None, sha256Sum=None
            ),
        ],
    )
    extensions = Client("test").get_extensions()
    assert extensions[0] == {
        "id": "ext_1",
        "name": "custom-panel",
        "publisher": "acme",
        "display_name": "Custom Panel",
        "description": "A custom panel",
        "active_version": "1.2.0",
        "sha256_sum": "abc123",
    }
    assert extensions[1]["description"] is None
    assert extensions[1]["active_version"] is None
    assert extensions[1]["sha256_sum"] is None


@responses.activate
def test_get_extension_includes_download_link():
    responses.add(
        responses.GET,
        api_url("/v1/extensions/ext_1"),
        json=_extension_json(foxe="https://example.com/ext_1.foxe"),
    )
    extension = Client("test").get_extension(extension_id="ext_1")
    assert extension["display_name"] == "Custom Panel"
    assert extension["foxe"] == "https://example.com/ext_1.foxe"


@responses.activate
def test_publish_extension_uploads_package_bytes():
    package = b"PK\x03\x04foxe-contents"
    responses.add(
        responses.POST,
        api_url("/v1/extension-upload"),
        match=[header_matcher({"Content-Type": "application/octet-stream"})],
        json={"id": "ext_1"},
    )
    result = Client("test").publish_extension(data=package)
    assert result == {"id": "ext_1"}
    assert responses.calls[0].request.body == package


@responses.activate
def test_delete_extension():
    responses.add(
        responses.DELETE, api_url("/v1/extensions/ext_1"), json={"success": True}
    )
    Client("test").delete_extension(extension_id="ext_1")
    assert len(responses.calls) == 1
