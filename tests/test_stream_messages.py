from io import BytesIO
from datetime import datetime
from unittest.mock import MagicMock, patch

from mcap.records import Schema, Channel, Message

from foxglove.client import Client

from .generate import generate_json_data


def get_generated_data(url, **kwargs):
    assert url == "the_link"

    class Resp:
        def __init__(self):
            self.raw = BytesIO(generate_json_data())

        def raise_for_status(self):
            return None

    return Resp()


@patch("requests.Session.get", side_effect=get_generated_data)
def test_boot(arg):
    client = Client("test")
    client._make_stream_link = MagicMock(return_value="the_link")
    count = 0
    for schema, channel, message, decoded_message in client.iter_messages(
        device_id="test_id", start=datetime.now(), end=datetime.now()
    ):
        assert "level" in decoded_message
        assert isinstance(schema, Schema)
        assert isinstance(channel, Channel)
        assert isinstance(message, Message)
        count += 1

    assert count == 10
