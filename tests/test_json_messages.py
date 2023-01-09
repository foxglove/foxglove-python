from datetime import datetime
from unittest.mock import MagicMock

from foxglove_data_platform.client import Client

from .generate import generate_json_data


def test_download_with_decoder():
    client = Client("test")
    client.download_data = MagicMock(return_value=generate_json_data())
    messages = client.get_messages(
        device_id="test_id", start=datetime.now(), end=datetime.now()
    )
    assert len(messages) == 10
    for _, _, msg in messages:
        assert "level" in msg
