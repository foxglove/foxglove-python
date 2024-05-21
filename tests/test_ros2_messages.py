from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from foxglove.client import Client

from .generate import generate_ros2_data


def test_download_without_decoder():
    with patch("foxglove.client.DEFAULT_DECODER_FACTORIES", []):
        client = Client("test")
        client.download_data = MagicMock()
        client.download_data.return_value = generate_ros2_data()
        with pytest.raises(Exception):
            client.get_messages(
                device_id="test_id", start=datetime.now(), end=datetime.now()
            )


def test_download_with_decoder():
    client = Client("test")
    client.download_data = MagicMock()
    client.download_data.return_value = generate_ros2_data()
    messages = client.get_messages(
        device_id="test_id", start=datetime.now(), end=datetime.now()
    )
    assert len(messages) == 10
    for i, (_, _, msg) in enumerate(messages):
        assert msg.data == f"string message {i + 1}"
