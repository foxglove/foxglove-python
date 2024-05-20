from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from foxglove.client import Client

from .generate import generate_ros1_data


def test_download_without_decoder():
    with patch("foxglove.client.DEFAULT_DECODER_FACTORIES", []):
        client = Client("test")
        client.download_data = MagicMock()
        client.download_data.return_value = generate_ros1_data()
        with pytest.raises(Exception):
            for _ in client.iter_messages(
                device_id="test_id", start=datetime.now(), end=datetime.now()
            ):
                pass


def test_download_with_decoder():
    client = Client("test")
    client.download_data = MagicMock()
    client.download_data.return_value = generate_ros1_data()
    messages = list(
        client.iter_messages(
            device_id="test_id", start=datetime.now(), end=datetime.now()
        )
    )
    assert len(messages) == 10
