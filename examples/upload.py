import sys
from pathlib import Path

from foxglove.data_platform.client import Client


def progress_callback(size: int, progress: int):
    sys.stdout.write(".")


token = "<YOUR API TOKEN HERE>"
device_id = "<YOUR DEVICE ID>"

client = Client(token=token)

mcap_data = Path("my_mcap_data.mcap").read_bytes()

client.upload_data(
    device_id=device_id,
    filename="test upload",
    data=mcap_data,
    callback=progress_callback,
)
