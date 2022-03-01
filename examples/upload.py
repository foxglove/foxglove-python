from pathlib import Path

from foxglove.data_platform.client import Client

token = "<YOUR API TOKEN HERE>"
device_id = "<YOUR DEVICE ID>"

client = Client(token=token)

# Upload bytes
mcap_data = Path("my_mcap_data.mcap").read_bytes()

client.upload_data(
    device_id=device_id,
    filename="test upload",
    data=mcap_data,
    callback=lambda size, progress: print(size, progress),
)

# Streaming upload
with Path("my_mcap_data.mcap").open("rb") as mcap_stream:
    client.upload_data(
        device_id=device_id,
        filename="test upload stream",
        data=mcap_stream,
        callback=lambda size, progress: print(size, progress),
    )
