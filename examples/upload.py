from pathlib import Path

from foxglove_data_platform.client import Client

token = "<YOUR API TOKEN HERE>"
device_id = "<YOUR DEVICE ID>"

client = Client(token=token)

# Upload bytes
mcap_data = Path("my_mcap_data.mcap").read_bytes()

client.upload_data(
    device_id=device_id,
    filename="test mcap upload",
    data=mcap_data,
    callback=lambda size, progress: print(size, progress),
)

# Streaming upload
with Path("my_mcap_data.mcap").open("rb") as mcap_stream:
    client.upload_data(
        device_id=device_id,
        filename="test mcap upload stream",
        data=mcap_stream,
        callback=lambda size, progress: print(size, progress),
    )

# Upload ROS1 data
ros_data = Path("my_ros1_data.bag").read_bytes()
client.upload_data(
    device_id=device_id,
    filename="test ros upload",
    data=ros_data,
    callback=lambda size, progress: print(size, progress),
)
