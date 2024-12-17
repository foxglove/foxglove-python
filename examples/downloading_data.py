from datetime import datetime, timedelta

from foxglove.client import Client

token = "<YOUR API TOKEN HERE>"
device_id = "<YOUR DEVICE ID>"
client = Client(token=token)

data = client.download_data(
    device_id=device_id,
    start=datetime.now() - timedelta(hours=3),
    end=datetime.now() - timedelta(hours=1),
    callback=lambda progress: print(".", end=""),
)

print(f"downloaded {len(data)} bytes")
