import datetime
import sys
from datetime import datetime, timedelta

from foxglove_data_platform.client import Client

token = "<YOUR API TOKEN HERE>"
device_id = "<YOUR DEVICE ID>"
client = Client(token=token)

download_stream = client.download_data(
    device_id=device_id,
    start=datetime.now() - timedelta(hours=3),
    end=datetime.now() - timedelta(hours=1),
)

data = bytes()
for chunk in download_stream.iter_content(chunk_size=64 * 1024):
    sys.stdout.write(".")
    data += chunk

print("download", len(data))
