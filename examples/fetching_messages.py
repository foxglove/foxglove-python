import datetime
from datetime import datetime, timedelta

from foxglove_data_platform.client import Client

token = "<YOUR API TOKEN HERE>"
device_id = "<YOUR DEVICE ID>"
client = Client(token=token)

# Make sure you've imported either the mcap-ros1-support or mcap-protobuf-support
# libraries before making this call in order to get decoded messages.
messages = client.get_messages(
    device_id=device_id,
    start=datetime.now() - timedelta(hours=3),
    end=datetime.now() - timedelta(hours=1),
)

print(f"downloaded {len(messages)} messages")
