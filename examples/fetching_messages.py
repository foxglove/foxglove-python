import datetime
from datetime import datetime, timedelta

from foxglove.client import Client

token = "<YOUR API TOKEN HERE>"
device_id = "<YOUR DEVICE ID>"
client = Client(token=token)

# Make sure you've imported either the mcap-ros1-support or mcap-protobuf-support
# libraries before making this call in order to get decoded messages.
num_messages = 0
for message in client.iter_messages(
    device_id=device_id,
    start=datetime.now() - timedelta(hours=3),
    end=datetime.now() - timedelta(hours=1),
):
    num_messages += 1

print(f"downloaded {num_messages} messages")
