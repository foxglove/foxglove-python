from datetime import datetime

from foxglove.data_platform.client import Client

token = "<YOUR API TOKEN>"
device_id = "<YOUR DEVICE ID>"
client = Client(token=token)

event = client.create_event(
    device_id=device_id,
    time=datetime.now(),
    duration=0,
    metadata={"message": "Hi from python!"},
)
print(event)

events = client.list_events()
print(events)
