from datetime import datetime

from foxglove_client.client import Client

token = "<YOUR API TOKEN>"
device_id = "<YOUR DEVICE ID>"
client = Client(token=token)

event_time = datetime.now()

event = client.create_event(
    device_id=device_id,
    start=event_time,
    end=event_time,
    metadata={"message": "Hi from python!"},
)
print(event)

events = client.get_events(device_id=device_id)
print(events)
