# Python Client Library for Foxglove Data Platform

This library provides a convenient python client for [Foxglove's Data Platform](https://foxglove.dev/data-platform).

In order to use the client you will first have to create an API token for your organization on your organization's [settings page](https://console.foxglove.dev/organization).

## Sample Usage

```python
import datetime
import io
import time
from datetime import datetime, timedelta, timezone

from foxglove.data_platform.client import Client

token = "<YOUR API TOKEN HERE>"
device_id = "<YOUR DEVICE ID>"
client = Client(token=token)

event = client.create_event(
    device_id=device_id,
    time=datetime.now(),
    duration=0,
    metadata={"message": "Hi from python!"},
)
print(event)

download = client.download_data(
    device_id=device_id,
    start=datetime.now() - timedelta(hours=3),
    end=datetime.now() - timedelta(hours=1),
)
print("download", download.read())

events = client.list_events()
print(events)
```
