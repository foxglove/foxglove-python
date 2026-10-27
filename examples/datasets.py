from pathlib import Path

from foxglove.client import Client

token = "<YOUR API TOKEN HERE>"
project_id = "<YOUR PROJECT ID HERE>"
recording_id = "<YOUR RECORDING ID HERE>"
event_id = "<YOUR EVENT ID HERE>"
property_key = "<YOUR EPISODE CUSTOM PROPERTY KEY HERE>"

client = Client(token=token)

# An event episode uses the event's time range and its device's overlapping recordings.
episodes = client.create_episodes(
    project_id=project_id,
    episodes=[
        {"recordings": [recording_id]},
        {"event_id": event_id, "metadata": {"label": "example"}},
    ],
)
# Each change creates a new episode version. None unsets a property.
client.update_episodes(
    episodes=[{"id": episodes[0]["id"], "properties": {property_key: "reviewed"}}]
)
dataset = client.create_dataset(
    project_id=project_id,
    name="Training candidates",
    episode_ids=[episode["id"] for episode in episodes],
)
commit = client.commit_dataset(dataset_id=dataset["id"])

output_directory = client.download_dataset(
    dataset_id=dataset["id"],
    version_number=commit["committed"]["version_number"],
    output_directory=Path("training-candidates"),
    # include_properties=True,  # Include custom properties in manifest.json.
    # topics=["/camera", "/joint_states"],  # Omit to download all topics.
)
print(f"Exported dataset to {output_directory}")
print("Check manifest.json for partial, skipped, or failed episodes")

# A list call fetches one page. Automatic iteration follows cursors as needed.
page = client.get_dataset_versions(dataset_id=dataset["id"], limit=100)
for version in page.auto_paging_iter():
    print(version["version_number"], version["episode_count"])
