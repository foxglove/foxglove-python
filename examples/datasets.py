from pathlib import Path

from foxglove.client import Client

token = "<YOUR API TOKEN HERE>"
project_id = "<YOUR PROJECT ID HERE>"
recording_id = "<YOUR RECORDING ID HERE>"
event_id = "<YOUR EVENT ID HERE>"
# Use the key of an existing string-valued episode custom property.
property_key = "<YOUR EPISODE CUSTOM PROPERTY KEY HERE>"

client = Client(token=token)

episodes = client.create_episodes(
    project_id=project_id,
    episodes=[
        {
            "recordings": [recording_id],
            "properties": {property_key: "needs_review"},
        },
        # Capture the event's current time range and overlapping recordings.
        # Later changes to the event do not change this episode.
        {"event_id": event_id},
    ],
)
# Changing a property creates a new episode version.
# Omitted property keys keep their values; use None to remove a value.
client.update_episodes(
    episodes=[{"id": episodes[0]["id"], "properties": {property_key: "reviewed"}}]
)

# Properties are omitted from reads unless explicitly requested.
episode = client.get_episode(episode_id=episodes[0]["id"], include_properties=True)
print(episode["version"], episode["properties"])

dataset = client.create_dataset(
    project_id=project_id,
    name="Training candidates",
    episode_ids=[episode["id"] for episode in episodes],
)
# Commit the dataset, then download that committed version.
commit = client.commit_dataset(dataset_id=dataset["id"])
version_number = commit["committed"]["version_number"]

output_directory = client.download_dataset(
    dataset_id=dataset["id"],
    version_number=version_number,
    output_directory=Path("training-candidates"),
    # Uncomment to include episode properties in manifest.json.
    # This requires API access to episode properties.
    # include_properties=True,
    # topics=["/camera", "/joint_states"],  # Omit to download all topics.
)
print(f"Exported dataset to {output_directory}")
print("Check manifest.json for partial, skipped, or failed episodes")

# A list call fetches one page. Automatic iteration follows cursors as needed.
page = client.get_dataset_versions(dataset_id=dataset["id"], limit=100)
for version in page.auto_paging_iter():
    print(version["version_number"], version["episode_count"])
