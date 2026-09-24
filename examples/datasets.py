from pathlib import Path

from foxglove.client import Client

token = "<YOUR API TOKEN HERE>"
project_id = "<YOUR PROJECT ID HERE>"
recording_id = "<YOUR RECORDING ID HERE>"

client = Client(token=token)

episodes = client.create_episodes(
    project_id=project_id,
    episodes=[{"recordings": [recording_id]}],
)
dataset = client.create_dataset(
    project_id=project_id,
    name="Training candidates",
    episode_ids=[episodes[0]["id"]],
)
commit = client.commit_dataset(dataset_id=dataset["id"])

output_directory = client.download_dataset(
    dataset_id=dataset["id"],
    version_number=commit["committed"]["version_number"],
    output_directory=Path("training-candidates"),
    # topics=["/camera", "/joint_states"],  # Omit to download all topics.
)
print(f"Exported dataset to {output_directory}")
print("Check manifest.json for partial, skipped, or failed episodes")

# A list call fetches one page. Automatic iteration follows cursors as needed.
page = client.get_dataset_versions(dataset_id=dataset["id"], limit=100)
for version in page.auto_paging_iter():
    print(version["version_number"], version["episode_count"])
