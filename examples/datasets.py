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
)
print(f"Downloaded dataset to {output_directory}")
