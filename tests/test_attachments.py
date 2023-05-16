import responses
from faker import Faker
from foxglove_data_platform.client import Client
import arrow

from .api_url import api_url

fake = Faker()


@responses.activate
def test_download_attachment():
    id = "abcde"
    data = fake.binary(4096)
    responses.add(
        responses.GET,
        api_url(f"/v1/recording-attachments/{id}/download"),
        body=data,
    )
    client = Client("test")
    response_data = client.download_attachment(
        attachment_id=id,
    )
    assert data == response_data
