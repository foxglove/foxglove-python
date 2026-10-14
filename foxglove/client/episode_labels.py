"""Convert URI label field names at the Python/API boundary."""

from typing import Any, Dict, Mapping, Optional

from .types import EpisodeLabelValue


def labels_to_api(
    labels: Mapping[str, Optional[EpisodeLabelValue]],
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in labels.items():
        if value is not None and value["type"] == "URI":
            content = value["content"]
            result[key] = {
                "type": "URI",
                "content": {
                    "uri": content["uri"],
                    "checksum": content["checksum"],
                    "contentType": content["content_type"],
                },
            }
        else:
            result[key] = value
    return result


def labels_from_api(labels: Mapping[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in labels.items():
        if value["type"] == "URI":
            content = value["content"]
            result[key] = {
                "type": "URI",
                "content": {
                    "uri": content["uri"],
                    "checksum": content["checksum"],
                    "content_type": content["contentType"],
                },
            }
        else:
            result[key] = value
    return result
