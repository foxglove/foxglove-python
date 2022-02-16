from shlex import quote
from typing import Any, List, Optional, Tuple

import requests


def to_curl(
    request: requests.PreparedRequest,
    compressed: Optional[bool] = False,
    verify: Optional[bool] = True,
):
    """
    Returns string with curl command by provided request object
    Parameters
    ----------
    compressed : bool
        If `True` then `--compressed` argument will be added to result
    """
    parts: List[Tuple[Optional[str], Any]] = [
        ("curl", None),
        ("-X", request.method),
    ]

    for k, v in sorted(request.headers.items()):
        parts += [("-H", "{0}: {1}".format(k, v))]

    if request.body:
        body = request.body
        if isinstance(body, bytes):
            body = body.decode("utf-8")
        parts += [("-d", body)]

    if compressed:
        parts += [("--compressed", None)]

    if not verify:
        parts += [("--insecure", None)]

    parts += [(None, request.url)]

    flat_parts: List[str] = []
    for k, v in parts:
        if k:
            flat_parts.append(quote(k))
        if v:
            flat_parts.append(quote(v))

    return " ".join(flat_parts)
