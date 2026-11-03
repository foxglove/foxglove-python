# Repository patterns

These pointers were checked against commit `6499aa1`. Read the current files before copying a pattern; their contracts take precedence over this map.

## Where changes belong

| Concern | Starting point |
| --- | --- |
| `Client` methods, one per API operation | `foxglove/client/api.py` |
| Response conversion helpers (`_event_dict`, `_device_dict`, `_episode_dict`, `_dataset_version_dict`, ...) | bottom of `foxglove/client/api.py` |
| Shared request helpers (`without_nulls`, `camelize`, `bool_query_param`, `_episode_include`, `_UNSET`) | top of `foxglove/client/api.py` |
| Error handling (`json_or_raise`) | `foxglove/client/api.py` |
| Cursor pagination (`Page`, `auto_paging_iter`) and `Client._get_page` | `foxglove/client/pagination.py`, `foxglove/client/api.py` |
| Streaming and topic reads (`iter_messages`, `download_data`, `_make_stream_link`, `get_topics`) | `foxglove/client/api.py` |
| Dataset export and its manifest | `foxglove/client/dataset_download.py`, `Client.download_dataset` |
| Public exports | `foxglove/client/__init__.py` |
| Tests, one file per resource group | `tests/test_<resource>.py`, `tests/api_url.py` |
| Usage examples | `examples/` |
| Build, dependencies, and checks | `pyproject.toml`, `uv.lock`, `.github/workflows/ci.yml` |

## Client conventions

- Methods take keyword-only arguments (`*`) with snake_case names and send the API's camelCase names. Build optional query and body fields with `without_nulls(...)`; pass enum-like sort fields through `camelize` unless the value can name a custom property key, which must pass through unchanged.
- Use `_UNSET` only where the API distinguishes "clear to null" from "leave unchanged" on a nullable field (see `update_dataset`). Otherwise `None` means omit.
- Name methods after neighbors: `get_<resources>`, `get_<resource>`, `create_<resource>`, `update_<resource>`, `delete_<resource>`. Methods taking a resource by ID or key accept both as mutually exclusive arguments and URL-encode the path segment with a shared `_<resource>_identifier_for_path` helper.
- Convert responses with a module-level `_<resource>_dict` helper that returns snake_case keys. Read fields the spec marks required with `[]` and always-returned optional fields with `.get()`. When a field is present only with an `include` option or only from some endpoints, add the key only when the field is present (see `_episode_dict`), so callers who didn't request it don't get `None`. Convert timestamps with `arrow.get(value).datetime`. Pass free-form objects (properties, metadata, layout data, policies, creation context) through unchanged with the API's field names, and say so in the docstring.
- A cursor-paginated endpoint (a `cursor` query parameter and `fg-pagination-*` response headers) returns a `Page` via `self._get_page(path, mapper, params=..., collection=...)`. A non-paginated list returns a plain list.
- Delete methods call `json_or_raise(response)` and return nothing.
- Never remove or rename an existing result key or parameter in a sync. Add new keys alongside existing ones.
- Support Python 3.8: use `typing` generics (`Optional`, `List`, `Dict`), not `X | Y` or `list[str]`.
- Write short docstrings in the style of neighboring methods. Comments describe the code, not its history.

## Dataset export

`download_dataset` writes the same manifest as the app's dataset download, with file paths relative to the output directory. Changes to manifest fields, entry statuses, or the rules for which versions can be downloaded must match the public app and CLI behavior; check the current manifest contract before changing either side.

## Tests

- Use `responses` with strict `json_params_matcher` for request bodies and `query_param_matcher` for query strings, and assert the converted result.
- Extend the resource's existing test file when it covers the changed method; add `tests/test_<resource>.py` only for a new resource group. A test file named `test_events.py` already exists, so new event tests go there.
- Keep mocks complete for fields the conversion reads with `[]`, or the test fails with `KeyError` rather than exercising the change.

## Validation

Read current CI before running checks. At the checked revision CI runs on Python 3.8:

```sh
uv sync --frozen --dev --python 3.8
uv run black --check --diff --color .
uv run pyright foxglove
uv run python -m flake8 foxglove
uv run python -m flake8 examples
uv run python -m pytest
uv run python -m build --installer uv
```

The ROS message tests depend on packages from a separate index. If that index is unreachable, install the other development dependencies into a Python 3.8 environment, run the remaining checks, and report which test files were skipped. Point `pyright` at that environment's interpreter with `--pythonpath` so imports resolve. Rely on CI for the full suite; do not claim local runs covered tests they skipped.
