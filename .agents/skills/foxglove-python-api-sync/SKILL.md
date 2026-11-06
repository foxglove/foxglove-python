---
name: foxglove-python-api-sync
description: Update user-requested Foxglove Python client methods or fields against the public v1 OpenAPI spec, including scoped impact analysis and PR preparation.
---

# Foxglove Python client API sync

Sync only the methods, endpoints, or fields the user requests. A spec revision, revision range, or API PR supplies source evidence; it does not authorize syncing every change it contains. If the requested scope cannot be inferred from the conversation, clarify it before implementation. Broader API coverage requires an explicit request; when one is given, deliver it as one PR per resource group (the spec operation's first tag) unless the user says otherwise.

## Public repository content

This repository is public and published to PyPI as `foxglove-client`. Describe changes in terms of observable public API behavior and Python client behavior.

Private sources may inform implementation, but must not appear in public artifacts, including PR titles and descriptions, comments, branch names, commit messages, documentation, docstrings, code, and test fixtures, except for the Linear issue references allowed below. Do not include private repository names, paths, symbols, revisions, PRs, other issues or links, or details of backend architecture, storage, authorization mechanisms, or deployment.

Explain what callers can send, what they receive, and any compatibility changes. Include endpoint paths and schema fields only when they are part of the public API contract. Describe Python client internals when useful; do not describe the private API or app implementation.

Use publicly accessible references, with one exception: Foxglove Linear issue IDs and links are allowed for tracking and issue closure. Use the issue ID as the link text and `https://linear.app/foxglove/issue/<ISSUE-ID>` as the URL; omit descriptive URL slugs, issue titles, and other private issue details. Other non-public URLs are not allowed. An internal source does not become suitable for publication merely because it defines public behavior. Never mention open security issues, including their existence or details; the Linear exception does not apply to them.

## Establish scope and source

- Target `foxglove/foxglove-python`; read its README, `pyproject.toml`, and Git status.
- Source: the public v1 OpenAPI contract. When a source repository is supplied in the conversation, use a supplied revision, or resolve its current remote `main` to a full SHA and state that assumption in the private conversation. Retrieve committed content through authenticated GitHub access or `git show`; local uncommitted content is not upstream. Report inaccessible source rather than substituting docs.
- Skip operations, parameters, and properties marked `deprecated: true` or `x-internal: true`. An operation can be public while some of its request fields are internal; check each field.
- Use a supplied base when available. Otherwise compare only the requested client behavior with the pinned spec; no baseline is needed for a scoped update. A prior partial sync is not a repository-wide baseline.
- Before starting, list open PRs in this repository and check whether one already covers the requested scope or edits the same methods. Report overlaps instead of duplicating them.
- An implementation ticket is a tracker issue supplied to this skill as work to implement. Questions asking for analysis or recommendations do not trigger implementation or PR creation.
- Honor explicit delivery modes: analysis, local changes, or PR. When this skill is invoked with an implementation ticket and no delivery mode, implement, self-review, validate, and open a PR without a follow-up request. An analysis-only or local-only request overrides this default.

## Determine and implement the requested changes

Read [references/repository-patterns.md](references/repository-patterns.md) before implementation for repository locations, conventions, and checks. Verify pointers against the current checkout.

Compare semantic contracts for the requested operations, identified by method and path. Follow shared `$ref` dependencies, composed schemas, and path-level parameters. Check request serialization, omitted/null/empty values, response envelopes, pagination, timestamps, and which response fields are required versus optional.

Gap analysis has three parts; check each for the requested scope:

1. Operations the client does not call at all.
2. Query parameters and request-body fields an existing method cannot send.
3. Response fields an existing conversion drops, and fields it reads that the spec no longer returns. A field read with `[]` that the API removed raises `KeyError` for every caller, so treat it as a breakage to fix first.

Trace shared-helper effects (`_episode_dict`, `_dataset_episode_dict`, and similar) to preserve other methods, but implement only changes necessary for the requested behavior. Do not add unrelated operations or fill historical gaps unasked. Briefly flag incidental gaps only when useful.

Use [public API docs](https://docs.foxglove.dev/api) for usage context. A commit on main does not establish publication. If docs and spec materially disagree on requested behavior, report the discrepancy and avoid guessing; continue independent work.

Follow existing method, conversion, and error patterns. Preserve established client behavior unless the requested change requires an adjustment; never remove or rename an existing result key or parameter without saying so, and explain any breaking change. Avoid unrelated refactors, generated clients, or dependency upgrades.

Extend an existing focused test before adding a new test file. Add a new `tests/test_<resource>.py` only for a resource group with no existing file. Test request bodies and query strings with strict matchers and assert the converted result; do not add tests of generic HTTP error handling.

Before delivery, review the diff for correctness and simplicity. Remove redundant tests, fixtures, abstractions, and unrelated changes.

## PR descriptions

Whenever drafting, creating, or updating a PR description, fetch and follow the [Foxglove PR template](https://github.com/foxglove/.github/blob/main/.github/pull_request_template.md), including for a local draft. Use authenticated GitHub access if needed. Preserve its section order and Before/After table:

- **Changelog:** Always include a concise, one-sentence user-facing changelog message. Use `None` only when there is no user-facing change.
- **Docs:** Link the public documentation PR, public tracking issue, or Foxglove Linear issue using the format above; use `None` when no documentation changes are needed. If documentation work is needed but no permitted link is available, describe it briefly without private details. Do not invent links.
- **Description:** For a small change, use one short paragraph explaining the problem, resulting behavior, and compatibility, plus one concise validation sentence. Mention material blockers or publication uncertainty briefly; omit process history and repeated implementation details.
- **Before/After table:** Keep the template table with one representative call, such as `client.get_event_types()`. For new methods, state that they were previously unavailable. Distinguish illustrative expected output from observed test results.

Link public API documentation or a publicly accessible spec when useful. Omit private source links and identifiers except for the permitted Linear issue references. Describe the resulting behavior directly when no public reference is available. When the change resolves a public issue or Foxglove Linear issue, include an issue-closing link. For Linear, use `Fixes [<ISSUE-ID>](https://linear.app/foxglove/issue/<ISSUE-ID>)`. Keep the description proportional to the change. If the template cannot be fetched, disclose that limitation and use the structure above.

## Deliver

When PR creation is the delivery mode, whether explicitly requested or selected by default, check for existing work covering the same source and scope before creating it. Use `<user>/api-sync-<scope>` unless a branch is supplied; do not include private source identifiers in branch names. Stage only intended files; do not create an empty PR. For `gh`, use `--body-file` and explicit repository/base/head options.

Create a ready PR when checks pass and semantics are resolved; use a draft with explicit blockers otherwise. Verify remote state before retrying an uncertain push or PR creation. Do not merge, release, bump the package version, or modify the source API as part of a sync. Releases happen separately after the sync PRs merge; note in the result when downstream packages that pin `foxglove-client` (such as the Foxglove SDK's dataset extras) need a minimum-version bump after that release.

Return the PR link or local result, concise behavior changes, validation, and any unfinished requested work. Report private source revisions only in the private conversation, never in public artifacts.

Before delivery, review all public-facing text for internal details. Replace implementation explanations with public contract or client behavior descriptions, and remove private provenance except for the permitted Linear issue references.
