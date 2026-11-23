# Validated NVL72 and AWS reference runs

[Read the English reproduction guide](reference-guide.en.md).

This directory preserves the November 23 validation evidence and the English
Confluence guide. The executable source and both tested artifacts are pinned to
`faa47fe51f998ae97385e2b218be4a261006bfa6`, on branch
`benchmark/261123-scaleup-reference`. Check out that executable revision to run
the published binaries. This documentation branch is separate from the artifact's
source revision.

The guide includes the complete two-iteration results for all 22 queries on both
platforms, artifact locations and hashes, data paths, configuration exceptions,
runtime setup and timing definitions. The private optimizer is pinned to upstream
`nvspark/cudf-spark-private@61e9e96803771371dec116593f0c4eced9ac44d8`.
The personal fork used for the historical build is not a source requirement.
The evidence archives omit raw event logs and ephemeral service ownership tokens.

Render the final English body, with evidence links pinned to this documentation
commit, without writing to Confluence:

```bash
python3 benchmarks/scaleup_reference/docs/261123_reference/publish_wiki.py
```

The target is the existing SPARK page
[4172120563](https://nvidia.atlassian.net/wiki/pages/viewpage.action?pageId=4172120563),
under parent 3903565164. Publishing replaces that page's title and body with the
English guide and preserves its location. The script refuses a different page
version, snapshots the existing content under `/tmp`, and reads back the updated
page to verify the English title/body, source links, results and ancestry.

The Confluence CLI requires human confirmation. Run directly in a human-operated
VS Code/devcontainer Terminal, rather than through Codex's `!` command channel:

```bash
python3 benchmarks/scaleup_reference/docs/261123_reference/publish_wiki.py --publish
```

The default expected page version is 1. If it has changed, inspect and reconcile
the live content before deliberately selecting another `--expected-version`.
Publication uses repository evidence links, so it needs one confirmed page write
and no attachment uploads. Successful update and read-back receipts are saved
beside the rendered `/tmp/scaleup-reference-wiki.en.md`.
