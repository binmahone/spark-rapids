# Validated NVL72 and AWS reference runs

[Read the reproduction guide](reference-guide.zh.md).

This directory preserves the November 23 validation evidence and the prepared
Confluence guide. The executable source and both tested artifacts are pinned to
`faa47fe51f998ae97385e2b218be4a261006bfa6`, on branch
`benchmark/261123-scaleup-reference`. Checkout that source revision to run the
benchmark. This documentation branch adds no engine changes and is not the
artifact's source revision.

The guide records both complete 22-query, two-iteration runs, exact artifact
locations and hashes, dataset paths, configuration exceptions, runtime preparation
and timing definitions. The evidence archives omit raw event logs and ephemeral
service ownership tokens.

The Confluence CLI requires a human confirmation for writes. The automated
publication attempt was rejected before page creation with
`CONFIRMATION_REQUIRED`, because no interactive confirmation backend was available.
No Confluence page was created by that attempt.

Render the final content without writing to Confluence:

```bash
python3 benchmarks/scaleup_reference/docs/261123_reference/publish_wiki.py
```

To publish, run the following in a human-operated terminal on an environment with
the existing authenticated NVIDIA `confluence-cli`. Read the rendered Markdown
before approving the CLI prompt. The command creates one new child under page
3903565164, then reads it back and verifies its ancestry and key results. Evidence
links point to this documentation commit on GitHub, so no extra attachment writes
are needed. It does not overwrite the existing parent page.

```bash
python3 benchmarks/scaleup_reference/docs/261123_reference/publish_wiki.py --publish
```

If a matching child already exists, the script stops for inspection. A local
receipt is written under `/tmp` after successful creation; if read-back fails,
inspect that receipt before considering any further write.
