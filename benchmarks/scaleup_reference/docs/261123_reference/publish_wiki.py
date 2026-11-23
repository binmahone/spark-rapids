#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Render the English runbook and optionally update its existing Confluence page."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
PARENT = '3903565164'
PAGE_ID = '4172120563'
TITLE = 'NVL72 and AWS 48x Reference Configurations and Reproduction Guide (2026-11-23)'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--publish', action='store_true',
                        help='Update the existing page; the CLI asks the human for confirmation')
    parser.add_argument('--expected-version', type=int, default=1,
                        help='Refuse to replace a page with another version')
    parser.add_argument('--output', type=Path, default=Path('/tmp/scaleup-reference-wiki.en.md'))
    args = parser.parse_args()
    revision = subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip()
    base = ('https://github.com/binmahone/spark-rapids/blob/' + revision +
            '/benchmarks/scaleup_reference/docs/261123_reference/evidence/')
    checksums = json.loads((HERE / 'evidence/attachment-checksums.json').read_text())
    for name, record in checksums.items():
        assert hashlib.sha256((HERE / 'evidence' / name).read_bytes()).hexdigest() == record['sha256']
    names = list(checksums) + ['attachment-checksums.json']
    lines, fenced = [], False
    for line in (HERE / 'reference-guide.en.md').read_text().splitlines():
        if line.startswith('```'):
            fenced = not fenced
        if not fenced:
            for name in names:
                line = line.replace('`' + name + '`', '[' + name + '](' + base + name + ')')
        lines.append(line)
    body = '\n'.join(lines) + '\n'
    body += '\n\nEvidence directory: [fixed documentation commit](' + base.replace('/blob/', '/tree/').rstrip('/') + ').\n'
    assert not any('\u4e00' <= c <= '\u9fff' for c in body)
    args.output.write_text(body)
    print('Rendered review copy:', args.output)
    if not args.publish:
        return
    if not (sys.stdin.isatty() and sys.stderr.isatty()):
        parser.error('Run --publish directly in a human-operated VS Code/devcontainer Terminal, '
                     'not through Codex !; CLI confirmation requires terminal stdin and stderr')
    env = dict(os.environ, CONFLUENCE_CLI_SPACE='SPARK')

    def cli(*command):
        result = subprocess.run(['confluence-cli', *command, '--json'], stdout=subprocess.PIPE,
                                text=True, env=env, check=True)
        return json.loads(result.stdout)

    current = cli('page', 'get', PAGE_ID, '--format', 'html')
    assert current['data']['spaceKey'] == 'SPARK'
    assert str(current['data']['id']) == PAGE_ID
    if current['data']['version'] != args.expected_version:
        raise RuntimeError('Page version differs; inspect current content before updating: ' +
                           str(current['data']['version']))
    ancestors = cli('page', 'ancestors', PAGE_ID)
    assert PARENT in json.dumps(ancestors), ancestors
    args.output.with_suffix('.before.json').write_text(json.dumps(current, ensure_ascii=False, indent=2))
    print('Update title and body of https://nvidia.atlassian.net/wiki/pages/viewpage.action?pageId=' + PAGE_ID)
    print('Review the exact content in', args.output, 'before accepting the CLI prompt.', flush=True)
    updated = cli('page', 'update', PAGE_ID, '--file', str(args.output), '--title', TITLE)
    receipt = args.output.with_suffix('.updated.json')
    receipt.write_text(json.dumps(updated, ensure_ascii=False, indent=2) + '\n')
    data = updated['data']
    page_id = str(data.get('id') or data.get('page_id'))
    assert page_id == PAGE_ID, updated
    ancestors = cli('page', 'ancestors', page_id)
    assert PARENT in json.dumps(ancestors), ancestors
    page = cli('page', 'get', page_id, '--format', 'html')
    assert page['data']['spaceKey'] == 'SPARK', page['data']['spaceKey']
    assert page['data']['title'] == TITLE
    assert not any('\u4e00' <= c <= '\u9fff' for c in page['data']['content'])
    content = json.dumps(page, ensure_ascii=False)
    for marker in ['154.665', '447.041', '171.848', '467.840',
                   'faa47fe51f998ae97385e2b218be4a261006bfa6', 'per-query-results.csv',
                   'nvspark/cudf-spark-private/-/tree/61e9e96803771371dec116593f0c4eced9ac44d8',
                   'bf63ec567eabc0bcac5366f6afba0b0bba40f30a',
                   '3c92c2094dae1931ef7c2c19ee932cc26822a000',
                   'private-optimizer-provenance.json']:
        assert marker in content, marker
    args.output.with_suffix('.verified.json').write_text(json.dumps(page, ensure_ascii=False, indent=2))
    print('Updated and read back: https://nvidia.atlassian.net/wiki/pages/viewpage.action?pageId=' + page_id)


if __name__ == '__main__':
    main()
