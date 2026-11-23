#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Render the tested runbook and optionally create its Confluence child page."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
PARENT = '3903565164'
TITLE = 'NVL72 与 AWS 48x 标准配置及复现指南（2026-11-23）'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--publish', action='store_true',
                        help='Create the child page; the CLI asks the human for confirmation')
    parser.add_argument('--output', type=Path, default=Path('/tmp/scaleup-reference-wiki.md'))
    args = parser.parse_args()
    revision = subprocess.check_output(['git', '-C', str(HERE), 'rev-parse', 'HEAD'], text=True).strip()
    base = ('https://github.com/binmahone/spark-rapids/blob/' + revision +
            '/benchmarks/scaleup_reference/docs/261123_reference/evidence/')
    checksums = json.loads((HERE / 'evidence/attachment-checksums.json').read_text())
    for name, record in checksums.items():
        assert hashlib.sha256((HERE / 'evidence' / name).read_bytes()).hexdigest() == record['sha256']
    names = list(checksums) + ['attachment-checksums.json']
    lines, fenced = [], False
    for line in (HERE / 'reference-guide.zh.md').read_text().splitlines():
        if line.startswith('```'):
            fenced = not fenced
        if not fenced:
            for name in names:
                line = line.replace('`' + name + '`', '[' + name + '](' + base + name + ')')
        lines.append(line)
    body = '\n'.join(lines) + '\n'
    body += '\n\n证据文件目录：[固定提交](' + base.replace('/blob/', '/tree/').rstrip('/') + ')。\n'
    args.output.write_text(body)
    print('Rendered review copy:', args.output)
    if not args.publish:
        return
    if not (sys.stdin.isatty() and sys.stderr.isatty()):
        parser.error('Run --publish in a human-operated interactive terminal for CLI confirmation')
    env = dict(os.environ, CONFLUENCE_CLI_SPACE='SPARK')

    def cli(*command):
        result = subprocess.run(['confluence-cli', *command, '--json'], stdout=subprocess.PIPE,
                                text=True, env=env, check=True)
        return json.loads(result.stdout)

    children = cli('page', 'children', PARENT)['data']
    matches = [p for p in children if p['title'] == TITLE]
    if matches:
        raise RuntimeError('A matching page already exists; inspect it before any further write: ' +
                           matches[0]['id'])
    print('Create one new child under https://nvidia.atlassian.net/wiki/pages/viewpage.action?pageId=' + PARENT)
    print('Review the exact content in', args.output, 'before accepting the CLI prompt.', flush=True)
    created = cli('page', 'create', TITLE, body, '--parent', PARENT)
    receipt = args.output.with_suffix('.created.json')
    receipt.write_text(json.dumps(created, ensure_ascii=False, indent=2) + '\n')
    data = created['data']
    page_id = str(data.get('id') or data.get('page_id'))
    assert page_id and page_id != 'None', created
    ancestors = cli('page', 'ancestors', page_id)
    assert PARENT in json.dumps(ancestors), ancestors
    page = cli('page', 'get', page_id, '--format', 'html')
    assert page['data']['spaceKey'] == 'SPARK', page['data']['spaceKey']
    content = json.dumps(page, ensure_ascii=False)
    for marker in ['154.665', '447.041', '171.848', '467.840',
                   'faa47fe51f998ae97385e2b218be4a261006bfa6', 'per-query-results.csv']:
        assert marker in content, marker
    args.output.with_suffix('.verified.json').write_text(json.dumps(page, ensure_ascii=False, indent=2))
    print('Created and read back: https://nvidia.atlassian.net/wiki/pages/viewpage.action?pageId=' + page_id)


if __name__ == '__main__':
    main()
