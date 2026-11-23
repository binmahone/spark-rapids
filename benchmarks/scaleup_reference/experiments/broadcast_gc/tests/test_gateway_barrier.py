#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""The real Py4J worker must acknowledge queued deletions before the barrier returns."""
import argparse
from collections import deque
from pathlib import Path
import sys
import threading
from types import SimpleNamespace

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--spark-home', type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0, str(next(args.spark_home.glob('python/lib/py4j-*.zip'))))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from py4j.clientserver import FinalizerWorker, SHUTDOWN_FINALIZER_WORKER
from gateway_barrier import release_proxies

queue = deque()
entered, release, finished = threading.Event(), threading.Event(), threading.Event()
acknowledged = []


class Client:
    finalizer_deque = queue

    def garbage_collect_object(self, target, enqueue):
        assert not enqueue
        entered.set()
        assert release.wait(5)
        acknowledged.append(target)


client = Client()
queue.appendleft((client, 'old-query-proxy'))
worker = FinalizerWorker(queue)
worker.daemon = True
worker.start()
assert entered.wait(5)
errors = []


def wait_for_proxy_release():
    try:
        release_proxies(SimpleNamespace(_gateway=SimpleNamespace(_gateway_client=client)))
        assert acknowledged == ['old-query-proxy']
        finished.set()
    except BaseException as error:
        errors.append(error)


thread = threading.Thread(target=wait_for_proxy_release)
thread.start()
assert not finished.wait(.2), 'Barrier returned before the old Java reference was deleted'
release.set()
thread.join(5)
assert finished.is_set() and not errors, errors
queue.appendleft(SHUTDOWN_FINALIZER_WORKER)
worker.join(3)
assert not worker.is_alive()
print('PASS preceding_proxy_delete_acknowledged_before_return=true')
