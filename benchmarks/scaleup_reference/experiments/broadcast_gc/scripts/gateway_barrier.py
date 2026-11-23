# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""Release unreachable Python proxies and await preceding Py4J memory-delete acknowledgments."""
import gc
import threading


def release_proxies(sc):
    collected = gc.collect()
    client = sc._gateway._gateway_client
    if hasattr(client, 'finalizer_deque'):
        acknowledged = threading.Event()

        class Barrier:
            def garbage_collect_object(self, unused_id, unused_enqueue):
                acknowledged.set()

        client.finalizer_deque.appendleft((Barrier(), None))
        if not acknowledged.wait(10):
            raise TimeoutError('Py4J memory-delete acknowledgment barrier expired')
    return collected
