# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Count allocated scratch blocks while Spark creates and deletes temporary files."""
import os
import stat


def allocated_bytes(root):
    initial = os.stat(root, follow_symlinks=False)
    if not stat.S_ISDIR(initial.st_mode):
        raise ValueError('Scratch root must be a directory')
    seen = {(initial.st_dev, initial.st_ino)}
    total = initial.st_blocks * 512
    pending = [os.fspath(root)]
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        value = entry.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    identity = (value.st_dev, value.st_ino)
                    if value.st_dev != initial.st_dev or identity in seen:
                        continue
                    seen.add(identity)
                    total += value.st_blocks * 512
                    if stat.S_ISDIR(value.st_mode):
                        pending.append(entry.path)
        except FileNotFoundError:
            continue
    return total
