#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
"""Replace only the compiled file-IO entry point in a frozen, checksummed diagnostic jar."""
import argparse
import hashlib
import json
from pathlib import Path
import zipfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--base", required=True)
parser.add_argument("--classes", required=True)
parser.add_argument("--native", required=True)
parser.add_argument("--source-revision", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()
base = Path(args.base)
expected_base = "ab3b63e7bf10d4e304dfdfd6c06b236b7898c6c82859bdc65c01184522aab432"
assert hashlib.sha256(base.read_bytes()).hexdigest() == expected_base
classes = Path(args.classes)
package = "com/nvidia/spark/rapids/fileio/hadoop/"
replacement = {package + name + ".class": (classes / package / (name + ".class")).read_bytes()
               for name in ["HadoopInputFile", "KvikioHostRangeReader"]}
identity = {"base_jar_sha256": expected_base, "source_revision": args.source_revision,
            "bridge_sha256": hashlib.sha256(Path(args.native).read_bytes()).hexdigest(),
            "supported_file_handle_limits": [1, 2, 3, 4],
            "supports_strided_lane_tasks": True,
            "class_sha256": {name: hashlib.sha256(data).hexdigest()
                             for name, data in replacement.items()},
            "scope": "Only range IO changes; base cuDF, JNI, H2D, optimizer, and shuffle retained"}
output = Path(args.output)
with zipfile.ZipFile(base) as source, zipfile.ZipFile(output, "w") as target:
    for info in source.infolist():
        if info.filename not in replacement:
            target.writestr(info, source.read(info.filename))
    for name, data in replacement.items():
        target.writestr(name, data, compress_type=zipfile.ZIP_DEFLATED)
    target.writestr("META-INF/kvikio-q6-diagnostic.json", json.dumps(identity, indent=2))
with zipfile.ZipFile(base) as source, zipfile.ZipFile(output) as target:
    assert len(target.namelist()) == len(set(target.namelist()))
    assert target.read("cudf-java-version-info.properties") == source.read(
        "cudf-java-version-info.properties")
    assert target.read("cudf-spark-jni-version-info.properties") == source.read(
        "cudf-spark-jni-version-info.properties")
    for name in source.namelist():
        if name not in replacement:
            assert target.read(name) == source.read(name), name
identity["jar_sha256"] = hashlib.sha256(output.read_bytes()).hexdigest()
Path(str(output) + ".identity.json").write_text(json.dumps(identity, indent=2) + "\n")
print(json.dumps(identity, indent=2))
