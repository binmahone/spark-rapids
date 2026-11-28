#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Verify CPU and blocked Java stack capture before running a GPU workload."""
import argparse
import json
from pathlib import Path
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--work', type=Path, required=True)
a = p.parse_args()
directory = a.work / 'profilers/smoke'
directory.mkdir(exist_ok=False)
manifest = json.loads((a.work / 'profilers/manifest.json').read_text())
source = directory / 'ProfilerSmoke.java'
source.write_text('''public class ProfilerSmoke {
  static volatile long value;
  static void burnCpu() {
    long end = System.nanoTime() + 2_000_000_000L;
    long result = 1;
    while (System.nanoTime() < end) {
      for (int i = 0; i < 10000; i++) result = result * 31 + i;
      value = result;
    }
  }
  static void waitForWork() throws InterruptedException { Thread.sleep(2000); }
  public static void main(String[] args) throws Exception { burnCpu(); waitForWork(); }
}
''')
java = str(a.work / 'jdk17/bin/java')
subprocess.run([str(a.work / 'jdk17/bin/javac'), str(source)], check=True)
options = (f"-agentpath:{manifest['library']}=start,event=cpu,interval=10ms,wall=20ms,"
           f"cstack=dwarf,jstackdepth=256,timeout=30,file={directory}/profile-%p.jfr,log={directory}/profile-%p.log")
with (directory / 'launch.log').open('x') as log:
    subprocess.run([java, options, '-cp', str(directory), 'ProfilerSmoke'], stdout=log,
                   stderr=subprocess.STDOUT, check=True, timeout=60)
profiles = list(directory.glob('*.jfr'))
assert len(profiles) == 1
for mode, expected in [('cpu', 'burnCpu'), ('wall', 'waitForWork')]:
    output = directory / (mode + '.collapsed')
    subprocess.run([java, '-jar', str(a.work / 'profilers/jfr-converter.jar'), '--' + mode,
                   '--threads', '-o', 'collapsed', str(profiles[0]), str(output)], check=True, timeout=60)
    assert expected in output.read_text(), mode
(directory / 'result.json').write_text(json.dumps({'status': 'passed', 'cpu_hotspot': 'burnCpu',
    'blocked_stack': 'waitForWork', 'native_stack_coverage': 'Requires workload inspection'}) + '\n')
print('CPU and blocked Java stack capture passed')
