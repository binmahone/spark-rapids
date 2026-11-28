#!/usr/bin/env python3
"""Compile one queue replacement and verify behavior against the frozen implementation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import urllib.request
import zipfile

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parents[1]/'scripts'))
from common import read,write,sha
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--work',type=Path,required=True)
p.add_argument('--thread-id',required=True)
a=p.parse_args()
assert read(a.work/'evidence/owner.json')['thread_id']==a.thread_id
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
for area in ('code-profile','data-path-trace'):
 assert all(read(path)['status'] not in ('starting','running') for path in (a.work/area).glob('*/state.json'))
root=a.work/'queue-index-probe';root.mkdir(exist_ok=False)
engine=a.work/'artifacts/decimal-null-copy/cudf-spark_2.12-26.12.0-SNAPSHOT-cuda13.jar'
assert sha(engine)=='ec38a58426e662ce99a433e806a66fa927bb011c014465a5df0b0c31ded4d1cc'
spark=Path(read(a.work/'evidence/deployment.json')['spark_home'])/'jars'
asm,=spark.glob('xbean-asm9-shaded-*.jar')
commons,=spark.glob('commons-lang3-*.jar')
jdk=a.work/'jdk17'
source=a.work/'source/sql-plugin-api/src/main/java/com/nvidia/spark/rapids/HashedPriorityQueue.java'
state={'status':'building','owner_thread':a.thread_id,'commands':[],'engine_sha256':sha(engine),
 'source_revision':subprocess.check_output(['git','-C',str(HERE),'rev-parse','HEAD'],text=True).strip(),
 'source_sha256':sha(source)}
def run(command,name,check=True):
 command=list(map(str,command));state['commands'].append(command);write(root/'state.json',state)
 with (root/name).open('x') as log:
  result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,timeout=180,check=check)
 return result
try:
 classes=root/'classes';classes.mkdir();replacement=root/'replacement';replacement.mkdir()
 run([jdk/'bin/javac','--release','8','-cp',commons,'-d',replacement,source],'queue-compile.log')
 junit=root/'junit-platform-console-standalone-1.10.2.jar'
 url='https://repo.maven.apache.org/maven2/org/junit/platform/junit-platform-console-standalone/1.10.2/'+junit.name
 urllib.request.urlretrieve(url,junit)
 expected=urllib.request.urlopen(url+'.sha1').read().decode().strip()
 assert hashlib.sha1(junit.read_bytes()).hexdigest()==expected
 state['junit']={'url':url,'sha256':sha(junit),'published_sha1':expected}
 run([jdk/'bin/javac','--release','8','-cp',str(engine)+':'+str(asm)+':'+str(junit),'-d',classes,
  HERE/'QueueIndexProbe.java',HERE/'QueueIndexTest.java',
  a.work/'source/tests/src/test/java/com/nvidia/spark/rapids/TestHashedPriorityQueue.java'],'probe-compile.log')
 agent=root/'queue-index-probe.jar'
 with zipfile.ZipFile(agent,'w',compression=zipfile.ZIP_DEFLATED) as out:
  out.writestr('META-INF/MANIFEST.MF','Manifest-Version: 1.0\nPremain-Class: com.nvidia.spark.rapids.experiments.QueueIndexProbe\n\n')
  for file in classes.rglob('*.class'):out.write(file,str(file.relative_to(classes)))
  for file in replacement.rglob('*.class'):out.write(file,'replacement/'+str(file.relative_to(replacement)))
  with zipfile.ZipFile(asm) as dep:
   for name in dep.namelist():
    if name.startswith('org/apache/xbean/asm9/') and name.endswith('.class'):out.writestr(name,dep.read(name))
  with zipfile.ZipFile(engine) as jar:
   names=['com/nvidia/spark/rapids/HashedPriorityQueue'+suffix for suffix in ('.class','$1.class')]
   state['baseline_classes']={name:hashlib.sha256(jar.read(name)).hexdigest() for name in names}
  out.writestr('queue-baseline.properties',''.join(name[:-6]+'='+value+'\n' for name,value in state['baseline_classes'].items()))
 cp=':'.join(map(str,(agent,engine,commons,junit)))
 for mode in ('control','candidate'):
  base=[jdk/'bin/java','-javaagent:'+str(agent)+'='+mode,'-cp',cp]
  run([*base,'org.junit.platform.console.ConsoleLauncher','execute','--select-class',
   'com.nvidia.spark.rapids.TestHashedPriorityQueue','--disable-banner','--reports-dir',root/('junit-'+mode)],'junit-'+mode+'.log')
  run([*base,'com.nvidia.spark.rapids.experiments.QueueIndexTest'],'state-'+mode+'.log')
  bounded=run([*base,'com.nvidia.spark.rapids.experiments.QueueIndexTest','bounded'],'bounded-'+mode+'.log',check=False)
  assert (bounded.returncode==0)==(mode=='candidate')
  if mode=='control':assert 'AssertionError: Queue state differs' in (root/'bounded-control.log').read_text()
 import xml.etree.ElementTree as ET
 for mode in ('control','candidate'):
  tests=ET.parse(root/('junit-'+mode)/'TEST-junit-jupiter.xml').getroot()
  assert int(tests.attrib['tests'])==9 and int(tests.attrib['failures'])==0 and int(tests.attrib['errors'])==0
 state.update(status='completed',agent_sha256=sha(agent),
  candidate_classes={str(f.relative_to(replacement)):sha(f) for f in replacement.rglob('*.class')},
  validation=['Nine existing queue tests pass on both implementations.',
   '10,000 differential operations cover equal-key removal, collisions, priority updates, copy iteration, growth and clear.',
   'Candidate drains 4096 items within two hash computations per item; frozen control rejects the same bound.'])
except BaseException as error:
 state.update(status='failed',error=repr(error));raise
finally:write(root/'state.json',state)
print(json.dumps({k:state[k] for k in ('status','agent_sha256','validation')}))
