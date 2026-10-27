#!/bin/sh
set -eu
exec python3 - "$1" <<'SAMPLE_PY'
import datetime,json,os,time,urllib.request
from pathlib import Path
out=Path(__import__('sys').argv[1]);out.mkdir(parents=True,exist_ok=True)
def read(p):
 try:return Path(p).read_text(errors='replace')
 except OSError:return ''
def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
(out/'started-at.txt').write_text(stamp());(out/'clock-ticks.txt').write_text(str(os.sysconf('SC_CLK_TCK'))+'\n')
for source,dest in [('/proc/mounts','mounts.txt'),('/proc/cpuinfo','cpuinfo.txt'),('/proc/meminfo','meminfo.txt')]: (out/dest).write_text(read(source))
hw={'uname':list(os.uname()),'cpu_count':os.cpu_count(),'boot_id':read('/proc/sys/kernel/random/boot_id').strip(),'limits':{p:read('/sys/fs/cgroup/'+p).strip() for p in ['cpu.max','cpuset.cpus.effective','memory.max','memory.swap.max','io.max']}}
hw['java_heap_flags']=[]
for p in Path('/proc').iterdir():
 if p.name.isdecimal() and read(p/'comm').strip()=='java':
  try:args=(p/'cmdline').read_bytes().split(bytes([0]))
  except OSError:continue
  hw['java_heap_flags'] += [a.decode(errors='replace') for a in args if a.startswith((b'-Xmx',b'-Xms',b'-XX:MaxRAMPercentage'))]
(out/'hardware.json').write_text(json.dumps(hw,indent=2));(out/'ready').touch()
paths=['/proc/stat','/proc/diskstats','/proc/net/dev','/proc/meminfo','/proc/vmstat','/proc/pressure/cpu','/proc/pressure/io','/proc/pressure/memory','/sys/fs/cgroup/cpu.stat','/sys/fs/cgroup/memory.events','/sys/fs/cgroup/memory.current','/sys/fs/cgroup/memory.stat','/sys/fs/cgroup/io.stat']
with (out/'resources.txt').open('a',buffering=1) as log:
 for n in range(7200):
  if (out/'stop').exists():break
  start=time.monotonic();log.write(str(time.time())+'\n')
  for p in paths:log.write('FILE '+p+'\n'+read(p)+'\n')
  for p in Path('/proc').iterdir():
   if not p.name.isdecimal():continue
   name=read(p/'comm').strip()
   if name not in ('ferrosa','postgres','java','oxigraph','python3') and not name.startswith('ld-linux'):continue
   for f in ['stat','status','io']:
    value=read(p/f)
    if value:log.write('FILE '+str(p/f)+'\n'+value+'\n')
   try:threads=list((p/'task').iterdir())
   except OSError:continue
   for t in threads:
    for f in ['schedstat','wchan']:log.write('FILE '+str(t/f)+'\n'+read(t/f)+'\n')
   if (name=='ferrosa' or name.startswith('ld-linux')) and n%10==0:
    try:
     with urllib.request.urlopen('http://127.0.0.1:9090/metrics',timeout=.2) as req:(out/('metrics-'+str(n)+'.prom')).write_bytes(req.read())
    except OSError:pass
  time.sleep(max(0,1-(time.monotonic()-start)))
(out/'finished-at.txt').write_text(stamp());(out/'done').touch()
SAMPLE_PY
