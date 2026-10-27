"""Validate private Bolt fixtures and measure their logical rows.

Correctness: exact census and values; legacy bootstrap never assumes new indexes.
Last revised: 2026-10-26
Last changed: Separate explicit legacy bootstrap from cold current-product setup.
"""
import argparse
import base64
import concurrent.futures
import faulthandler
import json
import random
import socket
import statistics
import struct
import threading
import time
import urllib.request
from pathlib import Path

from neo4j import GraphDatabase
from validated_multimetric_bench import make_payloads

QUERY='MATCH (n:BenchRow {pk: $pk}) RETURN n.pk AS pk, n.bucket AS bucket, n.payload AS payload'

def summary(samples):
    samples=sorted(samples)
    return {'operations':len(samples),'mean_ms':statistics.mean(samples)*1000,
            'p50_ms':statistics.median(samples)*1000,
            'p99_ms':samples[min(len(samples)-1,int(len(samples)*.99))]*1000}

def main():
    p=argparse.ArgumentParser(); p.add_argument('--runtime-config',type=Path,required=True)
    p.add_argument('--target',required=True); p.add_argument('--action',choices=['tcp','seed','measure','profile','profile-count','access'],required=True)
    p.add_argument('--legacy-bootstrap',action='store_true',help='Seed only: old Ferrosa schema inference with positive sentinels and no index expectation')
    p.add_argument('--out',type=Path,required=True); p.add_argument('--label',default='BenchRow'); p.add_argument('--reads',type=int,default=400); p.add_argument('--serial-reads',type=int,default=50); p.add_argument('--ready-file',type=Path); p.add_argument('--go-file',type=Path)
    a=p.parse_args(); is_profile=a.action.startswith('profile'); is_count=a.action=='profile-count'; faulthandler.dump_traceback_later(300,exit=True)
    assert not a.legacy_bootstrap or (a.target=='ferrosa' and a.action=='seed')
    global QUERY
    assert a.label in ('BenchRow','BenchWrite')
    QUERY=QUERY.replace('BenchRow',a.label)
    index_prefix='bench' if a.label=='BenchRow' else 'benchwrite'
    table_name=a.label.lower()+'_v'
    config=json.loads(a.runtime_config.read_text())[a.target]
    out={'target':a.target,'action':a.action,'legacy_bootstrap':a.legacy_bootstrap,'started_at':time.time(),'validated':False}
    def save():
        out['updated_at']=time.time()
        a.out.parent.mkdir(parents=True,exist_ok=True); a.out.write_text(json.dumps(out,indent=2,default=str)+'\n')
    if a.action=='tcp':
        rows=[]
        for _ in range(32):
            began=time.perf_counter_ns()
            with socket.create_connection((config['host'],config['bolt_port']),timeout=10) as sock:
                raw=sock.getsockopt(socket.IPPROTO_TCP,socket.TCP_INFO,256)
                rows.append({'connect_ms':(time.perf_counter_ns()-began)/1e6,
                             'kernel_syn_rtt_us':struct.unpack_from('=I',raw,68)[0],
                             'total_retrans':struct.unpack_from('=I',raw,100)[0]})
        out.update(samples=rows,kernel_syn_rtt_us_p50=statistics.median(r['kernel_syn_rtt_us'] for r in rows),validated=True)
        save(); return
    host='['+config['host']+']' if ':' in config['host'] else config['host']
    driver=GraphDatabase.driver(f"bolt://{host}:{config['bolt_port']}",auth=(config['user'],config['password']),
                               connection_timeout=10,max_transaction_retry_time=0,max_connection_pool_size=16)
    db='bench' if a.target=='ferrosa' else None
    payloads=make_payloads(512)
    def session(): return driver.session(database=db)
    def count():
        with session() as s:
            row=s.run(f'MATCH (n:{a.label}) RETURN count(n) AS c').single(); return row['c'] if row else 0
    def validate(row,pk):
        assert row is not None and tuple(row)==(pk,pk%64,payloads[pk%512]),'wrong point value'
    def http(path,body=None):
        token=base64.b64encode((config['user']+':'+config['password']).encode()).decode()
        req=urllib.request.Request(f'http://[{config["host"]}]:{config["graph_port"]}'+path,
            data=json.dumps(body).encode() if body is not None else None,
            headers={'Authorization':'Basic '+token,'Content-Type':'application/json'})
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=15) as response:
            return json.load(response)
    def metrics():
        if a.target!='ferrosa': return {}
        try:
            with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
                 f'http://[{config["host"]}]:9090/metrics',timeout=5) as response:
                lines=response.read().decode().splitlines()
            d={}
            for line in lines:
                if line.startswith(('ferrosa_storage_read_','ferrosa_sched_')):
                    k,v=line.rsplit(None,1); d[k]=float(v)
            return d
        except Exception as error:return {'capture_error':type(error).__name__}
    def census(expected):
        with session() as s:
            result=list(s.run(f'MATCH (n:{a.label}) RETURN n.pk AS pk,n.bucket AS bucket,n.payload AS payload'))
        assert len(result)==expected and {r[0] for r in result}==set(range(expected))
        for row in result: validate(row,row[0])
        assert count()==expected
    try:
        if a.action=='seed':
            if a.target=='ferrosa':
                from cassandra.cluster import Cluster
                from cassandra.auth import PlainTextAuthProvider
                from cassandra.policies import RoundRobinPolicy
                cluster=Cluster([config['host']],port=9042,protocol_version=4,compression=False,
                    auth_provider=PlainTextAuthProvider(username=config['user'],password=config['password']),
                    load_balancing_policy=RoundRobinPolicy(),connect_timeout=10,control_connection_timeout=10)
                ddl=["CREATE KEYSPACE IF NOT EXISTS bench WITH replication = {'class':'SimpleStrategy','replication_factor':1}"]
                try:
                    cs=cluster.connect()
                    for statement in ddl:cs.execute(statement,timeout=30)
                finally:cluster.shutdown()
                out['schema_bootstrap']=ddl
                with session() as x:
                    assert x.run('RETURN 1 AS ready').single()['ready']==1
                    if a.legacy_bootstrap:
                        # Both keys are outside 0..19999. Old inference needs all
                        # scalar fields in the first CREATE and cannot parse negatives.
                        began=time.perf_counter()
                        for sentinel in (20000,20001):
                            x.run(f"CREATE (n:{a.label} {{pk: {sentinel},bucket: {sentinel%64},payload: '__bootstrap__'}}) RETURN 1").consume()
                            values=x.run(f'MATCH (n:{a.label} {{pk: {sentinel}}}) RETURN n.pk,n.bucket,n.payload').single()
                            assert values is not None and tuple(values)==(sentinel,sentinel%64,'__bootstrap__')
                        out['legacy_schema_bootstrap_ms']=(time.perf_counter()-began)*1000
                        sentinels=(20000,20001)
                        out['ordinary_bootstrap_writes']=['CREATE pk=20000 with all three scalar fields',
                            'CREATE pk=20001 with all three scalar fields','validate and DELETE both owned sentinels']
                    else:
                        began=time.perf_counter()
                        x.run(f"CREATE (n:{a.label} {{pk: -2}}) RETURN 1").consume()
                        out['initial_label_setup_ms']=(time.perf_counter()-began)*1000
                        began=time.perf_counter()
                        x.run(f"CREATE (n:{a.label} {{pk: -1,bucket: -1,payload: '__bootstrap__'}}) RETURN 1").consume()
                        out['first_new_scalar_properties_ms']=(time.perf_counter()-began)*1000
                        out['first_write_and_index_setup_ms']=out['initial_label_setup_ms']+out['first_new_scalar_properties_ms']
                        assert x.run(f'MATCH (n:{a.label} {{pk: -2}}) RETURN n.pk').single()[0]==-2
                        assert x.run(f'MATCH (n:{a.label} {{payload: "__bootstrap__"}}) RETURN n.pk,n.bucket').single().values()==[-1,-1]
                        sentinels=(-2,-1)
                        out['ordinary_bootstrap_writes']=['CREATE pk=-2','CREATE pk=-1,bucket=-1,payload=bootstrap',
                            'validate old key and new scalar index','DELETE both sentinels by indexed pk']
                    for sentinel in sentinels:
                        x.run(f'MATCH (n:{a.label} {{pk: {sentinel}}}) DETACH DELETE n').consume()
                cluster=Cluster([config['host']],port=9042,protocol_version=4,compression=False,
                    auth_provider=PlainTextAuthProvider(username=config['user'],password=config['password']),
                    load_balancing_policy=RoundRobinPolicy())
                try:
                    cs=cluster.connect()
                    out['schema_columns']=[dict(row._asdict()) for row in cs.execute(
                        "SELECT column_name,type FROM system_schema.columns WHERE keyspace_name = 'bench' AND table_name = '"+table_name+"'")]
                    columns={row['column_name']:row['type'] for row in out['schema_columns']}
                    assert {'pk','bucket','payload'}<=columns.keys(),columns
                    assert columns['pk'] in ('int','bigint','varint') and columns['bucket'] in ('int','bigint','varint'),columns
                    assert columns['payload'] in ('text','varchar'),columns
                finally:cluster.shutdown()
                save()
            if a.target=='neo4j':
                with session() as x:
                    x.run(f'MATCH (n:{a.label}) DETACH DELETE n').consume()
                    for prop in ['pk','bucket','payload']:
                        x.run(f'CREATE INDEX {index_prefix}_{prop} IF NOT EXISTS FOR (n:{a.label}) ON (n.{prop})').consume()
                    x.run('CALL db.awaitIndexes(120)').consume()
            # Own fresh benchmark namespace; reject inherited nonempty data rather than relabeling it.
            initial=count()
            assert initial==0,'expected fresh graph fixture'
            out.update(rows=0,size_probes=[],index_attempts=[],insert_stages=[]); save()
            if a.target=='neo4j':
                with session() as s:out['version']=[dict(r) for r in s.run('CALL dbms.components() YIELD name,versions,edition RETURN name,versions,edition')]
                save()
            for stop in [500,2000,8000,20000]:
                load_start=time.perf_counter(); load_from=out['rows']
                for begin in range(out['rows'],stop,128):
                    batch=[{'pk':pk,'bucket':pk%64,'payload':payloads[pk%512]} for pk in range(begin,min(stop,begin+128))]
                    with session() as s:
                        s.run('UNWIND $rows AS row CREATE (n:'+a.label+' {pk: row.pk,bucket: row.bucket,payload: row.payload})',rows=batch).consume()
                    out['rows']=begin+len(batch)
                load_seconds=time.perf_counter()-load_start
                out['insert_stages'].append({'from':load_from,'to':stop,'seconds':load_seconds,'rows_s':(stop-load_from)/load_seconds})
                assert count()==stop
                samples=[]
                with session() as s:
                    for i in range(14):
                        pk=(i*47)%stop; began=time.perf_counter(); row=s.run(QUERY,pk=pk).single()
                        elapsed=time.perf_counter()-began; validate(row,pk)
                        if i>=4:samples.append(elapsed)
                out['size_probes'].append({'rows':stop,**summary(samples)}); save()
                print(json.dumps({'target':a.target,'seeded':stop,'point_p50_ms':out['size_probes'][-1]['p50_ms']}),flush=True)
            census(20000)
            out['insert_rows_s']=20000/sum(row['seconds'] for row in out['insert_stages'])
            if a.target=='ferrosa':
                cluster=Cluster([config['host']],port=9042,protocol_version=4,compression=False,
                    auth_provider=PlainTextAuthProvider(username=config['user'],password=config['password']),
                    load_balancing_policy=RoundRobinPolicy())
                try:
                    cs=cluster.connect()
                    out['automatic_indexes']=[dict(row._asdict()) for row in cs.execute(
                        "SELECT index_name,kind,options FROM system_schema.indexes WHERE keyspace_name = 'bench' AND table_name = '"+table_name+"'")]
                    if not a.legacy_bootstrap:
                        expected={'__bolt_'+table_name+'_'+prop+'_idx' for prop in ('pk','bucket','payload')}
                        assert {row['index_name'] for row in out['automatic_indexes']}==expected
                finally:cluster.shutdown()
            if a.target=='neo4j':
                with session() as s:
                    out['indexes']=[dict(r) for r in s.run("SHOW INDEXES YIELD name,state,type,labelsOrTypes,properties WHERE name IN "+repr([index_prefix+"_"+prop for prop in ("pk","bucket","payload")])+" RETURN name,state,type,labelsOrTypes,properties")]
                    out['plan']=s.run('EXPLAIN '+QUERY,pk=42).consume().plan
                assert len(out['indexes'])==3
                for prop in ['pk','bucket','payload']:
                    index=next(row for row in out['indexes'] if row['name']==index_prefix+'_'+prop)
                    assert index['state']=='ONLINE' and index['type']=='RANGE'
                    assert index['labelsOrTypes']==[a.label] and index['properties']==[prop]
                assert 'IndexSeek' in json.dumps(out['plan'],default=str)

            else:
                try:out['plan']=http('/graph/explain',{'query':QUERY.replace('$pk','42'),'keyspace':'bench'})
                except Exception as error:out['plan_error']={'type':type(error).__name__,'message':str(error)[:1000]}
                try:out['schema']=http('/graph/schema?keyspace=bench')
                except Exception as error:out['schema_error']=type(error).__name__
            out['validated']=True; save(); return
        if a.action=='access':
            assert a.target=='ferrosa'
            point=http('/graph/query',{'query':QUERY.replace('$pk','42'),'keyspace':'bench'})
            counted=http('/graph/query',{'query':'MATCH (n:'+a.label+') RETURN count(n) AS c','keyspace':'bench'})
            out.update(point=point,count=counted,validated=True); save(); return
        census(20000)
        out.update(rows=20000,payload_bytes=96,payload_variants=512,cases=[])
        rng=random.Random(1234); keys=[rng.randrange(20000) for _ in range(a.reads)]
        out['profile_kind']='count' if is_count else 'point'
        def perform(s,pk):
            return s.run('MATCH (n:'+a.label+') RETURN count(n) AS c').single() if is_count else s.run(QUERY,pk=pk).single()
        def check(row,pk):
            if is_count:assert row is not None and row['c']==20000
            else:validate(row,pk)
        for concurrency,n in ([(8,a.reads)] if is_profile else [(1,a.serial_reads),(8,a.reads)]):
            barrier=threading.Barrier(concurrency+1); go=threading.Event(); gate={}
            def worker(index):
                with session() as s:
                    for _ in range(4):check(perform(s,keys[index]),keys[index])
                    barrier.wait(timeout=45); assert go.wait(timeout=60)
                    samples=[]; i=index
                    while time.perf_counter()<gate['deadline'] if is_profile else i<n:
                        pk=keys[i%len(keys)]; began=time.perf_counter(); row=perform(s,pk)
                        elapsed=time.perf_counter()-began; check(row,pk); samples.append(elapsed); i+=concurrency
                    return samples,time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                jobs=[pool.submit(worker,i) for i in range(concurrency)]
                try:barrier.wait(timeout=45)
                except threading.BrokenBarrierError:
                    go.set()
                    for job in jobs:job.result()
                    raise
                if a.ready_file:
                    a.ready_file.touch(); deadline=time.monotonic()+60
                    while not a.go_file.exists():
                        if time.monotonic()>deadline:raise TimeoutError('profile gate')
                        time.sleep(.02)
                before=metrics(); began=time.perf_counter(); gate['deadline']=began+30; go.set()
                results=[j.result(timeout=200) for j in jobs]
            samples=[x for xs,_ in results for x in xs]; elapsed=max(end for _,end in results)-began
            if not is_profile:assert len(samples)==n
            out['cases'].append({'concurrency':concurrency,**summary(samples),'elapsed_s':elapsed,
                                 'ops_s':len(samples)/elapsed,'metrics_before':before,'metrics_after':metrics()}); save()
        if not is_profile:
            counts=[]
            for _ in range(5):
                began=time.perf_counter(); assert count()==20000; counts.append(time.perf_counter()-began)
            out['count']=summary(counts)
            began=time.perf_counter()
            with session() as s:scan=list(s.run(f'MATCH (n:{a.label}) RETURN n.pk AS pk,n.bucket AS bucket,n.payload AS payload LIMIT 10000'))
            elapsed=time.perf_counter()-began
            assert len(scan)==10000 and len({r[0] for r in scan})==10000 and all(0<=r[0]<20000 for r in scan)
            for row in scan:validate(row,row[0])
            out['scan']={'rows':10000,'seconds':elapsed,'rows_s':10000/elapsed}
        census(20000); out['validated']=True; save()
    finally:driver.close()

if __name__=='__main__':main()
