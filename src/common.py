import csv
import hashlib
import io
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

ROOT = Path('/app')
HOSTS = os.getenv('ZK_HOSTS', 'localhost:2181,localhost:2182,localhost:2183')


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True).encode('utf-8')


def log(run, actor, event, **fields):
    record = dict(ts=time.time(), utc=datetime.now(timezone.utc).isoformat(),
                  actor=actor, event=event, **fields)
    line = encode(record).decode('utf-8')
    path = ROOT / 'logs' / run / f'{actor}.jsonl'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as stream:
        stream.write(line + '\n')
    print(line, flush=True)
    return record


def connect(hosts=HOSTS, timeout=15):
    client = KazooClient(hosts=hosts, timeout=10.0)
    try:
        client.start(timeout=timeout)
    except BaseException:
        client.stop()
        client.close()
        raise
    return client


def initialize(zk, run):
    base = f'/skills-failover/{run}'
    zk.ensure_path(base)
    for name, data in [('epoch', b'0'), ('checkpoint', b'null'), ('result', b'null')]:
        try:
            zk.create(f'{base}/{name}', data)
        except NodeExistsError:
            pass
    return base


def normalize_skills(value):
    # Same normalization as homework 3: src/mapreduce.py.
    return sorted({s.strip().casefold() for s in (value or '').split(';') if s.strip()})


def pairs_csv(counter):
    stream = io.StringIO(newline='')
    writer = csv.writer(stream)
    writer.writerow(['skill_1', 'skill_2', 'vacancy_count'])
    for pair, count in sorted(counter.items(), key=lambda item: (-item[1], item[0])):
        writer.writerow([*pair, count])
    return b'\xef\xbb\xbf' + stream.getvalue().encode('utf-8')


def unpack_counts(items):
    return Counter({(a, b): count for a, b, count in items})


def checksum(data):
    return hashlib.sha256(data).hexdigest()


def checked_commit(transaction):
    results = transaction.commit()
    # Kazoo returns exception objects for failed multi operations.
    for result in results:
        if isinstance(result, Exception) and type(result).__name__ == 'BadVersionError':
            raise result
    for result in results:
        if isinstance(result, Exception):
            raise result
    return results
