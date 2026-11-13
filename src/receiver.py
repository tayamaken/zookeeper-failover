"""Publication receiver: epoch check and pointer update are one ZK transaction."""
import json
import re
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from kazoo.exceptions import BadVersionError

from common import (ROOT, checked_commit, checksum, connect, encode, initialize,
                    log, pairs_csv, unpack_counts)

zk = None
write_lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, code, body):
        payload = encode(body)
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        self.reply(200, {'healthy': zk.connected})

    def do_POST(self):
        try:
            data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            run = data['run']
            if not re.fullmatch(r'[a-zA-Z0-9_-]+', run):
                raise ValueError('Invalid run id')
            with write_lock:
                self.store(data)
        except BadVersionError:
            log(data['run'], 'receiver', 'rejected', operation=self.path,
                coordinator=data['coordinator'], epoch=data['epoch'], error='BadVersionError')
            self.reply(409, {'error': 'BadVersionError'})
        except Exception as error:
            self.reply(500, {'error': type(error).__name__, 'detail': str(error)})

    def store(self, data):
        run, actor = data['run'], data['coordinator']
        if not re.fullmatch(r'[a-zA-Z0-9_-]+', actor):
            raise ValueError('Invalid coordinator id')
        base = initialize(zk, run)
        if self.path == '/checkpoint':
            # Unique, immutable object; the published pointer is the checkpoint.
            path = ROOT / '.runtime' / run / f'checkpoint-{uuid.uuid4().hex}.json'
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(encode(data))
            pointer = {'path': str(path), 'rows': data['rows'], 'epoch': data['epoch'],
                       'source_sha256': data['source_sha256']}
            txn = zk.transaction()
            txn.check(base + '/epoch', version=data['epoch_version'])
            txn.set_data(base + '/checkpoint', encode(pointer))
            checked_commit(txn)
            self.reply(200, pointer)
        elif self.path == '/publish':
            result = pairs_csv(unpack_counts(data['counts']))
            relative = f'results/{run}/{actor}-epoch-{data["epoch"]}.csv'
            path = ROOT / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(result)
            pointer = {'coordinator': actor, 'epoch': data['epoch'], 'path': relative,
                       'rows': data['rows'], 'sha256': checksum(result),
                       'unique_pairs': len(data['counts']), 'pair_occurrences':
                       sum(row[2] for row in data['counts']), 'fencing': data['fencing']}
            txn = zk.transaction()
            if data['fencing']:
                txn.check(base + '/epoch', version=data['epoch_version'])
            txn.set_data(base + '/result', encode(pointer))
            checked_commit(txn)
            stat = zk.exists(base + '/result')
            log(run, 'receiver', 'published', message='записал результат',
                pointer_version=stat.version, **pointer)
            self.reply(200, dict(pointer, pointer_version=stat.version))
        else:
            self.reply(404, {'error': 'Unknown operation'})


if __name__ == '__main__':
    zk = connect()
    try:
        ThreadingHTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
    finally:
        zk.stop()
        zk.close()
