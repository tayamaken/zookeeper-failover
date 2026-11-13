import argparse
import json
import socket
import sys
import uuid

from common import connect


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['roles', 'write', 'snapshot'])
    parser.add_argument('--hosts', default='zk1:2181,zk2:2181,zk3:2181')
    parser.add_argument('--run')
    parser.add_argument('--expect-unavailable', action='store_true')
    args = parser.parse_args()
    if args.action == 'roles':
        for host in ['zk1', 'zk2', 'zk3']:
            try:
                with socket.create_connection((host, 2181), 2) as stream:
                    stream.settimeout(3)
                    stream.sendall(b'srvr')
                    chunks = []
                    while data := stream.recv(65536):
                        chunks.append(data)
                print(host + '\n' + b''.join(chunks).decode(), flush=True)
            except OSError as error:
                print(f'{host}: unavailable ({error})', flush=True)
        return
    try:
        zk = connect(args.hosts, timeout=5)
    except Exception as error:
        print(json.dumps({'available': False, 'error': type(error).__name__}))
        return 0 if args.expect_unavailable else 1
    try:
        if args.expect_unavailable:
            raise AssertionError('Connected despite expected loss of quorum')
        if args.action == 'write':
            path = '/quorum-probe-' + uuid.uuid4().hex
            zk.create(path, b'quorum-write-ok')
            value, _ = zk.get(path)
            zk.delete(path)
            print(json.dumps({'available': True, 'write_read_delete': value.decode()}))
        else:
            snapshot = {}
            base = f'/skills-failover/{args.run}'
            for name in ['epoch', 'checkpoint', 'result']:
                value, stat = zk.get(base + '/' + name)
                snapshot[name] = {'value': json.loads(value), 'version': stat.version}
            print(json.dumps(snapshot, ensure_ascii=False, sort_keys=True))
    finally:
        zk.stop()
        zk.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
