"""Two real processes, ephemeral sequential election, resumable skill pair count."""
import argparse
import csv
import itertools
import json
import os
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

from kazoo.exceptions import BadVersionError

from common import (checksum, connect, encode, initialize, log,
                    normalize_skills, unpack_counts)


def post(endpoint, data):
    request = urllib.request.Request(os.environ['RECEIVER_URL'] + endpoint,
                                     data=encode(data),
                                     headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        payload = json.load(error)
        if error.code == 409 and payload['error'] == 'BadVersionError':
            raise BadVersionError() from error
        raise


def main(actor):
    run = os.environ['RUN_ID']
    fencing = os.getenv('FENCING', '1') == '1'
    data_path = Path(os.environ['DATA_PATH'])
    source_hash = checksum(data_path.read_bytes())
    zk = connect()
    zk.add_listener(lambda state: log(run, actor, 'session_state', state=state))
    base = initialize(zk, run)
    lock = zk.Lock(base + '/election', actor)
    log(run, actor, 'candidate', session_timeout=10, session_id=zk.client_id[0])
    try:
        with lock:
            # A completed job must not be recomputed by a late standby.
            current_result, _ = zk.get(base + '/result')
            if json.loads(current_result) is not None:
                log(run, actor, 'already_completed')
                return
            value, stat = zk.get(base + '/epoch')
            epoch = int(value) + 1
            epoch_stat = zk.set(base + '/epoch', str(epoch).encode(), version=stat.version)
            log(run, actor, 'became_leader', epoch=epoch, epoch_version=epoch_stat.version)
            pointer, _ = zk.get(base + '/checkpoint')
            pointer = json.loads(pointer)
            rows, counts = 0, Counter()
            if pointer:
                checkpoint = json.loads(Path(pointer['path']).read_bytes())
                if checkpoint['source_sha256'] != source_hash:
                    raise ValueError('Checkpoint belongs to another dataset')
                rows, counts = checkpoint['rows'], unpack_counts(checkpoint['counts'])
            log(run, actor, 'restored_checkpoint', rows=rows, unique_pairs=len(counts))
            payload = dict(run=run, coordinator=actor, epoch=epoch,
                           epoch_version=epoch_stat.version, fencing=fencing,
                           source_sha256=source_hash)
            batch_size = int(os.getenv('BATCH_ROWS', '200'))
            delay = float(os.getenv('BATCH_DELAY', '0.5'))
            can_checkpoint = True
            with data_path.open(encoding='utf-8-sig', newline='') as stream:
                reader = csv.DictReader(stream)
                if 'key_skills' not in (reader.fieldnames or []):
                    raise ValueError('Missing key_skills column')
                # Resume by logical CSV rows, including quoted multiline fields.
                for _ in range(rows):
                    next(reader)
                while True:
                    batch = list(itertools.islice(reader, batch_size))
                    if not batch:
                        break
                    # Deterministic slowdown makes a mid-computation pause observable.
                    time.sleep(delay)
                    for row in batch:
                        counts.update(itertools.combinations(normalize_skills(row['key_skills']), 2))
                    rows += len(batch)
                    payload.update(rows=rows, counts=[[a, b, count] for (a, b), count in sorted(counts.items())])
                    if can_checkpoint:
                        try:
                            post('/checkpoint', payload)
                            log(run, actor, 'checkpoint_saved', rows=rows, unique_pairs=len(counts))
                        except BadVersionError:
                            can_checkpoint = False
                            log(run, actor, 'stale_checkpoint_rejected', error='BadVersionError', rows=rows)
            # Intentionally let an obsolete worker attempt publication after resume.
            # Fencing must protect the recipient, independently of client callbacks.
            try:
                result = post('/publish', payload)
                log(run, actor, 'publication_accepted', **result)
            except BadVersionError:
                log(run, actor, 'publication_rejected', error='BadVersionError', epoch=epoch)
    finally:
        zk.stop()
        zk.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True)
    main(parser.parse_args().id)
