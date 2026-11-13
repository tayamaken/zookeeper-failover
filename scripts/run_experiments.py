"""Run the actual Docker experiments and retain command output; stdlib only."""
import csv
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
OUTPUT = ROOT / 'logs' / 'commands.txt'
ENV = dict(os.environ)


def command(*args, timeout=180, check=True):
    result = subprocess.run(args, env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            encoding='utf-8', errors='replace', timeout=timeout)
    text = '$ ' + ' '.join(args) + '\n' + result.stdout + f'\nexit_code={result.returncode}\n'
    with OUTPUT.open('a', encoding='utf-8') as stream:
        stream.write(text + '\n')
    print(text, flush=True)
    if check and result.returncode:
        raise RuntimeError(f'Command failed: {args}')
    return result.stdout


def compose(*args, **kwargs):
    return command('docker', 'compose', *args, **kwargs)


def probe(*args):
    return compose('exec', '-T', 'receiver', 'python', 'src/probe.py', *args)


def events(run, actor):
    path = ROOT / 'logs' / run / f'{actor}.jsonl'
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]


def wait_for(predicate, timeout=90, description='condition'):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.2)
    raise TimeoutError(description)


def quorum_experiment():
    roles = probe('roles')
    # Parse each named section, keeping all the raw srvr output in commands.txt.
    leader = None
    for node in ['zk1', 'zk2', 'zk3']:
        section = roles.split(node + '\n', 1)[1].split('\nzk', 1)[0]
        if 'Mode: leader' in section:
            leader = node
    assert leader is not None
    followers = [node for node in ['zk1', 'zk2', 'zk3'] if node != leader]
    first, survivor = followers
    try:
        compose('stop', first)
        probe('write', '--hosts', f'{survivor}:2181')
        probe('roles')
        compose('stop', leader)
        probe('write', '--hosts', f'{survivor}:2181', '--expect-unavailable')
        probe('roles')
    finally:
        compose('start', first, leader)
    compose('up', '-d', '--wait', 'zk1', 'zk2', 'zk3', 'receiver')
    probe('write')
    probe('roles')
    return {'initial_leader': leader, 'stopped_first': first, 'survivor': survivor,
            'one_down_write_ok': True, 'two_down_unavailable': True, 'recovered': True}


def ephemeral_experiment():
    # The owner is a real zkCli Java process, with its own session and open stdin.
    path = '/homework-ephemeral-' + str(int(time.time()))
    cli_output = ROOT / 'logs' / 'zkcli-owner.txt'
    with cli_output.open('w', encoding='utf-8') as stream:
        owner = subprocess.Popen(['docker', 'compose', 'exec', '-T', 'zk1',
                                  'zkCli.sh', '-server', 'zk1:2181'],
                                 stdin=subprocess.PIPE, stdout=stream, stderr=subprocess.STDOUT,
                                 text=True, encoding='utf-8', env=ENV)
        try:
            owner.stdin.write(f'create -e {path} ephemeral-demo\n')
            owner.stdin.flush()
            wait_for(lambda: f'Created {path}' in cli_output.read_text(encoding='utf-8', errors='replace'),
                     description='zkCli ephemeral creation')
            before = compose('exec', '-T', 'zk2', 'zkCli.sh', '-server', 'zk2:2181', 'get', '-s', path)
            assert 'ephemeral-demo' in before and 'ephemeralOwner = 0x0' not in before
            owner.stdin.write('quit\n')
            owner.stdin.flush()
            owner.wait(timeout=20)
        finally:
            if owner.poll() is None:
                owner.terminate()
                owner.wait(timeout=10)
    after = compose('exec', '-T', 'zk2', 'zkCli.sh', '-server', 'zk2:2181', 'get', path, check=False)
    assert 'Node does not exist' in after
    return {'path': path, 'created_by': 'zkCli', 'existed_before_close': True, 'absent_after_close': True}


def read_pairs(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        return {(row['skill_1'], row['skill_2']): int(row['vacancy_count']) for row in csv.DictReader(stream)}


def failover_experiment(fencing, stamp):
    run = ('fenced-' if fencing else 'unfenced-') + stamp
    ENV.update(RUN_ID=run, FENCING=str(int(fencing)))
    compose('up', '-d', '--no-deps', '--force-recreate', 'coordinator-a', 'coordinator-b')
    paused = None
    try:
        def ready():
            for actor in ['coordinator-a', 'coordinator-b']:
                actor_events = events(run, actor)
                if any(e['event'] == 'checkpoint_saved' and e['rows'] >= 600 for e in actor_events):
                    return actor
        leader = wait_for(ready, description='leader checkpoint at row 600')
        standby = 'coordinator-b' if leader == 'coordinator-a' else 'coordinator-a'
        command_start = time.time()
        compose('pause', leader)
        paused = leader
        pause_confirmed = time.time()
        print(f'Paused {leader}, waiting for {standby} to complete before resume.', flush=True)
        def backup_finished():
            return next((e for e in events(run, standby) if e['event'] == 'publication_accepted'), None)
        backup_result = wait_for(backup_finished, timeout=60, description='standby publication')
        # Hold the whole container frozen for at least 40 seconds, including heartbeats.
        wait_for(lambda: time.time() - pause_confirmed >= 40, timeout=45, description='40 second pause')
        compose('unpause', leader)
        paused = None
        resume_confirmed = time.time()
        expected_event = 'publication_rejected' if fencing else 'publication_accepted'
        stale_result = wait_for(lambda: next((e for e in events(run, leader)
                                              if e['event'] == expected_event), None),
                                timeout=90, description='resumed leader publication attempt')
        # Verify both coordinator processes exited cleanly, including lock/session cleanup.
        for actor in [leader, standby]:
            cid = compose('ps', '-a', '-q', actor).strip()
            assert command('docker', 'wait', cid, timeout=30).strip() == '0'
        snapshot = json.loads(probe('snapshot', '--run', run).strip().splitlines()[-1])
        target_dir = ROOT / 'results' / run
        (target_dir / 'zookeeper-state.json').write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding='utf-8')
        publications = [e for e in events(run, 'receiver') if e['event'] == 'published']
        assert len(publications) == (1 if fencing else 2)
        assert snapshot['result']['version'] == (1 if fencing else 2)
        assert snapshot['result']['value']['coordinator'] == (standby if fencing else leader)
        if fencing:
            assert stale_result['error'] == 'BadVersionError'
        restored = next(e for e in events(run, standby) if e['event'] == 'restored_checkpoint')
        assert 0 < restored['rows'] < 6907
        promoted = next(e for e in events(run, standby) if e['event'] == 'became_leader')
        baseline = read_pairs(ROOT / 'baseline' / 'skill_pairs.csv')
        comparisons = []
        for path in sorted(target_dir.glob('*.csv')):
            actual = read_pairs(path)
            assert actual == baseline, f'Results differ from homework 3: {path}'
            comparisons.append({'path': path.relative_to(ROOT).as_posix(), 'all_pairs_equal_hw3': True,
                                'unique_pairs': len(actual), 'pair_occurrences': sum(actual.values()),
                                'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
        assert len(comparisons) == 2
        summary = dict(run=run, fencing=fencing, leader=leader, standby=standby,
                       pause_command_started_ts=command_start, pause_confirmed_ts=pause_confirmed,
                       standby_became_leader_ts=promoted['ts'],
                       failover_seconds=round(promoted['ts'] - pause_confirmed, 6),
                       pause_seconds=round(resume_confirmed - pause_confirmed, 6),
                       restored_rows=restored['rows'], accepted_publications=len(publications),
                       final_pointer=snapshot['result'], baseline_comparisons=comparisons,
                       resumed_publication_event=stale_result['event'])
        (target_dir / 'verification.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
        print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
        return summary
    finally:
        if paused:
            compose('unpause', paused, check=False)
        compose('stop', 'coordinator-a', 'coordinator-b', check=False)


def main():
    OUTPUT.parent.mkdir(exist_ok=True)
    stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
    compose('config', '--quiet')
    compose('build', 'receiver', timeout=600)
    compose('up', '-d', '--wait', 'zk1', 'zk2', 'zk3', 'receiver', timeout=240)
    try:
        ensemble = quorum_experiment()
        ephemeral = ephemeral_experiment()
        runs = [failover_experiment(False, stamp), failover_experiment(True, stamp)]
        result = dict(ensemble=ensemble, ephemeral=ephemeral, runs=runs)
        (ROOT / 'results' / 'verification.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        print('ALL EXPERIMENTS PASSED', flush=True)
    finally:
        for node in ['zk1', 'zk2', 'zk3']:
            output = compose('logs', '--no-color', '--no-log-prefix', node, check=False)
            (ROOT / 'logs' / f'{node}.log').write_text(output, encoding='utf-8')
            config = compose('exec', '-T', node, 'sh', '-c', 'cat /conf/zoo.cfg; echo MYID; cat /data/myid', check=False)
            (ROOT / 'logs' / f'{node}-config.txt').write_text(config, encoding='utf-8')
        compose('logs', '--no-color', 'receiver', check=False)


if __name__ == '__main__':
    main()
