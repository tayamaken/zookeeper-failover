"""Verify saved evidence independently, without Docker or third-party packages."""
import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def pairs(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        return {(r['skill_1'], r['skill_2']): int(r['vacancy_count']) for r in csv.DictReader(stream)}


def main():
    metadata = json.loads((ROOT / 'baseline/source.json').read_text(encoding='utf-8'))
    baseline_path = ROOT / 'baseline/skill_pairs.csv'
    assert hashlib.sha256(baseline_path.read_bytes()).hexdigest() == metadata['baseline_sha256']
    baseline = pairs(baseline_path)
    assert len(baseline) == metadata['unique_pairs']
    evidence = json.loads((ROOT / 'results/verification.json').read_text(encoding='utf-8'))
    assert evidence['ensemble']['one_down_write_ok']
    assert evidence['ensemble']['two_down_unavailable']
    assert evidence['ephemeral']['existed_before_close']
    assert evidence['ephemeral']['absent_after_close']
    assert len(evidence['runs']) == 2
    assert {run['fencing'] for run in evidence['runs']} == {False, True}
    for run in evidence['runs']:
        count = 1 if run['fencing'] else 2
        assert run['accepted_publications'] == count
        assert run['final_pointer']['version'] == count
        assert run['pause_seconds'] >= 40
        assert 0 < run['restored_rows'] < metadata['rows']
        receiver_events = [json.loads(line) for line in
                           (ROOT / 'logs' / run['run'] / 'receiver.jsonl').read_text(encoding='utf-8').splitlines()]
        publications = [e for e in receiver_events if e['event'] == 'published']
        assert len(publications) == count
        assert [e['pointer_version'] for e in publications] == list(range(1, count + 1))
        state = json.loads((ROOT / 'results' / run['run'] / 'zookeeper-state.json').read_text(encoding='utf-8'))
        assert state['result'] == run['final_pointer']
        expected_actor = run['standby'] if run['fencing'] else run['leader']
        assert state['result']['value']['coordinator'] == expected_actor
        leader_log = [json.loads(line) for line in
                      (ROOT / 'logs' / run['run'] / f'{run["leader"]}.jsonl').read_text(encoding='utf-8').splitlines()]
        standby_log = [json.loads(line) for line in
                       (ROOT / 'logs' / run['run'] / f'{run["standby"]}.jsonl').read_text(encoding='utf-8').splitlines()]
        promoted = next(e for e in standby_log if e['event'] == 'became_leader')
        restored = next(e for e in standby_log if e['event'] == 'restored_checkpoint')
        assert restored['rows'] == run['restored_rows']
        assert abs(promoted['ts'] - run['pause_confirmed_ts'] - run['failover_seconds']) < 0.00001
        if run['fencing']:
            assert any(e['event'] == 'publication_rejected' and e['error'] == 'BadVersionError' for e in leader_log)
            assert any(e['event'] == 'rejected' and e['operation'] == '/publish' for e in receiver_events)
        for item in run['baseline_comparisons']:
            path = ROOT / item['path']
            assert pairs(path) == baseline
            assert hashlib.sha256(path.read_bytes()).hexdigest() == item['sha256']
        assert pairs(ROOT / state['result']['value']['path']) == baseline
        print(f'{run["run"]}: OK; publications={count}; pointer_version={count}; '
              f'restored_rows={run["restored_rows"]}; failover={run["failover_seconds"]:.3f}s')
    print('All saved evidence and every pair count match homework 3.')


if __name__ == '__main__':
    main()
