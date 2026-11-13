"""Copy the existing homework 3 dataset without downloading or changing it."""
import argparse
import hashlib
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path,
                        default=ROOT.parent / 'java-vacancy-analysis/data/java_vacancies.csv')
    args = parser.parse_args()
    source = args.source.resolve(strict=True)
    metadata = json.loads((ROOT / 'baseline/source.json').read_text(encoding='utf-8'))
    actual = hashlib.sha256(source.read_bytes()).hexdigest()
    if actual != metadata['dataset_sha256']:
        raise SystemExit('Dataset SHA256 differs from homework 3; file was not copied.')
    target = ROOT / 'data/java_vacancies.csv'
    target.parent.mkdir(exist_ok=True)
    if source != target.resolve():
        shutil.copyfile(source, target)
    print(f'Input ready: {target} (SHA256 {actual})')


if __name__ == '__main__':
    main()
