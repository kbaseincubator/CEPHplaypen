#!/usr/bin/env python3
"""Run warp analyze on .zst files, parse output into a CSV, then compare with viz_warp TSV."""

import csv
import json
import re
import subprocess
import sys
from pathlib import Path

from backports import zstd

MIB_TO_MB = 1048576 / 1e6  # warp reports MiB/s; convert to MB/s to match viz_warp

_SIZE_UNITS = {'B': 1, 'KB': 1_000, 'MB': 1_000**2, 'GB': 1_000**3,
               'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3}
_OP_ORDER = {'GET': 0, 'PUT': 1, 'DELETE': 2, 'STAT': 3}


def _size_bytes(size_str):
    m = re.match(r'^([0-9.]+)\s*([A-Za-z]+)$', size_str)
    return int(float(m.group(1)) * _SIZE_UNITS.get(m.group(2), 1)) if m else 0


def _row_sort_key(row):
    return (_size_bytes(row[0]), int(row[1]), _OP_ORDER.get(row[3], 99))


def _extract_params(cmdline):
    params = {}
    for pattern, key, cast in [
        (r'--concurrent=(\d+)', 'concurrency', int),
        (r'--obj\.size=(\S+)', 'obj_size_str', str),
        (r'--duration=(\S+)', 'duration', str),
    ]:
        m = re.search(pattern, cmdline)
        if m:
            params[key] = cast(m.group(1))
    return params


def _fmt_duration(dur_str):
    m = re.fullmatch(r'(?:(\d+)m)?(?:(\d+)s)?', dur_str or '')
    if not m or not m.group(0):
        return dur_str
    return f'{int(m.group(1) or 0) * 60 + int(m.group(2) or 0)}s'


def _parse_analyze(text):
    """Parse warp analyze output into a list of dicts, one per operation."""
    results = []
    for block in re.split(r'─+', text):
        m = re.search(r'Report: (\w+) \((\d+) reqs\)', block)
        if not m or m.group(1) == 'Total':
            continue
        op = m.group(1)
        n = int(m.group(2))

        mib_m = re.search(r'Average: ([\d.]+) MiB/s,\s*([\d.]+) obj/s', block)
        ops_m = re.search(r'Average: ([\d.]+) obj/s', block)
        avg_mb_s = round(float(mib_m.group(1)) * MIB_TO_MB, 3) if mib_m else 0.0
        avg_ops_s = float(mib_m.group(2)) if mib_m else float(ops_m.group(1)) if ops_m else 0.0

        p50_m = re.search(r'50%: ([\d.]+)ms', block)
        std_m = re.search(r'StdDev: ([\d.]+)ms', block)
        p50_ms = float(p50_m.group(1)) if p50_m else ''
        stddev_ms = float(std_m.group(1)) if std_m else ''

        results.append({
            'operation': op,
            'avg_mb_s': avg_mb_s,
            'avg_ops_s': avg_ops_s,
            'p50_ms': p50_ms,
            'stddev_ms': stddev_ms,
            'n': n,
        })
    return results


def make_gold_standard(zst_files, out_path, warp):
    rows = []
    for path in sorted(zst_files):
        with zstd.open(path, 'rt') as f:
            data = json.load(f)
        params = _extract_params(data['commandline'])
        size = params.get('obj_size_str', '?')
        conc = params.get('concurrency', '?')
        dur = _fmt_duration(params.get('duration', ''))

        result = subprocess.run([warp, 'analyze', path],
                                capture_output=True, text=True)
        for op_row in _parse_analyze(result.stdout):
            rows.append([size, conc, dur, op_row['operation'],
                         op_row['avg_mb_s'], op_row['avg_ops_s'],
                         op_row['p50_ms'], op_row['stddev_ms'], op_row['n']])

    rows.sort(key=_row_sort_key)
    with open(out_path, 'w', newline='') as f:
        w = csv.writer(f, delimiter='\t')
        w.writerow(['obj_size', 'concurrency', 'duration', 'operation',
                    'avg_mb_s', 'avg_ops_s', 'p50_ms', 'stddev_ms', 'n'])
        w.writerows(rows)
    print(f'Saved {out_path}')
    return rows


def compare(mine_path, gold_path):
    def load(path):
        with open(path) as f:
            return list(csv.DictReader(f, delimiter='\t'))

    mine = {(r['obj_size'], r['concurrency'], r['duration'], r['operation']): r
            for r in load(mine_path)}
    gold = {(r['obj_size'], r['concurrency'], r['duration'], r['operation']): r
            for r in load(gold_path)}

    cols = ['avg_mb_s', 'avg_ops_s', 'p50_ms', 'stddev_ms']
    header = f"{'key':<30} {'col':<12} {'mine':>12} {'gold':>12} {'diff%':>8}"
    print(header)
    print('-' * len(header))

    for key in sorted(mine):
        if key not in gold:
            print(f"{' | '.join(key):<30}  NOT IN GOLD")
            continue
        m, g = mine[key], gold[key]
        for col in cols:
            mv = m.get(col, '')
            gv = g.get(col, '')
            try:
                mf, gf = float(mv), float(gv)
                pct = f'{abs(mf - gf) / gf * 100:.1f}%' if gf else 'n/a'
            except (ValueError, ZeroDivisionError):
                pct = 'n/a'
            label = ' | '.join(key)
            print(f'{label:<30} {col:<12} {str(mv):>12} {str(gv):>12} {pct:>8}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Generate warp gold standard TSV and compare with viz_warp output.')
    parser.add_argument('json_files', nargs='+', metavar='FILE')
    parser.add_argument('--warp', required=True, metavar='PATH', help='Path to warp binary')
    parser.add_argument('-o', '--output', default='warp_gold_standard.tsv', metavar='FILE')
    parser.add_argument('--compare', default='warp_benchmark.tsv', metavar='FILE')
    args = parser.parse_args()

    make_gold_standard(args.json_files, args.output, args.warp)
    print()
    compare(args.compare, args.output)
