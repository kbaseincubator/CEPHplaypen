#!/usr/bin/env python3
"""Generate benchmark charts from warp mixed JSON output files."""

import argparse
import csv
import glob
import json
import re
import sys
from pathlib import Path

from backports import zstd

import matplotlib.pyplot as plt
import numpy as np

from parse_warp import parse_warp_json

_SIZE_UNITS = {
    'B': 1, 'KB': 1_000, 'MB': 1_000**2, 'GB': 1_000**3,
    'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3,
}

_OP_COLORS = {
    'GET': '#4c72b0', 'PUT': '#dd8452', 'DELETE': '#55a868', 'STAT': '#c44e52',
}


def _parse_obj_size_bytes(size_str):
    m = re.match(r'^([0-9.]+)\s*([A-Za-z]+)$', size_str)
    if not m:
        return 0
    return int(float(m.group(1)) * _SIZE_UNITS.get(m.group(2), 1))


def _extract_params(cmdline):
    params = {}
    for pattern, key, cast in [
        (r'--concurrent=(\d+)', 'concurrency', int),
        (r'--obj\.size=(\S+)', 'obj_size_str', str),
        (r'--duration=(\S+)', 'duration', str),
        (r'--objects=(\d+)', 'objects', int),
    ]:
        m = re.search(pattern, cmdline)
        if m:
            params[key] = cast(m.group(1))
    if 'obj_size_str' in params:
        params['obj_size_bytes'] = _parse_obj_size_bytes(params['obj_size_str'])
    return params


def _open(path):
    p = Path(path)
    if p.suffix == '.zst':
        return zstd.open(p, 'rt', encoding='utf-8')
    return open(p)


def _load_runs(json_files):
    runs = []
    for path in json_files:
        with _open(path) as f:
            data = json.load(f)
        params = _extract_params(data['commandline'])
        ops = {op['operation']: op for op in parse_warp_json(data)}
        runs.append({'path': str(path), 'params': params, 'ops': ops})
    runs.sort(key=lambda r: (r['params'].get('obj_size_bytes', 0),
                              r['params'].get('concurrency', 0)))
    return runs


def _fmt_duration(dur_str):
    """Convert Go duration string like '2m0s' or '180s' to plain seconds."""
    m = re.fullmatch(r'(?:(\d+)m)?(?:(\d+)s)?', dur_str or '')
    if not m or not m.group(0):
        return dur_str
    minutes, seconds = int(m.group(1) or 0), int(m.group(2) or 0)
    return f'{minutes * 60 + seconds}s'


def _xtick(params):
    size = params.get('obj_size_str', '?')
    conc = params.get('concurrency', '?')
    dur = _fmt_duration(params.get('duration', ''))
    return f"{size}\nc={conc}\nd={dur}"


def _fmt_count(n):
    return f'n={n/1000:.0f}k' if n >= 1000 else f'n={n}'


def _annotate_counts(ax, bars, counts, threshold=100, y_tops=None):
    for i, (bar, n) in enumerate(zip(bars, counts)):
        if n >= threshold:
            continue
        h = bar.get_height()
        if h <= 0:
            continue
        x = bar.get_x() + bar.get_width() / 2
        top = y_tops[i] if y_tops is not None else h
        ax.annotate(_fmt_count(n), xy=(x, top), xytext=(0, 4),
                    textcoords='offset points', ha='center', va='bottom',
                    fontsize=10, rotation=90, color='#333333')


def _plot_mbps(ax, runs):
    labels = [_xtick(r['params']) for r in runs]
    x = np.arange(len(runs))
    width = 0.35

    for i, op_name in enumerate(('GET', 'PUT')):
        vals, counts = [], []
        for r in runs:
            op = r['ops'].get(op_name)
            vals.append(op.get('avg_throughput_bytes_per_s', 0) / 1e6 if op else 0)
            counts.append(op['total_requests'] if op else 0)
        bars = ax.bar(x + (i - 0.5) * width, vals, width,
                      label=op_name, color=_OP_COLORS[op_name])
        _annotate_counts(ax, bars, counts)

    ax.set_title('Throughput')
    ax.set_ylabel('avg MB/s')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    ax.set_ylim(bottom=0, top=ax.get_ylim()[1] * 1.4)


def _plot_ops(ax, runs):
    labels = [_xtick(r['params']) for r in runs]
    x = np.arange(len(runs))
    op_names = ('GET', 'PUT', 'DELETE', 'STAT')
    width = 0.8 / len(op_names)

    for i, op_name in enumerate(op_names):
        vals, counts = [], []
        for r in runs:
            op = r['ops'].get(op_name)
            vals.append(op['avg_throughput_ops_per_s'] if op else 0)
            counts.append(op['total_requests'] if op else 0)
        offset = (i - (len(op_names) - 1) / 2) * width
        bars = ax.bar(x + offset, vals, width, label=op_name, color=_OP_COLORS[op_name])
        _annotate_counts(ax, bars, counts)

    ax.set_title('Request Rate')
    ax.set_ylabel('avg ops/s')
    ax.set_yscale('log')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.legend()
    ax.grid(axis='y', alpha=0.3, which='both')
    ax.set_ylim(top=ax.get_ylim()[1] * 3)


def _plot_latency(ax, runs):
    labels = [_xtick(r['params']) for r in runs]
    x = np.arange(len(runs))
    op_names = ('GET', 'PUT', 'DELETE', 'STAT')
    width = 0.8 / len(op_names)

    for i, op_name in enumerate(op_names):
        p50s, err_lo, err_hi = [], [], []
        for r in runs:
            op = r['ops'].get(op_name)
            reqs = op.get('reqs') if op else None
            p50 = reqs['p50_ms'] if reqs else 0
            std = reqs['stddev_ms'] if reqs else 0
            p50s.append(p50)
            err_lo.append(min(std, p50 * 0.9))
            err_hi.append(std)
        counts = [r['ops'][op_name]['total_requests'] if op_name in r['ops'] else 0
                  for r in runs]
        offset = (i - (len(op_names) - 1) / 2) * width
        bars = ax.bar(x + offset, p50s, width, yerr=[err_lo, err_hi], capsize=2,
                      color=_OP_COLORS[op_name], alpha=0.85, label=op_name)
        y_tops = [p + e for p, e in zip(p50s, err_hi)]
        _annotate_counts(ax, bars, counts, y_tops=y_tops)

    ax.set_title('Request Latency')
    ax.set_ylabel('ms, p50 ± 1σ')
    ax.set_yscale('log')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.legend()
    ax.grid(axis='y', alpha=0.3, which='both')
    ax.set_ylim(top=ax.get_ylim()[1] * 5)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'{v:g}'))


def _write_tsv(runs, path):
    op_names = ('GET', 'PUT', 'DELETE', 'STAT')
    with open(path, 'w', newline='') as f:
        w = csv.writer(f, delimiter='\t')
        w.writerow(['obj_size', 'concurrency', 'duration', 'operation',
                    'avg_mb_s', 'avg_ops_s', 'p50_ms', 'stddev_ms', 'n'])
        for r in runs:
            p = r['params']
            size = p.get('obj_size_str', '?')
            conc = p.get('concurrency', '?')
            dur = _fmt_duration(p.get('duration', ''))
            for op_name in op_names:
                op = r['ops'].get(op_name)
                if not op:
                    continue
                avg_mb_s = round(op.get('avg_throughput_bytes_per_s', 0) / 1e6, 3)
                avg_ops_s = op['avg_throughput_ops_per_s']
                reqs = op.get('reqs') or {}
                p50 = reqs.get('p50_ms', '')
                std = reqs.get('stddev_ms', '')
                w.writerow([size, conc, dur, op_name, avg_mb_s, avg_ops_s, p50, std,
                            op['total_requests']])
    print(f'Saved {path}')


def main(json_files, prefix):
    runs = _load_runs(json_files)

    fig, axes = plt.subplots(3, 1, figsize=(10, 15))
    fig.suptitle('MinIO Production Benchmark  |  warp mixed', fontsize=13)
    fig.text(0.5, 0.955, 'c = concurrency (parallel connections)   d = test duration',
             ha='center', fontsize=9, color='#555555')
    fig.text(0.5, 0.945, 'n = number of requests (shown only where n < 100)',
             ha='center', fontsize=9, color='#555555')

    _plot_mbps(axes[0], runs)
    _plot_ops(axes[1], runs)
    _plot_latency(axes[2], runs)

    plt.tight_layout(rect=[0, 0, 1, 0.935])
    plt.savefig(f'{prefix}.png', dpi=150, bbox_inches='tight')
    print(f'Saved {prefix}.png')

    _write_tsv(runs, f'{prefix}.tsv')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Visualize warp mixed benchmark results.')
    parser.add_argument('json_files', nargs='+', metavar='FILE')
    parser.add_argument('-o', '--output', default='warp_benchmark', metavar='PREFIX')
    args = parser.parse_args()
    files = [f for pattern in args.json_files for f in sorted(glob.glob(pattern)) or [pattern]]
    main(files, args.output)
