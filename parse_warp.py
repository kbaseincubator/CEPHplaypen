#!/usr/bin/env python3

# NOTE: Written by Ai and spot checked


import json
import sys


def aggregate_latency(segments, field='dur'):
    """Aggregate latency stats across time-window segments.
    Warp uses unweighted mean across segments (equal weight per 10s window).
    field='dur' for request duration, field='first_byte' for TTFB."""
    if field == 'first_byte':
        segs = [s['first_byte'] for s in segments if 'first_byte' in s]
        if not segs:
            return None
        avg_key, p50_key, p90_key, p99_key = 'average_millis', 'median_millis', 'p90_millis', 'p99_millis'
        fast_key, slow_key, std_key = 'fastest_millis', 'slowest_millis', 'std_dev_millis'
        extra = {
            'best_ms':  min(s['fastest_millis'] for s in segs),
            'p25_ms':   _umean(segs, 'p25_millis'),
            'p75_ms':   _umean(segs, 'p75_millis'),
            'worst_ms': max(s['slowest_millis'] for s in segs),
        }
    else:
        segs = segments
        avg_key, p50_key, p90_key, p99_key = 'dur_avg_millis', 'dur_median_millis', 'dur_90_millis', 'dur_99_millis'
        fast_key, slow_key, std_key = 'fastest_millis', 'slowest_millis', 'std_dev_millis'
        extra = {}

    result = {
        'avg_ms':     round(_umean(segs, avg_key), 1),
        'p50_ms':     round(_umean(segs, p50_key), 1),
        'p90_ms':     round(_umean(segs, p90_key), 1),
        'p99_ms':     round(_umean(segs, p99_key), 1),
        'fastest_ms': round(min(s[fast_key] for s in segs), 1),
        'slowest_ms': round(max(s[slow_key] for s in segs), 1),
        'stddev_ms':  round(_umean(segs, std_key), 1),
    }
    result.update({k: round(v, 1) for k, v in extra.items()})
    return result


def _umean(segs, key):
    return sum(s[key] for s in segs) / len(segs)


def parse_op(op_name, op):
    tp = op['throughput']
    seg = tp['segmented']
    has_bytes = op['total_bytes'] > 0

    # Flatten all clients' segments for latency aggregation
    all_segs = [
        item['single_sized_requests']
        for client_segs in op['requests_by_client'].values()
        for item in client_segs
        if 'single_sized_requests' in item
        and item['single_sized_requests'].get('merged_entries', 0) > 0
    ]

    result = {
        'operation':            op_name,
        'total_requests':       op['total_requests'],
        'concurrency':          op['concurrency'],
        'start_time':           tp['start_time'],
        'end_time':             tp['end_time'],
        'measured_duration_s':  tp['measure_duration_millis'] / 1000,
        'avg_throughput_ops_per_s': round(tp['objects'] / (tp['measure_duration_millis'] / 1000), 2),
    }

    if has_bytes:
        result['avg_throughput_bytes_per_s'] = tp['bytes'] / (tp['measure_duration_millis'] / 1000)
        # obj size is consistent across segments; grab from first available
        for s in all_segs:
            if 'obj_size' in s:
                result['obj_size_bytes'] = s['obj_size']
                break

    result['throughput'] = {
        'segments':           len(seg['segments']),
        'segment_duration_s': seg['segment_duration_millis'] / 1000,
        'fastest': _tp_entry(seg, 'fastest', has_bytes),
        'median':  _tp_entry(seg, 'median',  has_bytes),
        'slowest': _tp_entry(seg, 'slowest', has_bytes),
    }

    if all_segs:
        result['reqs'] = aggregate_latency(all_segs)
        if 'first_byte' in all_segs[0]:
            ttfb = aggregate_latency(all_segs, field='first_byte')
            if ttfb:
                # Rename for TTFB conventions
                ttfb['best_ms']   = ttfb.pop('fastest_ms', ttfb.get('best_ms'))
                ttfb['worst_ms']  = ttfb.pop('slowest_ms', ttfb.get('worst_ms'))
                ttfb['median_ms'] = ttfb.pop('p50_ms')
                result['ttfb'] = ttfb

    return result


def _tp_entry(seg, key, has_bytes):
    entry = {
        'ops_per_s':  seg[f'{key}_ops'],
        'start_time': seg[f'{key}_start'],
    }
    if has_bytes:
        entry['bytes_per_s'] = seg[f'{key}_bps']
    return entry


def parse_warp_json(data):
    return [
        parse_op(op_name, op)
        for op_name, op in data['by_op_type'].items()
    ]


if __name__ == '__main__':
    path = sys.argv[1]
    with open(path) as f:
        data = json.load(f)
    print(json.dumps(parse_warp_json(data), indent=2))
