#!/usr/bin/env python3
"""
Pool Trace Analyzer for RAPIDS Multi-File Reader Thread Pool

This script parses the [POOL_TRACE] logs and generates:
1. Thread pool utilization timeline
2. Per-task execution statistics
3. Swimlane visualization (HTML output)

Usage:
    python pool_trace_analyzer.py <log_file> [--output <output_dir>]

Log format expected:
    [POOL_TRACE] {"event":"SUBMIT","ts":1703123456789,"sparkTaskId":123,"runnerId":456,
                  "file":"path/to/file.parquet","offset":0,"length":1234567}
    [POOL_TRACE] {"event":"START","ts":1703123456800,"sparkTaskId":123,"runnerId":456,
                  "activeThreads":3,"poolSize":10,"schedTimeMs":11}
    [POOL_TRACE] {"event":"END","ts":1703123457000,"sparkTaskId":123,"runnerId":456,
                  "execTimeMs":200}
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class RunnerEvent:
    event: str
    ts: int
    spark_task_id: int
    runner_id: int
    file: Optional[str] = None
    offset: Optional[int] = None
    length: Optional[int] = None
    active_threads: Optional[int] = None
    pool_size: Optional[int] = None
    sched_time_ms: Optional[int] = None
    exec_time_ms: Optional[int] = None
    thread_id: Optional[int] = None
    thread_name: Optional[str] = None


@dataclass
class RunnerLifecycle:
    runner_id: int
    spark_task_id: int
    file: str
    offset: int
    length: int
    submit_ts: int
    start_ts: Optional[int] = None
    end_ts: Optional[int] = None
    sched_time_ms: Optional[int] = None
    exec_time_ms: Optional[int] = None
    active_threads_at_start: Optional[int] = None
    pool_size: Optional[int] = None
    thread_id: Optional[int] = None
    thread_name: Optional[str] = None


def parse_log_line(line: str) -> Optional[RunnerEvent]:
    """Parse a single log line containing [POOL_TRACE]."""
    match = re.search(r'\[POOL_TRACE\]\s*(\{.*\})', line)
    if not match:
        return None

    try:
        data = json.loads(match.group(1))
        return RunnerEvent(
            event=data.get('event'),
            ts=data.get('ts'),
            spark_task_id=data.get('sparkTaskId'),
            runner_id=data.get('runnerId'),
            file=data.get('file'),
            offset=data.get('offset'),
            length=data.get('length'),
            active_threads=data.get('activeThreads'),
            pool_size=data.get('poolSize'),
            sched_time_ms=data.get('schedTimeMs'),
            exec_time_ms=data.get('execTimeMs'),
            thread_id=data.get('threadId'),
            thread_name=data.get('threadName'),
        )
    except json.JSONDecodeError as e:
        print(f"Warning: Failed to parse JSON: {e}", file=sys.stderr)
        return None


def parse_log_file(log_file: str) -> List[RunnerEvent]:
    """Parse all [POOL_TRACE] events from a log file."""
    events = []
    with open(log_file, 'r') as f:
        for line in f:
            event = parse_log_line(line)
            if event:
                events.append(event)
    return events


def build_runner_lifecycles(events: List[RunnerEvent]) -> Dict[int, RunnerLifecycle]:
    """Build complete lifecycle for each runner from events."""
    lifecycles: Dict[int, RunnerLifecycle] = {}

    for event in events:
        runner_id = event.runner_id

        if event.event == 'SUBMIT':
            lifecycles[runner_id] = RunnerLifecycle(
                runner_id=runner_id,
                spark_task_id=event.spark_task_id,
                file=event.file or '',
                offset=event.offset or 0,
                length=event.length or 0,
                submit_ts=event.ts,
            )
        elif event.event == 'START':
            if runner_id in lifecycles:
                lc = lifecycles[runner_id]
                lc.start_ts = event.ts
                lc.sched_time_ms = event.sched_time_ms
                lc.active_threads_at_start = event.active_threads
                lc.pool_size = event.pool_size
                lc.thread_id = event.thread_id
                lc.thread_name = event.thread_name
                # Also update file info from START event (new format)
                if event.file:
                    lc.file = event.file
                if event.offset is not None:
                    lc.offset = event.offset
                if event.length is not None:
                    lc.length = event.length
        elif event.event == 'END':
            if runner_id in lifecycles:
                lc = lifecycles[runner_id]
                lc.end_ts = event.ts
                lc.exec_time_ms = event.exec_time_ms

    return lifecycles


def compute_pool_utilization(events: List[RunnerEvent]) -> List[Tuple[int, int, int]]:
    """
    Compute thread pool utilization over time.
    Returns list of (timestamp, active_threads, pool_size).
    """
    utilization = []
    for event in events:
        if event.event == 'START' and event.active_threads is not None:
            utilization.append((event.ts, event.active_threads, event.pool_size or 0))
    return sorted(utilization, key=lambda x: x[0])


def generate_statistics(lifecycles: Dict[int, RunnerLifecycle]) -> str:
    """Generate statistics report."""
    lines = []
    lines.append("=" * 80)
    lines.append("POOL TRACE STATISTICS")
    lines.append("=" * 80)
    lines.append("")

    # Group by spark task
    by_spark_task: Dict[int, List[RunnerLifecycle]] = defaultdict(list)
    for lc in lifecycles.values():
        by_spark_task[lc.spark_task_id].append(lc)

    lines.append(f"Total runners: {len(lifecycles)}")
    lines.append(f"Unique Spark tasks: {len(by_spark_task)}")
    lines.append("")

    # Overall timing stats
    exec_times = [lc.exec_time_ms for lc in lifecycles.values() if lc.exec_time_ms]
    sched_times = [lc.sched_time_ms for lc in lifecycles.values() if lc.sched_time_ms]

    if exec_times:
        lines.append("Execution time (ms):")
        lines.append(f"  Min: {min(exec_times)}, Max: {max(exec_times)}, "
                     f"Avg: {sum(exec_times)/len(exec_times):.2f}")
    if sched_times:
        lines.append("Schedule wait time (ms):")
        lines.append(f"  Min: {min(sched_times)}, Max: {max(sched_times)}, "
                     f"Avg: {sum(sched_times)/len(sched_times):.2f}")
    lines.append("")

    # Per Spark task breakdown
    lines.append("-" * 80)
    lines.append("PER SPARK TASK BREAKDOWN")
    lines.append("-" * 80)

    for task_id in sorted(by_spark_task.keys()):
        runners = by_spark_task[task_id]
        lines.append(f"\nSpark Task {task_id}: {len(runners)} runners")

        for lc in sorted(runners, key=lambda x: x.submit_ts):
            file_short = os.path.basename(lc.file) if lc.file else 'N/A'
            lines.append(f"  Runner {lc.runner_id}: {file_short} "
                         f"(offset={lc.offset}, len={lc.length}) "
                         f"sched={lc.sched_time_ms}ms exec={lc.exec_time_ms}ms")

    return "\n".join(lines)


def generate_swimlane_svg(lifecycles: Dict[int, RunnerLifecycle]) -> str:
    """Generate an SVG swimlane visualization with thread-based lanes."""
    if not lifecycles:
        return '<svg xmlns="http://www.w3.org/2000/svg"><text>No data</text></svg>'

    # Find time range
    all_ts = []
    for lc in lifecycles.values():
        all_ts.append(lc.submit_ts)
        if lc.start_ts:
            all_ts.append(lc.start_ts)
        if lc.end_ts:
            all_ts.append(lc.end_ts)

    min_ts = min(all_ts)
    max_ts = max(all_ts)
    time_range = max_ts - min_ts if max_ts > min_ts else 1

    # Group by thread_id for thread-based swimlane view
    by_thread: Dict[int, List[RunnerLifecycle]] = defaultdict(list)
    for lc in lifecycles.values():
        if lc.thread_id is not None:
            by_thread[lc.thread_id].append(lc)
        else:
            by_thread[-1].append(lc)  # Unknown thread

    # Also group by spark task for coloring
    spark_task_ids = sorted(set(lc.spark_task_id for lc in lifecycles.values()))
    task_to_color_idx = {tid: i for i, tid in enumerate(spark_task_ids)}

    # Colors for spark tasks
    colors = [
        '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7',
        '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9',
        '#F8B500', '#00CED1', '#FF69B4', '#32CD32', '#FFD700',
    ]

    # SVG dimensions
    margin_left = 180
    margin_top = 80
    margin_right = 20
    lane_height = 40
    bar_height = 30
    track_width = 1000

    num_threads = len(by_thread)
    legend_height = ((len(spark_task_ids) // 8) + 2) * 20
    svg_height = margin_top + num_threads * lane_height + legend_height + 50
    svg_width = margin_left + track_width + margin_right

    svg_parts = []
    svg_parts.append(f'''<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="{svg_width}" height="{svg_height}"
     viewBox="0 0 {svg_width} {svg_height}">
  <defs>
    <style>
      .title {{ font: bold 18px Arial; fill: #333; }}
      .subtitle {{ font: 12px Arial; fill: #666; }}
      .lane-label {{ font: 11px Arial; fill: #333; }}
      .timeline-label {{ font: 9px Arial; fill: #666; }}
      .legend-text {{ font: 10px Arial; fill: #333; }}
      .bar-text {{ font: 9px Arial; fill: #000; }}
    </style>
  </defs>
  <rect width="100%" height="100%" fill="#f8f9fa"/>

  <!-- Title -->
  <text x="{svg_width//2}" y="25" text-anchor="middle" class="title">
    Thread Pool Swimlane View
  </text>
  <text x="{svg_width//2}" y="45" text-anchor="middle" class="subtitle">
    {len(lifecycles)} runners across {num_threads} threads, {len(spark_task_ids)} Spark tasks
  </text>
''')

    # Timeline header
    for i in range(11):
        pct = i * 10
        x = margin_left + (track_width * pct // 100)
        ts_label = (time_range * pct // 100)
        svg_parts.append(f'  <text x="{x}" y="{margin_top - 10}" '
                        f'class="timeline-label" text-anchor="middle">{ts_label}ms</text>')
        svg_parts.append(f'  <line x1="{x}" y1="{margin_top - 5}" x2="{x}" '
                        f'y2="{margin_top + num_threads * lane_height}" '
                        f'stroke="#ddd" stroke-width="1" stroke-dasharray="3,3"/>')

    # Swimlanes by thread
    row = 0
    for thread_id in sorted(by_thread.keys()):
        runners = sorted(by_thread[thread_id], key=lambda x: x.start_ts or x.submit_ts)
        y = margin_top + row * lane_height

        # Lane background
        svg_parts.append(f'  <rect x="0" y="{y}" width="{svg_width}" height="{lane_height}" '
                        f'fill="{"#fff" if row % 2 == 0 else "#f0f0f0"}"/>')

        # Lane label - show thread name if available
        thread_label = runners[0].thread_name if runners and runners[0].thread_name else f"Thread-{thread_id}"
        # Truncate long thread names
        if len(thread_label) > 22:
            thread_label = thread_label[:20] + ".."
        svg_parts.append(f'  <text x="{margin_left - 5}" y="{y + lane_height//2 + 4}" '
                        f'text-anchor="end" class="lane-label">{thread_label}</text>')

        # Draw execution bars for each runner on this thread
        for lc in runners:
            if lc.start_ts and lc.end_ts:
                color = colors[task_to_color_idx.get(lc.spark_task_id, 0) % len(colors)]
                exec_start_x = margin_left + ((lc.start_ts - min_ts) / time_range) * track_width
                exec_width = max(((lc.end_ts - lc.start_ts) / time_range) * track_width, 4)

                # Build comprehensive tooltip with all info
                file_name = os.path.basename(lc.file) if lc.file else 'N/A'
                tooltip = (f"Runner: R{lc.runner_id}\\n"
                          f"Spark Task: {lc.spark_task_id}\\n"
                          f"Thread: {lc.thread_name} (ID:{lc.thread_id})\\n"
                          f"File: {file_name}\\n"
                          f"Full Path: {lc.file}\\n"
                          f"Offset: {lc.offset:,} bytes\\n"
                          f"Length: {lc.length:,} bytes ({lc.length/1024/1024:.1f} MB)\\n"
                          f"Schedule Wait: {lc.sched_time_ms}ms\\n"
                          f"Execution Time: {lc.exec_time_ms}ms\\n"
                          f"Active Threads: {lc.active_threads_at_start}/{lc.pool_size}")

                svg_parts.append(f'  <rect x="{exec_start_x:.1f}" y="{y + 5}" '
                                f'width="{exec_width:.1f}" height="{bar_height}" '
                                f'fill="{color}" rx="3" stroke="#333" stroke-width="0.5">')
                svg_parts.append(f'    <title>{tooltip}</title>')
                svg_parts.append(f'  </rect>')

                # Text label if bar is wide enough
                if exec_width > 30:
                    label = f"R{lc.runner_id}"
                    svg_parts.append(f'  <text x="{exec_start_x + exec_width/2:.1f}" '
                                    f'y="{y + lane_height//2 + 3}" '
                                    f'text-anchor="middle" class="bar-text">{label}</text>')

        row += 1

    # Legend for Spark Tasks
    legend_y = margin_top + num_threads * lane_height + 20
    svg_parts.append(f'  <text x="{margin_left}" y="{legend_y}" class="lane-label">'
                    f'Spark Tasks:</text>')
    legend_y += 15
    legend_x = margin_left
    for task_id in spark_task_ids:
        color = colors[task_to_color_idx[task_id] % len(colors)]
        svg_parts.append(f'  <rect x="{legend_x}" y="{legend_y}" width="14" height="14" '
                        f'fill="{color}" rx="2" stroke="#333" stroke-width="0.5"/>')
        svg_parts.append(f'  <text x="{legend_x + 18}" y="{legend_y + 11}" '
                        f'class="legend-text">T{task_id}</text>')
        legend_x += 70
        if legend_x > svg_width - 100:
            legend_x = margin_left
            legend_y += 20

    svg_parts.append('</svg>')
    return '\n'.join(svg_parts)


def generate_color_for_task(task_idx: int, total_tasks: int) -> str:
    """Generate a distinct color for each task using HSL color space."""
    # Use golden ratio to spread hues evenly
    golden_ratio = 0.618033988749895
    hue = (task_idx * golden_ratio) % 1.0
    # Keep saturation and lightness in good range for visibility
    saturation = 0.65 + (task_idx % 3) * 0.1  # 65-85%
    lightness = 0.55 + (task_idx % 2) * 0.1   # 55-65%

    # Convert HSL to RGB
    def hsl_to_rgb(h, s, l):
        if s == 0:
            r = g = b = l
        else:
            def hue_to_rgb(p, q, t):
                if t < 0: t += 1
                if t > 1: t -= 1
                if t < 1/6: return p + (q - p) * 6 * t
                if t < 1/2: return q
                if t < 2/3: return p + (q - p) * (2/3 - t) * 6
                return p
            q = l * (1 + s) if l < 0.5 else l + s - l * s
            p = 2 * l - q
            r = hue_to_rgb(p, q, h + 1/3)
            g = hue_to_rgb(p, q, h)
            b = hue_to_rgb(p, q, h - 1/3)
        return int(r * 255), int(g * 255), int(b * 255)

    r, g, b = hsl_to_rgb(hue, saturation, lightness)
    return f'#{r:02x}{g:02x}{b:02x}'


def generate_swimlane_html(lifecycles: Dict[int, RunnerLifecycle],
                           utilization: List[Tuple[int, int, int]]) -> str:
    """Generate an HTML swimlane visualization with thread-based lanes."""
    if not lifecycles:
        return "<html><body><h1>No data to display</h1></body></html>"

    # Find time range
    all_ts = []
    for lc in lifecycles.values():
        all_ts.append(lc.submit_ts)
        if lc.start_ts:
            all_ts.append(lc.start_ts)
        if lc.end_ts:
            all_ts.append(lc.end_ts)

    min_ts = min(all_ts)
    max_ts = max(all_ts)
    time_range = max_ts - min_ts if max_ts > min_ts else 1

    # Group by thread_id for thread-based swimlane view
    by_thread: Dict[int, List[RunnerLifecycle]] = defaultdict(list)
    for lc in lifecycles.values():
        if lc.thread_id is not None:
            by_thread[lc.thread_id].append(lc)
        else:
            by_thread[-1].append(lc)

    # Get unique spark tasks and generate colors dynamically
    spark_task_ids = sorted(set(lc.spark_task_id for lc in lifecycles.values()))
    task_colors = {tid: generate_color_for_task(i, len(spark_task_ids))
                   for i, tid in enumerate(spark_task_ids)}

    num_threads = len(by_thread)

    html_parts = []
    html_parts.append('''<!DOCTYPE html>
<html>
<head>
    <title>Thread Pool Swimlane Visualization</title>
    <style>
        * { box-sizing: border-box; }
        body { font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 20px;
               background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); color: #eee;
               min-height: 100vh; }
        h1 { color: #4ECDC4; margin-bottom: 5px; }
        h2 { color: #45B7D1; margin-top: 30px; }
        .subtitle { color: #888; margin-bottom: 20px; }
        .container { max-width: 100%; overflow: hidden; background: rgba(0,0,0,0.2);
                     border-radius: 8px; padding: 15px; cursor: default;
                     user-select: none; }
        .swimlane { position: relative; margin: 3px 0; height: 45px;
                    background: rgba(255,255,255,0.03); border-radius: 4px; }
        .swimlane:nth-child(odd) { background: rgba(255,255,255,0.06); }
        .lane-label { position: absolute; left: 0; width: 220px; text-align: right;
                      padding-right: 15px; font-size: 11px; line-height: 45px;
                      color: #aaa; overflow: hidden; text-overflow: ellipsis;
                      white-space: nowrap; }
        .lane-track { position: absolute; left: 230px; right: 10px; height: 100%;
                      border-left: 1px solid #333; overflow: visible; }
        .task-bar { position: absolute; height: 35px; top: 5px; border-radius: 4px;
                    font-size: 10px; color: #000; overflow: hidden; white-space: nowrap;
                    text-overflow: ellipsis; padding: 2px 6px; cursor: pointer;
                    border: 1px solid rgba(0,0,0,0.3); font-weight: bold;
                    display: flex; align-items: center; justify-content: center;
                    transition: transform 0.1s, box-shadow 0.1s; }
        .task-bar:hover { transform: scale(1.05); box-shadow: 0 4px 12px rgba(0,0,0,0.4);
                          z-index: 100; }
        .legend { margin: 20px 0; padding: 15px; background: rgba(0,0,0,0.3);
                  border-radius: 8px; display: flex; flex-wrap: wrap; gap: 8px; }
        .legend-title { width: 100%; font-weight: bold; margin-bottom: 5px; color: #4ECDC4; }
        .legend-item { display: inline-flex; align-items: center; padding: 4px 8px;
                       background: rgba(255,255,255,0.05); border-radius: 4px; font-size: 11px; }
        .legend-color { width: 14px; height: 14px; border-radius: 3px; margin-right: 6px;
                        border: 1px solid rgba(0,0,0,0.3); }
        .timeline { position: relative; height: 25px; margin: 5px 0; margin-left: 230px; }
        .timeline-label { position: absolute; font-size: 10px; color: #666;
                          transform: translateX(-50%); }
        .timeline-line { position: absolute; top: 20px; bottom: 0; width: 1px;
                         background: rgba(255,255,255,0.1); }
        table { border-collapse: collapse; width: 100%; margin: 20px 0;
                background: rgba(0,0,0,0.2); border-radius: 8px; overflow: hidden; }
        th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid #333; }
        th { background: rgba(0,0,0,0.3); color: #4ECDC4; font-weight: 600; }
        tr:hover { background: rgba(255,255,255,0.05); }
        .tooltip-box { position: fixed; background: #2a2a3e; color: #fff; padding: 12px 15px;
                       border-radius: 8px; font-size: 12px; z-index: 10000; display: none;
                       max-width: 450px; box-shadow: 0 8px 25px rgba(0,0,0,0.5);
                       border: 1px solid #444; line-height: 1.6; }
        .tooltip-box .label { color: #888; }
        .tooltip-box .value { color: #4ECDC4; font-weight: 500; }
        .tooltip-box hr { border: none; border-top: 1px solid #444; margin: 8px 0; }

        .zoom-controls { display: flex; align-items: center; gap: 12px; margin: 15px 0;
                         padding: 12px 15px; background: rgba(0,0,0,0.3); border-radius: 8px;
                         flex-wrap: wrap; }
        .zoom-controls button { padding: 8px 16px; background: #4ECDC4; color: #000;
                                border: none; border-radius: 4px; cursor: pointer;
                                font-weight: bold; font-size: 14px;
                                transition: background 0.2s, transform 0.1s; }
        .zoom-controls button:hover { background: #45B7D1; transform: scale(1.05); }
        .zoom-controls button:active { transform: scale(0.95); }
        .zoom-info { color: #aaa; font-size: 12px; }
        .zoom-hint { color: #666; font-size: 11px; margin-left: auto; }
        #view-range { color: #4ECDC4; font-weight: 500; margin-left: 5px; }
    </style>
</head>
<body>
    <h1>Thread Pool Swimlane View</h1>
    <div class="subtitle">''' + f'{len(lifecycles)} runners across {num_threads} threads, '
                               f'{len(spark_task_ids)} Spark tasks</div>' + '''

    <div id="tooltip" class="tooltip-box"></div>

    <div class="zoom-controls">
        <button id="zoom-in">Zoom In (+)</button>
        <button id="zoom-out">Zoom Out (-)</button>
        <button id="zoom-reset">Reset</button>
        <span class="zoom-info">View: <span id="view-range">0ms - ''' + str(time_range) + '''ms (Zoom: 1.0x)</span></span>
        <span class="zoom-hint">Ctrl + Scroll to zoom, Drag to pan</span>
    </div>
''')

    # Legend for Spark Tasks
    html_parts.append('<div class="legend">')
    html_parts.append('<div class="legend-title">Spark Tasks</div>')
    for task_id in spark_task_ids:
        color = task_colors[task_id]
        html_parts.append(f'<span class="legend-item">'
                         f'<span class="legend-color" style="background:{color}"></span>'
                         f'T{task_id}</span>')
    html_parts.append('</div>')

    # Timeline header
    html_parts.append('<div class="container">')
    html_parts.append('<div class="timeline">')
    for i in range(11):
        pct = i * 10
        ts_label = time_range * pct // 100
        html_parts.append(f'<span class="timeline-label" style="left:{pct}%">{ts_label}ms</span>')
        html_parts.append(f'<div class="timeline-line" style="left:{pct}%"></div>')
    html_parts.append('</div>')

    # Swimlanes by thread
    for thread_id in sorted(by_thread.keys()):
        runners = sorted(by_thread[thread_id], key=lambda x: x.start_ts or x.submit_ts)
        thread_label = runners[0].thread_name if runners and runners[0].thread_name else f"Thread-{thread_id}"

        html_parts.append(f'<div class="swimlane">')
        html_parts.append(f'<div class="lane-label" title="{thread_label}">{thread_label}</div>')
        html_parts.append(f'<div class="lane-track">')

        for lc in runners:
            if lc.start_ts and lc.end_ts:
                color = task_colors.get(lc.spark_task_id, '#888')
                exec_start = ((lc.start_ts - min_ts) / time_range) * 100
                exec_width = max(((lc.end_ts - lc.start_ts) / time_range) * 100, 0.8)

                file_name = os.path.basename(lc.file) if lc.file else 'N/A'
                length_mb = lc.length / 1024 / 1024 if lc.length else 0

                # Data attributes for tooltip
                data_attrs = (f'data-runner="{lc.runner_id}" '
                             f'data-task="{lc.spark_task_id}" '
                             f'data-thread="{lc.thread_name}" '
                             f'data-threadid="{lc.thread_id}" '
                             f'data-file="{file_name}" '
                             f'data-path="{lc.file}" '
                             f'data-offset="{lc.offset:,}" '
                             f'data-length="{lc.length:,}" '
                             f'data-lengthmb="{length_mb:.1f}" '
                             f'data-sched="{lc.sched_time_ms}" '
                             f'data-exec="{lc.exec_time_ms}" '
                             f'data-active="{lc.active_threads_at_start}" '
                             f'data-pool="{lc.pool_size}"')

                html_parts.append(
                    f'<div class="task-bar" {data_attrs} '
                    f'style="left:{exec_start:.2f}%;width:{exec_width:.2f}%;background:{color}">'
                    f'R{lc.runner_id}</div>')

        html_parts.append('</div></div>')

    html_parts.append('</div>')

    # Statistics table
    html_parts.append('<h2>Runner Details</h2>')
    html_parts.append('<table>')
    html_parts.append('<tr><th>Runner</th><th>Spark Task</th><th>Thread</th><th>File</th>'
                     '<th>Offset</th><th>Length</th><th>Sched(ms)</th><th>Exec(ms)</th>'
                     '<th>Active/Pool</th></tr>')

    for lc in sorted(lifecycles.values(), key=lambda x: x.start_ts or x.submit_ts):
        file_short = os.path.basename(lc.file) if lc.file else 'N/A'
        thread_short = (lc.thread_name[:20] + '..') if lc.thread_name and len(lc.thread_name) > 22 else (lc.thread_name or 'N/A')
        length_mb = f"{lc.length/1024/1024:.1f}MB" if lc.length else 'N/A'
        html_parts.append(
            f'<tr><td>R{lc.runner_id}</td><td>T{lc.spark_task_id}</td>'
            f'<td title="{lc.thread_name}">{thread_short}</td>'
            f'<td title="{lc.file}">{file_short}</td>'
            f'<td>{lc.offset:,}</td><td>{length_mb}</td>'
            f'<td>{lc.sched_time_ms or "N/A"}</td><td>{lc.exec_time_ms or "N/A"}</td>'
            f'<td>{lc.active_threads_at_start or "?"}/{lc.pool_size or "?"}</td></tr>')

    html_parts.append('</table>')

    # JavaScript for tooltip and zoom
    html_parts.append(f'''
<script>
const TOTAL_TIME_RANGE = {time_range};

document.addEventListener('DOMContentLoaded', function() {{
    const tooltip = document.getElementById('tooltip');
    const container = document.querySelector('.container');
    const swimlanes = document.querySelectorAll('.swimlane');
    const bars = document.querySelectorAll('.task-bar');
    const timeline = document.querySelector('.timeline');

    // Zoom state
    let scale = 1;
    let offsetX = 0;  // in percentage (0-100)
    const minScale = 1;
    const maxScale = 50;

    // Update the view range display
    function updateViewRange() {{
        const viewStart = offsetX / scale;
        const viewEnd = (offsetX + 100) / scale;
        const startMs = Math.round(TOTAL_TIME_RANGE * viewStart / 100);
        const endMs = Math.round(TOTAL_TIME_RANGE * viewEnd / 100);
        document.getElementById('view-range').textContent =
            `${{startMs}}ms - ${{endMs}}ms (Zoom: ${{scale.toFixed(1)}}x)`;
    }}

    // Apply transform to all lane tracks
    function applyTransform() {{
        document.querySelectorAll('.lane-track').forEach(track => {{
            track.style.transform = `scaleX(${{scale}}) translateX(${{-offsetX / scale}}%)`;
            track.style.transformOrigin = 'left center';
        }});

        // Update timeline labels
        timeline.innerHTML = '';
        const viewStart = offsetX / scale;
        const viewWidth = 100 / scale;
        for (let i = 0; i <= 10; i++) {{
            const pct = i * 10;
            const actualPct = viewStart + (viewWidth * pct / 100);
            const tsLabel = Math.round(TOTAL_TIME_RANGE * actualPct / 100);
            const labelEl = document.createElement('span');
            labelEl.className = 'timeline-label';
            labelEl.style.left = pct + '%';
            labelEl.textContent = tsLabel + 'ms';
            timeline.appendChild(labelEl);

            const lineEl = document.createElement('div');
            lineEl.className = 'timeline-line';
            lineEl.style.left = pct + '%';
            timeline.appendChild(lineEl);
        }}

        updateViewRange();
    }}

    // Mouse wheel zoom
    container.addEventListener('wheel', function(e) {{
        if (e.ctrlKey || e.metaKey) {{
            e.preventDefault();
            const rect = container.getBoundingClientRect();
            const mouseX = (e.clientX - rect.left) / rect.width;  // 0 to 1

            const oldScale = scale;
            const zoomFactor = e.deltaY > 0 ? 0.9 : 1.1;
            scale = Math.min(maxScale, Math.max(minScale, scale * zoomFactor));

            // Adjust offset to zoom towards mouse position
            const mousePos = offsetX + mouseX * 100;
            offsetX = mousePos - (mousePos - offsetX) * (scale / oldScale);
            offsetX = Math.max(0, Math.min(offsetX, 100 * scale - 100));

            applyTransform();
        }}
    }});

    // Drag to pan
    let isDragging = false;
    let startX = 0;
    let startOffset = 0;

    container.addEventListener('mousedown', function(e) {{
        if (e.button === 0 && scale > 1) {{
            isDragging = true;
            startX = e.clientX;
            startOffset = offsetX;
            container.style.cursor = 'grabbing';
        }}
    }});

    document.addEventListener('mousemove', function(e) {{
        if (isDragging) {{
            const rect = container.getBoundingClientRect();
            const dx = (startX - e.clientX) / rect.width * 100;
            offsetX = Math.max(0, Math.min(startOffset + dx * scale, 100 * scale - 100));
            applyTransform();
        }}
    }});

    document.addEventListener('mouseup', function() {{
        if (isDragging) {{
            isDragging = false;
            container.style.cursor = scale > 1 ? 'grab' : 'default';
        }}
    }});

    // Zoom control buttons
    document.getElementById('zoom-in').addEventListener('click', function() {{
        scale = Math.min(maxScale, scale * 1.5);
        offsetX = Math.min(offsetX, 100 * scale - 100);
        applyTransform();
    }});

    document.getElementById('zoom-out').addEventListener('click', function() {{
        scale = Math.max(minScale, scale / 1.5);
        offsetX = Math.max(0, Math.min(offsetX, 100 * scale - 100));
        applyTransform();
    }});

    document.getElementById('zoom-reset').addEventListener('click', function() {{
        scale = 1;
        offsetX = 0;
        applyTransform();
    }});

    // Tooltip handling
    bars.forEach(bar => {{
        bar.addEventListener('mouseenter', function(e) {{
            const r = this.dataset;
            tooltip.innerHTML = `
                <div><span class="label">Runner:</span> <span class="value">R${{r.runner}}</span></div>
                <div><span class="label">Spark Task:</span> <span class="value">T${{r.task}}</span></div>
                <hr>
                <div><span class="label">Thread:</span> <span class="value">${{r.thread}}</span></div>
                <div><span class="label">Thread ID:</span> <span class="value">${{r.threadid}}</span></div>
                <hr>
                <div><span class="label">File:</span> <span class="value">${{r.file}}</span></div>
                <div><span class="label">Path:</span> <span class="value" style="word-break:break-all;font-size:10px">${{r.path}}</span></div>
                <div><span class="label">Offset:</span> <span class="value">${{r.offset}} bytes</span></div>
                <div><span class="label">Length:</span> <span class="value">${{r.length}} bytes (${{r.lengthmb}} MB)</span></div>
                <hr>
                <div><span class="label">Schedule Wait:</span> <span class="value">${{r.sched}} ms</span></div>
                <div><span class="label">Execution Time:</span> <span class="value">${{r.exec}} ms</span></div>
                <div><span class="label">Active Threads:</span> <span class="value">${{r.active}} / ${{r.pool}}</span></div>
            `;
            tooltip.style.display = 'block';
        }});

        bar.addEventListener('mousemove', function(e) {{
            tooltip.style.left = (e.pageX + 15) + 'px';
            tooltip.style.top = (e.pageY + 15) + 'px';
        }});

        bar.addEventListener('mouseleave', function() {{
            tooltip.style.display = 'none';
        }});
    }});

    // Initial state
    updateViewRange();
}});
</script>
''')

    # Pool utilization over time
    if utilization:
        html_parts.append('<h2>Pool Utilization Timeline</h2>')
        html_parts.append('<div class="stats"><pre>')
        html_parts.append('Timestamp(ms)  Active  PoolSize\n')
        html_parts.append('-' * 40 + '\n')
        for ts, active, pool_size in utilization[:50]:  # Limit to first 50
            html_parts.append(f'{ts - min_ts:>12}  {active:>6}  {pool_size:>8}\n')
        if len(utilization) > 50:
            html_parts.append(f'... and {len(utilization) - 50} more entries\n')
        html_parts.append('</pre></div>')

    html_parts.append('</body></html>')

    return ''.join(html_parts)


def main():
    parser = argparse.ArgumentParser(
        description='Analyze RAPIDS Multi-File Reader thread pool trace logs')
    parser.add_argument('log_file', help='Path to the log file containing [POOL_TRACE] entries')
    parser.add_argument('--output', '-o', default='.',
                        help='Output directory for generated files (default: current directory)')

    args = parser.parse_args()

    if not os.path.exists(args.log_file):
        print(f"Error: Log file not found: {args.log_file}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.output, exist_ok=True)

    print(f"Parsing log file: {args.log_file}")
    events = parse_log_file(args.log_file)
    print(f"Found {len(events)} [POOL_TRACE] events")

    if not events:
        print("No [POOL_TRACE] events found in the log file.")
        sys.exit(0)

    lifecycles = build_runner_lifecycles(events)
    print(f"Built {len(lifecycles)} runner lifecycles")

    utilization = compute_pool_utilization(events)

    # Generate statistics report
    stats = generate_statistics(lifecycles)
    stats_file = os.path.join(args.output, 'pool_trace_stats.txt')
    with open(stats_file, 'w') as f:
        f.write(stats)
    print(f"Statistics saved to: {stats_file}")
    print()
    print(stats)

    # Generate HTML swimlane
    html = generate_swimlane_html(lifecycles, utilization)
    html_file = os.path.join(args.output, 'pool_trace_swimlane.html')
    with open(html_file, 'w') as f:
        f.write(html)
    print(f"\nSwimlane HTML saved to: {html_file}")

    # Generate SVG swimlane
    svg = generate_swimlane_svg(lifecycles)
    svg_file = os.path.join(args.output, 'pool_trace_swimlane.svg')
    with open(svg_file, 'w') as f:
        f.write(svg)
    print(f"Swimlane SVG saved to: {svg_file}")


if __name__ == '__main__':
    main()

