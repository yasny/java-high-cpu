#!/usr/bin/env python3
import locale
import re
import argparse
from typing import Dict, Any
from operator import itemgetter
from datetime import datetime
from pathlib import Path


def parse_threaddump(filename: str, datetime_regex: re, datetime_format: str) -> Dict[str, Any]:
    result = dict()
    current_date = None
    skip_next = False

    with open(filename, 'r') as f:
        for raw_line in f:
            line = raw_line.rstrip()

            if skip_next:
                skip_next = False
                continue

            if datetime_regex.match(line):
                current_date = datetime.strptime(line, datetime_format)
                result[current_date] = dict()
                result[current_date]['filename'] = filename
                # NOTE(iwalker): high-cpu-jstack will print out a localized date and then jstack will print out a date.
                # I'm just going to ignore the jstack date... don't know if that's good or bad.
                skip_next = True
                continue

            if "Full thread" in line or len(line) == 0:
                continue

            if not current_date:
                continue

            if 'nid=0x' in line:
                nid = re.search('nid=(0x[0-9a-zA-Z]+)', line)[1]
                thread_id = re.match('"([^"]+)"', line)[1]
                result[current_date][nid] = {'lines': [line], 'id': thread_id}
            else:
                result[current_date][nid]['lines'].append(line)

    return result


def parse_top(filename: str, datetime_regex: re, datetime_format: str) -> Dict[str, Any]:
    result = dict()
    current_date = None
    line_count = 0

    with open(filename, 'r') as f:
        for raw_line in f:
            line = raw_line.strip()
            line_count += 1

            if len(line) == 0 or "PID" in line:
                continue

            if datetime_regex.match(line):
                current_date = datetime.strptime(line, datetime_format)
                result[current_date] = dict()
                result[current_date]['threads'] = dict()
                result[current_date]['filename'] = filename
                continue

            if not current_date:
                continue

            if line.startswith('top'):
                result[current_date]['uptime'] = re.search('up[ 0-9a-zA-Z]+', line)[0]
                load_avgs = re.search('load average: ([0-9]+.[0-9]+), ([0-9]+.[0-9]+), ([0-9]+.[0-9]+)', line)
                result[current_date]['load_averages'] = {'1 min': load_avgs[1], '5 min': load_avgs[2], '15 min': load_avgs[3]}
                continue

            if line.startswith('Threads'):
                result[current_date]['tasks'] = re.search('[0-9]+ total', line)[0]
                continue

            if line.startswith('%Cpu'):
                us = re.search('([0-9]+.[0-9]+) us', line)[1]
                sy = re.search('([0-9]+.[0-9]+) sy', line)[1]
                id = re.search('([0-9]+.[0-9]+) id', line)[1]
                result[current_date]['cpu'] = {'us': us, 'sy': sy, 'id': id}
                continue

            if line.startswith('KiB Mem') or line.startswith('MiB Mem'):
                continue

            if line.startswith('KiB Swap') or line.startswith('MiB Swap'):
                continue

            # limit -= 1
            # if limit <= 0:
            #     continue

            # 108335 jboss     20   0   14.7g   8.6g  40040 S  6.2  4.4   0:00.02 Thread-4 (Activ
            fields = line.split()

            pid = int(fields[0])
            hex_pid = hex(pid)
            cpu = float(fields[8])
            mem = float(fields[9])

            result[current_date]['threads'][hex_pid] = {
                'id': line_count,
                'pid': pid,
                'hex_pid': hex_pid,
                'cpu': cpu,
                'mem': mem,
                'top_line': line,
                'status': fields[7],
                'command': ' '.join(fields[11:])
            }

    return result


def print_report(top_data, threaddump_data, limit, cpu_limit, thread_ids, thread_names, print_thread_info, print_stack_trace, cores=1, width=80) -> None:
    for timestamp in sorted(top_data.keys()):
        # TODO(iwalker): handle timestamps being slightly off in top/tdump output?
        top = top_data[timestamp]
        current_limit = limit

        hit_threads = 0
        total_cpu_usage = 0
        hit_cpu_usage = 0

        output = list()

        total_cpu_usage = sum([x['cpu'] for x in top['threads'].values()])

        for thread in sorted(top['threads'].values(), key=itemgetter('cpu'), reverse=True):
            if current_limit != -1:
                current_limit -= 1
                if current_limit < 0:
                    break

            if thread['cpu'] < cpu_limit:
                continue

            nid = thread['hex_pid']

            if thread_ids:
                if str(thread['pid']) not in thread_ids and str(nid) not in thread_ids:
                    continue

            if nid not in threaddump_data[timestamp]:
                threaddump = top['threads'][nid]
                thread_name = threaddump['command']
            else:
                threaddump = threaddump_data[timestamp][nid]
                thread_name = threaddump['id']

            if thread_names:
                if not any([re.search(x, thread_name) for x in thread_names]):
                    continue

            if print_thread_info and 'lines' in threaddump:
                thread_name = threaddump['lines'][0]

            hit_threads += 1
            hit_cpu_usage += thread["cpu"]

            output.append(f'{thread["pid"]:<10} {thread["hex_pid"]:<8} {thread["cpu"]:6.2f} {thread["mem"]:6.2f} {thread_name}')

            if print_stack_trace and 'lines' in threaddump:
                for stack in threaddump['lines'][1:]:
                    output.append(f'{" " * 34}{stack}')

        print('=' * width)
        print(f'DATE/TIME: {timestamp.strftime("%Y-%m-%d %H:%M:%S")}')
        print(f'HIGH-CPU : {top["filename"]}')
        print(f'TDUMP    : {threaddump_data[timestamp]["filename"]}')
        print(f'CPU      : {top["cpu"]["us"]} us, {top["cpu"]["sy"]} sy, {top["cpu"]["id"]} id, load average: {top["load_averages"]["1 min"]} / {top["load_averages"]["5 min"]} / {top["load_averages"]["15 min"]}')  # noqa: E501

        if hit_cpu_usage != total_cpu_usage:
            # NOTE(iwalker): not sure if total_cpu_usage/cores is correct...
            # print(f'CPU%     : {hit_cpu_usage:.2f}% / {total_cpu_usage:.2f}% ({total_cpu_usage / cores:.2f}%)')
            print(f'CPU%     : {hit_cpu_usage:.2f}% / {total_cpu_usage:.2f}%')
        else:
            # NOTE(iwalker): not sure if total_cpu_usage/cores is correct...
            # print(f'CPU%     : {total_cpu_usage:.2f}% ({total_cpu_usage / cores:.2f}%)')
            print(f'CPU%     : {total_cpu_usage:.2f}%')

        if thread_ids or thread_names:
            print(f'TASKS    : {hit_threads} / {top["tasks"]}')
        else:
            print(f'TASKS    : {top["tasks"]}')

        print('-' * width)

        if output:
            print('\n'.join(output))

    print('=' * width)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyze top and Java threaddumps')
    parser.add_argument('datadir', type=str)
    parser.add_argument('-p', '--pid', type=int, help='process ID')
    parser.add_argument('-l', '--limit', type=int, help='limit to top X threads', default=-1)
    parser.add_argument('-S', '--print-stack-trace', action='store_true', help='print stack trace for each thread', default=False)
    parser.add_argument('-I', '--print-thread-info', action='store_true', help='print detailed thread info', default=False)
    parser.add_argument('--cpu', type=float, help='only show threads over CPU%% (example: 4.8)', default=0.0)
    # NOTE(iwalker): datetime format/regex are based on en_US.UTF-8 locale
    parser.add_argument('--datetime-format', type=str, help='strptime format', default='%a %b %d %H:%M:%S %p %Z %Y')
    parser.add_argument('--datetime-regex', type=str, help='regex used to check for a datetime string',
                        default='\\w{3,4} \\w{3,4} {1,2}\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2} (AM|PM) \\w{3,4} \\d{4}')
    parser.add_argument('-t', '--thread-id', dest='thread_ids', action='append', help='limit results to specified *thread* PID (decimal), or NID (hex)')
    parser.add_argument('-T', '--thread-name', dest='thread_names', action='append', help='limit results to specified thread name (regex possible)')

    args = parser.parse_args()

    # NOTE(iwalker): make sure we use the user's specified locale
    # this is *required* to handle %A (weekday abbreviation) correctly
    locale.setlocale(locale.LC_TIME, '')

    datetime_regex = re.compile(args.datetime_regex)

    # NOTE(iwalker): parse top output
    glob_pattern = 'high-cpu.out'
    if args.pid:
        glob_pattern = f'high-cpu-{args.pid}.out'

    top_data = dict()

    for high_cpu_file in Path(args.datadir).rglob(glob_pattern):
        top_data.update(parse_top(high_cpu_file, datetime_regex, args.datetime_format))

    # NOTE(iwalker): parse threaddump
    glob_pattern = 'high-cpu-tdump.out'
    if args.pid:
        glob_pattern = f'high-cpu-tdump-{args.pid}.out'

    threaddump_data = dict()

    for threaddump_file in Path(args.datadir).rglob(glob_pattern):
        threaddump_data.update(parse_threaddump(threaddump_file, datetime_regex, args.datetime_format))

    # NOTE(iwalker): print out the top/threaddump data
    print_report(top_data, threaddump_data, args.limit, args.cpu, args.thread_ids, args.thread_names, args.print_thread_info, args.print_stack_trace)
