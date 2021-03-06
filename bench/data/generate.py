#!/usr/bin/env python3

try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode

import os
import json
import random

import numpy as np
import pandas as pd

QUERIES_DIR = "queries"
QUERIES_OUTPUT_DIR = "queries.generated"
PRELOAD_DIR = "preload"
PRELOAD_OUTPUT_DIR = "preload.generated"
TARGET_URL = "http://localhost:9090"


def parse_duration(s):
    if s.endswith("s"):
        return int(s[:-1])
    elif s.endswith("m"):
        return int(s[:-1]) * 60
    elif s.endswith("h"):
        return int(s[:-1]) * 60 * 60
    elif s.endswith("d"):
        return int(s[:-1]) * 24 * 60 * 60
    elif s.endswith("w"):
        return int(s[:-1]) * 7 * 24 * 60 * 60
    elif s.endswith("y"):
        return int(s[:-1]) * 365 * 24 * 60 * 60
    else:
        return int(s)


def proc_queries_line(line, dst_fp):
    blocks = [s.strip() for s in line.split("\t")]
    if len(blocks) != 6:
        print("Skipped line", index, "--", line)
        return
    pivot_time, duration, step, time_random_range, variation_count, query = \
        parse_duration(blocks[0]), parse_duration(blocks[1]), \
        parse_duration(blocks[2]), \
        parse_duration(blocks[3]), int(blocks[4]), blocks[5]

    for i in range(variation_count):
        time = int(pivot_time + time_random_range * random.random())
        q = {
            'query': query,
            'start': int(time) - int(duration),
            'step': step,
            'end': time,
        }
        print("GET", "%s/api/v1/query_range?%s" % (TARGET_URL, urlencode(q)), file=dst_fp)


def proc_queries_file(src_fp, dstdir):
    for index, line in enumerate(src_fp):
        if line.startswith("#"):
            continue
        dstpath = os.path.join(dstdir, str(index))
        with open(dstpath, 'w') as dst_fp:
            proc_queries_line(line, dst_fp)


def generate_queries():
    for fname in os.listdir(QUERIES_DIR):
        srcpath = os.path.join(QUERIES_DIR, fname)
        srcname, _ = os.path.splitext(fname)
        dstdir = os.path.join(QUERIES_OUTPUT_DIR, srcname)
        os.makedirs(dstdir, exist_ok=True)
        with open(srcpath) as src_fp:
            proc_queries_file(src_fp, dstdir)


class MetricDefinition(object):
    def __init__(self, metric, generator_expr):
        self.metric = metric
        self.expr = generator_expr

    def samples(self, ctr):
        return eval(self.expr, {'np': np}, {'ctr': ctr})


def generate_preload_file():
    for fname in os.listdir(PRELOAD_DIR):
        srcpath = os.path.join(PRELOAD_DIR, fname)
        dstpath = os.path.join(PRELOAD_OUTPUT_DIR, fname + ".parquet")
        try:
            if os.path.getmtime(srcpath) < os.path.getctime(dstpath):
                continue
        except FileNotFoundError:
            pass
        series = {}
        count = 100000
        timestamps = None
        with open(srcpath) as src_fp:
            for index, line in enumerate(src_fp):
                if index == 0:  # Header line
                    dic = json.loads(line)
                    count = int(dic["count"])
                    if "start" in dic:
                        start = parse_duration(dic["start"])
                    else:
                        start = 0
                    if "step" in dic:
                        step = parse_duration(dic["step"])
                    else:
                        step = 0
                    timestamps = pd.Index(range(start, step * count, step)) * 1000 # seconds --> ms
                    continue
                if line.startswith("#"):
                    continue
                blocks = [s.strip() for s in line.split("\t")]
                if len(blocks) != 2:
                    print("Skipped line", index, "--", line)
                    continue
                metric, expr = blocks[0], blocks[1]
                series[metric] = pd.Series(
                    eval(expr, {'np': np}, {'ctr': count}),
                    index=timestamps,
                    dtype=float
                )

        df = pd.DataFrame(series)
        df.to_parquet(dstpath)


if __name__ == '__main__':
    os.makedirs(QUERIES_OUTPUT_DIR, exist_ok=True)
    os.makedirs(PRELOAD_OUTPUT_DIR, exist_ok=True)
    generate_queries()
    generate_preload_file()
