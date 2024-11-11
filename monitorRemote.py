#! /usr/bin/env python3
#
# Monitor a directory tree for changes and spit them out
#
# Resurrect and modernize code from SUNRISE-2019
#
# Nov-2024, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
from TPWUtils.INotify import INotify
import time
import os.path
import logging
from TPWUtils import Logger

parser = ArgumentParser()
Logger.addArgs(parser)
parser.add_argument("tgt", type=str, help="Directory to watch")
parser.add_argument("delay", type=float, help="Seconds after inotify before printing out")
args = parser.parse_args()

Logger.mkLogger(args)

try:
    tgt = os.path.abspath(os.path.expanduser(args.tgt))
    delay = args.delay

    i = INotify(args)
    i.start()
    i.addTree(tgt)
    q = i.queue

    while True:
        (t0, fn) = q.get()
        sources = set()
        sources.add(fn if os.path.isdir(fn) else os.path.dirname(fn))
        dt = max(0.1, t0 - time.time() + delay) # How long to sleep
        logging.info("Modified %s sleeping for %s", fn, dt)
        time.sleep(dt)
        while not q.empty():
            (t0, fn) = q.get()
            q.task_done()
            sources.add(fn if os.path.isdir(fn) else os.path.dirname(fn))
        src = os.path.commonpath(sources)
        logging.info("Sending %s", src)
        print(f"src:{src}")
        q.task_done()
except:
    logging.exception("Unexpected exception")
