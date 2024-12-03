#! /usr/bin/env python3
#
# Efficient push/pull unidirectional syncing for ship/shore based applications
#
# This is a rewrite of my ARCTERX and SUNRISE code.
#
# Nov-2024, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import yaml
import subprocess
import queue
import time
import os.path
import re
import logging
from TPWUtils import Logger
from TPWUtils.INotify import INotify
from TPWUtils.Thread import Thread

class PushTo(Thread):
    def __init__(self, dirname:str, targets:tuple, args:ArgumentParser) -> None:
        Thread.__init__(self, "Push_" + dirname, args)
        self.__dirRoot = os.path.abspath(os.path.expanduser(dirname))
        self.__targets = targets

    def rsyncTo(self, src:str, tgt:str) -> None:
        cmd = (
                self.args.rsync,
                "--verbose",
                "--archive",
                "--temp-dir", self.args.cache,
                "--delete-delay",
                os.path.join(src, ""), # Add trailing slash
                tgt,
                )
        sp = subprocess.run(
                cmd,
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                )
        if sp.stdout:
            try:
                sp.stdout = str(sp.stdout, "utf-8")
            except:
                pass
        if sp.returncode:
            logging.warning("Failed executing %s\n%s", " ".join(sp.args), sp.stdout)
        elif sp.stdout:
            logging.info("Executed: %s\n%s", " ".join(sp.args), sp.stdout)
        else:
            logging.info("Executed: %s", " ".join(sp.args))

        return sp.returncode == 0

    def runIt(self) -> None: # Called on start
        args = self.args
        dirRoot = self.__dirRoot
        targets = self.__targets
        sleepTime = self.args.pushDelay
        logging.info("Starting %s with delay %s", targets, sleepTime)
        inotify = INotify(args)
        inotify.start()
        inotify.addTree(dirRoot)
        q = inotify.queue

        # Do an initial sync to know where we're starting at

        sources = set()
        sources.add(dirRoot)
        src = dirRoot

        qClearSources = True

        for tgt in targets:
            if not self.rsyncTo(src, tgt): qClearSources = False

        if qClearSources: sources = set()


        while True:
            (t0, fn) = q.get()
            dirname = fn if os.path.isdir(fn) else os.path.dirname(fn)
            sources.add(dirname)
            dt = max(t0 - time.time() + sleepTime, 0.1)
            logging.info("Sleeping for %s seconds due to %s", dt, fn)
            time.sleep(dt)
            while not q.empty():
                (t0, fn) = q.get()
                dirname = fn if os.path.isdir(fn) else os.path.dirname(fn)
                sources.add(dirname)

            src = os.path.commonpath(sources) # Common path for all updated files
            relpath = os.path.relpath(src, start=dirRoot) # Strip off root portion of path

            qClearSources = True

            for tgt in targets:
                if relpath != ".": tgt = os.path.join(tgt, relpath)
                if not self.rsyncTo(src, tgt):
                    qClearSources = False

            if qClearSources: sources = set()

            q.task_done()

class MonitorRemote(Thread):
    def __init__(self, hostname:str, directory:str, args:ArgumentParser) -> None:
        Thread.__init__(self, "monitor_" + src, args)
        self.__hostname = hostname
        self.__directory = directory
        self.queue = queue.Queue()

    def runIt(self) -> None: # Called on start
        args = self.args
        hostname = self.__hostname
        directory = self.__directory
        q = self.queue
        logging.info("Starting host %s directory %s", hostname, directory)
        cmd = (
                args.ssh,
                hostname,
                args.monitorRemote,
                "--verbose",
                "--logfile", os.path.abspath(os.path.expanduser("~/logs/monitorRemote.log")),
                directory,
                str(args.pullDelay)
                )

        for cnt in range(args.retries):
            with subprocess.Popen(cmd, 
                                  shell=False, 
                                  stdout=subprocess.PIPE, 
                                  stderr=subprocess.STDOUT,) as proc:
                while True:
                    line = proc.stdout.readline()
                    if not line: break
                    matches = re.match(b"^:(.+)\s+$", line)
                    if not matches:
                        logging.info("unmatched line %s", line)
                        continue
                    try:
                        fn = str(matches[1], "utf-8")
                    except:
                        fn = matches[1]

                    logging.info("Sending %s", fn)
                    q.put(fn)

            logging.info("Retry %s/%s sleeping for %s", cnt, args.retries, args.retrySleep)
            time.sleep(args.retrySleep)
            q.put((time.time(), None))
        logging.info("Exiting")

class PullFrom(Thread):
    def __init__(self, dirname:str, src:str, args:ArgumentParser) -> None:
        Thread.__init__(self, "Pull_" + dirname, args)
        self.__dirRoot = os.path.abspath(os.path.expanduser(dirname))
        self.__source = src

    def rsyncFrom(self, src:str, tgt:str) -> bool:
        cmd = (
                self.args.rsync,
                "--verbose",
                "--archive",
                "--temp-dir", os.path.abspath(os.path.expanduser(self.args.cache)),
                "--delete-delay",
                os.path.join(src, ""), # Add trailing slash
                tgt,
                )
        sp = subprocess.run(
                cmd,
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                )
        if sp.stdout:
            try:
                sp.stdout = str(sp.stdout, "utf-8")
            except:
                pass
        if sp.returncode:
            logging.warning("Failed executing %s\n%s", " ".join(sp.args), sp.stdout)
        elif sp.stdout:
            logging.info("Executed: %s\n%s", " ".join(sp.args), sp.stdout)
        else:
            logging.info("Executed: %s", " ".join(sp.args))

        return sp.returncode == 0

    def runIt(self) -> None: # Called on start
        args = self.args
        tgt = self.__dirRoot
        src = self.__source
        (hostname, directory) = src.split(":", 1)
        sleepTime = self.args.pullDelay
        logging.info("Starting tgt %s src %s with delay %s", tgt, src, sleepTime)

        monitor = MonitorRemote(hostname, directory, args)
        monitor.start()
        q = monitor.queue

        self.rsyncFrom(src, tgt) # Do an initial sync to know where we're starting at

        while True:
            path = q.get()
            try:
                if ~isinstance(path, str):
                    path = str(path, "UTF-8")
                if path == ".":
                    srcPath = directory
                    tgtPath = tgt
                else:
                    srcPath = os.path.join(directory, path)
                    tgtPath = os.path.join(tgt, path)
                srcPath = hostname + ":" + srcPath
                self.rsyncFrom(srcPath, tgtPath)
            except:
                logging.exception("Unable to convert %s to str", path)
            q.task_done()

parser = ArgumentParser()
Logger.addArgs(parser)
parser.add_argument("--config", type=str, default="config.yaml", help="YAML configuration filename")
grp = parser.add_argument_group(description="Push related options")
grp.add_argument("--pushDelay", type=float, default=20,
                    help="Seconds to delay pushing after a file has been modified.")
grp = parser.add_argument_group(description="Pull related options")
grp.add_argument("--pullDelay", type=float, default=20,
                    help="Seconds between attempts to pull from host")
grp.add_argument("--bwlimit", type=str, help="Rsync --bw-limit RATE argument")
grp = parser.add_argument_group(description="Remote monitor related options")
grp.add_argument("--monitorRemote", type=str, 
                 default=os.path.join(os.path.dirname(__file__), "monitorRemote.py"),
                 help="Path on remote host to get modified files")
grp.add_argument("--retries", type=int, default=100,
                 help="How many ssh reconnect attempts before throwing an exception")
grp.add_argument("--retrySleep", type=int, default=600,
                 help="How long to wait between reconnect attempts")
grp = parser.add_argument_group(description="Path related options")
grp.add_argument("--ssh", type=str, default="/usr/bin/ssh", help="ssh command to use")
grp.add_argument("--rsync", type=str, default="/usr/bin/rsync", help="rsync command to use")
grp.add_argument("--cache", type=str, default="~/.cache", help="rsync --temp-dir")
args = parser.parse_args()

args.cache = os.path.abspath(os.path.expanduser(args.cache))

Logger.mkLogger(args)

thrds = []

with open(args.config, "r") as fp:
    a = yaml.safe_load(fp)
    for dirname in a:
        item = a[dirname]
        if item is None: continue
        if "pushTo" in item:
            thrds.append(PushTo(dirname, item["pushTo"], args))
            thrds[-1].start()
        if "pullFrom" in item:
            for src in item["pullFrom"]:
                thrds.append(PullFrom(dirname, src, args))
                thrds[-1].start()

try:
    Thread.waitForException()
except:
    logging.exception("Unexpected exception")
