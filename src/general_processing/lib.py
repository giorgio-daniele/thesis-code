import os
import re
import enum
import pandas
import shutil
import yaml
import ipaddress
import subprocess

from pathlib import Path

SERVERS = ["dazn", "sky"]

LOG_TCP_COMPLETE   = "log_tcp_complete"
LOG_TCP_PERIODIC   = "log_tcp_periodic"
LOG_UDP_COMPLETE   = "log_udp_complete"
LOG_UDP_PERIODIC   = "log_udp_periodic"
LOG_HAR_COMPLETE   = "log_har_complete"
LOG_BOT_COMPLETE   = "log_bot_complete"
LOG_NET_COMPLETE   = "log_net_complete"

class Document(enum.Enum):
    LOG_TCP_COMPLETE = 1
    LOG_TCP_PERIODIC = 2
    LOG_UDP_COMPLETE = 3
    LOG_UDP_PERIODIC = 4


class Protocol(enum.Enum):
    TCP = 1
    UDP = 2

CAP = ".pcap"
BOT = ".csv"
HAR = ".har"

TSTAT_BINARY = "tstat/tstat/tstat"
TSTAT_CONFIG = "tstat/tstat-conf/runtime.conf"
TSTAT_GLOBAL = "tstat/tstat-conf/globals.conf"

def load_yaml_file(path: str):
    with open(path, "r") as file:
        content = yaml.safe_load(file)
        return content
    return {}

def fetch(folder: str, prefix: str, suffix: str) -> list[str]:
    path = Path(folder)
    return sorted([str(f) for f in path.glob(f"{prefix}*{suffix}")])

def format_volume(value: float):
    for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB']:
        if value < 1024.0:
            return f"{value:.2f} {unit}"
        value /= 1024.0


def clean(path: str, pattern="test*"):
    for dir in Path(path).rglob(pattern):
        if dir.is_dir():
            shutil.rmtree(dir)

def tstat(files: list[str], folder: str):
    for num, cap in enumerate(files, start=1):

        out = Path(folder).parent / "tests" / f"test-{num}"
        cmd = [TSTAT_BINARY, "-G", TSTAT_GLOBAL, "-T", TSTAT_CONFIG, cap, "-s", str(out)]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)

        for subdir in Path(out).rglob("*"):
            if subdir.is_dir():
                for file in subdir.iterdir():
                    shutil.move(str(file), out)
                subdir.rmdir()

def streaming_periods(path: str):

    frame = pandas.read_csv(path, sep=r"\s+")
    frame = frame[~frame["event"].str.contains("sniffer|browser|origin|net|app", case=False, na=False)]
    frame = frame.reset_index(drop=True)

    return [(frame.loc[i, "rel"], frame.loc[i + 1, "rel"]) for i in range(0, len(frame) - 1, 2)]

