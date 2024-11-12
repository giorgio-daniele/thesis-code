import os
import re
import enum
import pandas
import shutil
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

def fetch(folder: str, prefix: str, suffix: str) -> list[str]:
    path = Path(folder)
    return sorted([str(f) for f in path.glob(f"{prefix}*{suffix}")])

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

def periods(path: str):

    frame = pandas.read_csv(path, sep=r"\s+")
    frame = frame[~frame["event"].str.contains("sniffer|browser|origin|net|app", case=False, na=False)]
    frame = frame.reset_index(drop=True)

    return [(frame.loc[i, "rel"], frame.loc[i + 1, "rel"]) for i in range(0, len(frame) - 1, 2)]



TCP_METRICS = [
    "ts", 
    "te", 
    "idle",
    "avg_span",
    "std_span",
    "max_span",
    "min_span",
    # Client metrics
    "c_ack_cnt", 
    "c_ack_cnt_p", 
    "c_bytes_all", 
    # Server metrics
    "s_ack_cnt", 
    "s_ack_cnt_p", 
    "s_bytes_all", 
    # Ground-truth
    "avg_video_rate",
    "avg_audio_rate",
    "video_requests_sequence",
    "audio_requests_sequence"
]


UDP_METRICS = [
    "ts", 
    "te", 
    "idle",
    "avg_span",
    "std_span",
    "max_span",
    "min_span",
    # Client metrics
    "c_bytes_all", 
    # Server metrics
    "s_bytes_all", 
    # Ground-truth
    "avg_video_rate",
    "avg_audio_rate",
    "video_requests_sequence",
    "audio_requests_sequence"
]

