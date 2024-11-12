import os
import pandas
import argparse

from collections import Counter
from lib.generic import *

# Constants for file names
NUM_EVS_FILE = "num_evs"
NUM_TCP_FILE = "num_tcp"
NUM_UDP_FILE = "num_udp"
TCP_STS_FILE = "tcp_sts"
UDP_STS_FILE = "udp_sts"

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", required=True)
    parser.add_argument("--server", required=True, choices=SERVERS)

    # Get the arguments
    return parser.parse_args().folder, parser.parse_args().server

#############################
#           MAIN            #
#############################

def main():
    folder, server = args()

    print(f"PROFILER is running on [{folder}] for service [{server}]...")

    # Check if the folder contains tests folder
    dir = os.path.join(folder, "tests")
    if not os.path.exists(dir):
        print(f"    There is an error: {folder} does not contain any tests")
        exit(1)

    # Check if tests folder is not empty
    if len(os.listdir(dir)) == 0:
        print(f"    There is an error: {folder}/tests is empty")
        exit(2)

    if server == "dazn":
        from lib.dazn import METADATA

    # Init statistics
    num_evs = 0
    num_tcp = 0
    num_udp = 0

    # Init frames
    tcp_sts = None
    udp_sts = None

    # Counters
    tcp_count = Counter()
    udp_count = Counter()

    if not os.path.exists(METADATA):
        print(f"    Metadata for {server} is not available")
        print(f"    Creating a new one...")
        os.makedirs(METADATA, exist_ok=True)
    else:
        try:
            with open(os.path.join(METADATA, NUM_EVS_FILE), "r") as f:
                num_evs = int(f.readline().strip())
            with open(os.path.join(METADATA, NUM_TCP_FILE), "r") as f:
                num_tcp = int(f.readline().strip())
            with open(os.path.join(METADATA, NUM_UDP_FILE), "r") as f:
                num_udp = int(f.readline().strip())
        except Exception as e:
            pass

        if os.path.exists(os.path.join(METADATA, TCP_STS_FILE)):
            tcp_sts = pandas.read_csv(os.path.join(METADATA, TCP_STS_FILE), sep=" ")
            tcp_count.update(dict(zip(tcp_sts["cn"], tcp_sts["count"])))

        if os.path.exists(os.path.join(METADATA, UDP_STS_FILE)):
            udp_sts = pandas.read_csv(os.path.join(METADATA, UDP_STS_FILE), sep=" ")
            udp_count.update(dict(zip(udp_sts["cn"], udp_sts["count"])))

    print(f"Statistics at this runtime")
    print(f"    NUM events: {num_evs}")
    print(f"    NUM tcp flows analyzed: {num_tcp}")
    print(f"    NUM udp flows analyzed: {num_udp}")

    
    for test in os.listdir(os.path.join(folder, "tests")):
        dir = os.path.join(folder, "tests")
        bot = os.path.join(dir, test, LOG_BOT_COMPLETE)
        tcp = os.path.join(dir, test, LOG_TCP_COMPLETE)
        udp = os.path.join(dir, test, LOG_UDP_COMPLETE)

        tcp = pandas.read_csv(tcp, sep=" ")
        udp = pandas.read_csv(udp, sep=" ")

        evs = periods(path=bot)

        for evn in evs:
            ts, te = evn[0], evn[1]

            """
            =============================
            =    Processing TCP Layer   =
            =============================
            """

            flows = None
            names = None

            # Extract all TCP flows that overlap [ts; te]
            flows = tcp[(tcp["ts"] <= te) & (tcp["te"] >= ts)]
            names = set(flows["cn"].tolist())
            tcp_count.update(names)
            num_tcp += len(flows)

            """
            =============================
            =    Processing UDP Layer   =
            =============================
            """

            flows = None
            names = None

            # Extract all UDP flows that overlap [ts; te]
            flows = udp[(udp["ts"] <= te) & (udp["te"] >= ts)]
            names = set(flows["cn"].tolist())
            udp_count.update(names)
            num_udp += len(flows)

            # Update the number of events processed
            num_evs += 1

    # Update on disk
    with open(os.path.join(METADATA, NUM_EVS_FILE), "w") as f:
        f.write(f"{num_evs}\n")
    with open(os.path.join(METADATA, NUM_TCP_FILE), "w") as f:
        f.write(f"{num_tcp}\n")
    with open(os.path.join(METADATA, NUM_UDP_FILE), "w") as f:
        f.write(f"{num_udp}\n")

    # Write back updated statistics for TCP CNAMEs
    with open(os.path.join(METADATA, TCP_STS_FILE), "w") as f:
        f.write("cn count\n")
        for cname, count in tcp_count.items():
            f.write(f"{cname} {count}\n")

    # Write back updated statistics for UDP CNAMEs
    with open(os.path.join(METADATA, UDP_STS_FILE), "w") as f:
        f.write("cn count\n")
        for cname, count in udp_count.items():
            f.write(f"{cname} {count}\n")

if __name__ == "__main__":
    print("-" * 50)
    main()
    print("-" * 50)
