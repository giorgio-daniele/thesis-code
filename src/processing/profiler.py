import os
import pandas
import argparse

from collections import Counter

from lib import LOG_BOT_COMPLETE
from lib import LOG_TCP_COMPLETE
from lib import LOG_UDP_COMPLETE
from lib import SERVERS

from lib import periods

# Output file names
STREAMING_PERIODS_OBSERVED     = "streaming_periods_observed.dat"
CPROVIDER_CNAMES_OBSERVED_TCP  = "content_provider_cnames_over_tcp.dat"
CPROVIDER_CNAMES_OBSERVED_UDP  = "content_provider_cnames_over_udp.dat"

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", required=True)
    parser.add_argument("--server", required=True, choices=SERVERS)
    return parser.parse_args().folder, parser.parse_args().server


def main():
    folder, server = args()

    # Validate input
    if not os.path.exists(folder):
        print(f"[ERROR]: {folder} does not exist")
        exit(2)

    if len(os.listdir(folder)) == 0:
        print(f"[ERROR]: {folder} is empty")
        exit(2)
    
    # Init new counters
    streaming_periods = 0
    cnames_tcp        = Counter()
    cnames_udp        = Counter()
    cnames_bytes_tcp  = Counter()
    cnames_bytes_udp  = Counter()
    
    meta = f"meta/{server}"

    try:
        with open(os.path.join(meta, STREAMING_PERIODS_OBSERVED), "r") as f:
            streaming_periods = int(f.readline().strip())
    except:
        pass
    
    if os.path.exists(os.path.join(meta, CPROVIDER_CNAMES_OBSERVED_TCP)):
        stats = pandas.read_csv(os.path.join(meta, CPROVIDER_CNAMES_OBSERVED_TCP), sep=" ")
        cnames_tcp.update(dict(zip(stats["cname"], stats["frequency"])))
        
        for cname, value in zip(stats["cname"], stats["volume"]):
            cnames_bytes_tcp[cname] += value

    if os.path.exists(os.path.join(meta, CPROVIDER_CNAMES_OBSERVED_UDP)):
        stats = pandas.read_csv(os.path.join(meta, CPROVIDER_CNAMES_OBSERVED_UDP), sep=" ")
        cnames_udp.update(dict(zip(stats["cname"], stats["frequency"])))
        
        for cname, value in zip(stats["cname"], stats["volume"]):
            cnames_bytes_udp[cname] += value
    
    for test in os.listdir(folder):
        bot_file = os.path.join(folder, test, LOG_BOT_COMPLETE)
        tcp_file = os.path.join(folder, test, LOG_TCP_COMPLETE)
        udp_file = os.path.join(folder, test, LOG_UDP_COMPLETE)
        
        tcp_data  = pandas.read_csv(tcp_file, sep=" ")
        udp_data  = pandas.read_csv(udp_file, sep=" ")
        intervals = periods(path=bot_file)
        
        for event in intervals:
            ts = event[0]
            te = event[1]
            
            # Update the number of streaming periods observed
            streaming_periods += 1
            
            # Select all TCP/UDP flows overlapping the current streaming period
            tcp_flows = tcp_data[(tcp_data["ts"] <= te) & (tcp_data["te"] >= ts)]
            udp_flows = udp_data[(udp_data["ts"] <= te) & (udp_data["te"] >= ts)]
            
            # Update the CNAMEs observed in TCP
            cnames_tcp.update(set(tcp_flows["cname"]))
            
            # Update the CNAMEs observed in UDP
            cnames_udp.update(set(udp_flows["cname"]))
            
            # Update the bytes counter per each CNAME for TCP
            for cname in tcp_flows["cname"].unique():
                cnames_bytes_tcp[cname] += tcp_flows[tcp_flows["cname"] == cname]["s_bytes_all"].sum()

            # Update the bytes counter per each CNAME for UDP
            for cname in udp_flows["cname"].unique():
                cnames_bytes_udp[cname] += udp_flows[udp_flows["cname"] == cname]["s_bytes_all"].sum()

    with open(os.path.join(meta, STREAMING_PERIODS_OBSERVED), "w") as f:
        f.write(f"{streaming_periods}\n")
    
    with open(os.path.join(meta, CPROVIDER_CNAMES_OBSERVED_TCP), "w") as f:
        f.write("cname frequency volume\n")
        for cname in cnames_tcp.keys():
            freq = cnames_tcp[cname]
            down = cnames_bytes_tcp[cname]
            f.write(f"{cname} {freq} {(down)}\n")

    with open(os.path.join(meta, CPROVIDER_CNAMES_OBSERVED_UDP), "w") as f:
        f.write("cname frequency volume\n")
        for cname in cnames_udp.keys():
            freq = cnames_udp[cname]
            down = cnames_bytes_udp[cname]
            f.write(f"{cname} {freq} {(down)}\n")

if __name__ == "__main__":
    main()
