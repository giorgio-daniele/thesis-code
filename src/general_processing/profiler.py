import os
import pandas
import argparse

from collections import Counter
from colorama import init, Fore, Style

from lib import LOG_BOT_COMPLETE
from lib import LOG_TCP_COMPLETE
from lib import LOG_TCP_PERIODIC
from lib import LOG_UDP_COMPLETE
from lib import LOG_UDP_PERIODIC
from lib import LOG_HAR_COMPLETE
from lib import SERVERS

from lib import streaming_periods

# Output file names
STREAMING_PERIODS_OBSERVED     = "streaming_periods_observed.dat"
CPROVIDER_CNAMES_OBSERVED_TCP  = "content_provider_cnames_over_tcp.dat"
CPROVIDER_CNAMES_OBSERVED_UDP  = "content_provider_cnames_over_udp.dat"

# Initialize colorama
init(autoreset=True)

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", required=True)
    parser.add_argument("--server", required=True, choices=SERVERS)
    return parser.parse_args()

def update_cnames_and_bytes(data: dict, cnames_counter: Counter, bytes_counter: Counter):
    for cname in data["cname"].unique():
        cnames_counter.update([cname])
        bytes_counter[cname] += data[data["cname"] == cname]["s_bytes_all"].sum()

def read_existing_stats(meta: str, file_name: str, cnames_counter: Counter, bytes_counter: Counter):
    file_path = os.path.join(meta, file_name)
    if os.path.exists(file_path):
        stats = pandas.read_csv(file_path, sep=" ")
        cnames_counter.update(dict(zip(stats["cname"], stats["frequency"])))
        for cname, value in zip(stats["cname"], stats["volume"]):
            bytes_counter[cname] += value

def write_stats(meta: str, file_name: str, cnames_counter: Counter, bytes_counter: Counter):
    with open(os.path.join(meta, file_name), "w") as f:
        f.write("cname frequency volume\n")
        for cname in cnames_counter:
            f.write(f"{cname} {cnames_counter[cname]} {bytes_counter[cname]}\n")

def main():
    # Extract the arguments
    arguments = args()

    # Access the argument values
    folder = arguments.folder
    server = arguments.server
    
    # Init new counters
    periods = 0
    cnames_tcp = Counter()
    cnames_udp = Counter()
    cnames_bytes_tcp = Counter()
    cnames_bytes_udp = Counter()

    meta = f"meta/{server}"

    # Load existing statistics if available
    try:
        with open(os.path.join(meta, STREAMING_PERIODS_OBSERVED), "r") as f:
            periods = int(f.readline().strip())
        print(f"{Fore.GREEN}\tExisting streaming periods found. Periods count: {periods}")
    except FileNotFoundError:
        print(f"{Fore.YELLOW}\tNo existing streaming periods found. Starting fresh.")

    # Read observed CNAME stats for TCP and UDP
    print(f"{Fore.CYAN}\tReading existing stats for TCP and UDP CNAMEs...")
    read_existing_stats(meta, CPROVIDER_CNAMES_OBSERVED_TCP, cnames_tcp, cnames_bytes_tcp)
    read_existing_stats(meta, CPROVIDER_CNAMES_OBSERVED_UDP, cnames_udp, cnames_bytes_udp)

    # Process each test
    for test in os.listdir(folder):
        print(f"{Fore.MAGENTA}\tProcessing test: {test}")
        
        bot_file = os.path.join(folder, test, LOG_BOT_COMPLETE)
        tcp_file = os.path.join(folder, test, LOG_TCP_COMPLETE)
        udp_file = os.path.join(folder, test, LOG_UDP_COMPLETE)

        # Read data
        print(f"{Fore.YELLOW}\tReading TCP and UDP data for test {test}...")
        tcp_data = pandas.read_csv(tcp_file, sep=" ")
        udp_data = pandas.read_csv(udp_file, sep=" ")
        intervals = streaming_periods(path=bot_file)

        # Process each streaming interval
        for ts, te in intervals:
            periods += 1
            print(f"{Fore.BLUE}\tProcessing streaming period: {ts} to {te}...")

            # Filter overlapping flows
            tcp_flows = tcp_data[(tcp_data["ts"] <= te) & (tcp_data["te"] >= ts)]
            udp_flows = udp_data[(udp_data["ts"] <= te) & (udp_data["te"] >= ts)]

            # Update CNAMEs and bytes counters
            print(f"{Fore.CYAN}\t\tUpdating CNAMEs and bytes counters...")
            update_cnames_and_bytes(tcp_flows, cnames_tcp, cnames_bytes_tcp)
            update_cnames_and_bytes(udp_flows, cnames_udp, cnames_bytes_udp)

    # Save the results
    print(f"{Fore.GREEN}\tSaving the updated statistics...")
    with open(os.path.join(meta, STREAMING_PERIODS_OBSERVED), "w") as f:
        f.write(f"{periods}\n")

    write_stats(meta, CPROVIDER_CNAMES_OBSERVED_TCP, cnames_tcp, cnames_bytes_tcp)
    write_stats(meta, CPROVIDER_CNAMES_OBSERVED_UDP, cnames_udp, cnames_bytes_udp)

if __name__ == "__main__":
    main()
    print("\n")
