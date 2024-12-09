import os
import re
import pandas
import yaml
import json
import datetime
import ipaddress
import argparse
import sys

from colorama import init, Fore

from urllib.parse import urlparse

from lib import LOG_BOT_COMPLETE
from lib import LOG_NET_COMPLETE
from lib import LOG_TCP_COMPLETE
from lib import LOG_TCP_PERIODIC
from lib import LOG_UDP_COMPLETE
from lib import LOG_UDP_PERIODIC
from lib import LOG_HAR_COMPLETE
from lib import SERVERS

from lib import Protocol
from lib import CAP
from lib import BOT
from lib import HAR

from lib import fetch
from lib import load_yaml_file

# Useful Tstat names
LAYER_7_PROTOCOL     = "con_t"
SNI_CLIENT_HELLO_TCP = "c_tls_SNI"
SNI_CLIENT_HELLO_UDP = "quic_SNI"
HTTP_SERVER_HOSTNAME = "http_hostname"
FULLY_QUALIFIED_NAME = "fqdn"

# Useful Tstat configuration files
TSTAT_BINARY = "tstat/tstat/tstat"
TSTAT_CONFIG = "tstat/tstat-conf/runtime.conf"
TSTAT_GLOBAL = "tstat/tstat-conf/globals.conf"

def process_har(file: str, server: str, start: float) -> pandas.DataFrame:
    
    # Define the records 
    records = []
    
    # Read the HAR file
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Load manifests based on the server
    manifest_paths = {
        "dazn": [
            os.path.join("src", "general_processing", "res", "dazn", "mpd_manifest_a.yaml"),
            os.path.join("src", "general_processing", "res", "dazn", "mpd_manifest_b.yaml")
        ]
    }
    
    manifests = manifest_paths.get(server, [])
    templates = [load_yaml_file(path=manifest) for manifest in manifests]

    for entry in data["log"]["entries"]:
        # Get ts
        ft = "%Y-%m-%dT%H:%M:%S.%fZ"
        ux = datetime.datetime(1970, 1, 1)
        ts = (datetime.datetime.strptime(entry["startedDateTime"], ft) - ux).total_seconds() * 1000
        ts = ts - (float(start))
        
        # Get te
        ds = ["blocked", "dns", "send", "wait", "receive", "ssl"]
        te = ts + sum(max(0, entry["timings"].get(k, 0)) for k in ds)
        
        # Get MIME
        mime = entry.get("response", {}).get("content", {}).get("mimeType", "")
        
        # Parse the URL
        data = urlparse(entry.get("request", {}).get("url", "-"))
        
        # Get the name of the machine
        machine  = data.netloc
        # Get the requested resource
        resource = data.path
        
        # Default rate
        rate = 0
        
        media = "video"
        if media in resource:
            for template in templates:
                for key, value in template.items():
                    if key in resource:
                        rate = value.get("bitrate", 0)
                        mime = "video/mp4"
                    
        media = "audio"
        if media in resource:
            for template in templates:
                for key, value in template.items():
                    if key in resource:
                        rate = value.get("bitrate", 0)
                        mime = "audio/mp4"
                    
        record = [ts, te, machine, resource, rate, mime]
        records.append(record)
        
    return pandas.DataFrame(records, columns=["ts", "te", "hostname", "resource", "rate", "mime"])

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--server", required=True, choices=SERVERS)
    return parser.parse_args()

def extract_cname(record: pandas.Series, protocol: Protocol) -> str:
    try:
        if protocol == Protocol.TCP:
            layer_7_protocol = record.get(LAYER_7_PROTOCOL, "-")
            if layer_7_protocol == 8912:    # HTTPS
                return record.get(SNI_CLIENT_HELLO_TCP, "-")
            if layer_7_protocol == 1:       # HTTP
                return record.get(HTTP_SERVER_HOSTNAME, "-")
            return record.get(FULLY_QUALIFIED_NAME, "-")
        
        if protocol == Protocol.UDP:
            layer_7_protocol = record.get(LAYER_7_PROTOCOL, "-")
            if layer_7_protocol == 27:      # QUIC
                return record.get(SNI_CLIENT_HELLO_UDP, "-")
            return record.get(FULLY_QUALIFIED_NAME, "-")
        
    except Exception as e:
        print(Fore.RED + f"Error in extracting CNAME: {e}")
        sys.exit(2)

def main():
    try:
        # Extract the arguments
        arguments = args()

        # Access the argument values
        folder = arguments.folder
        output = arguments.output
        server = arguments.server
        
        # Log the start of the process with colors
        print(Fore.YELLOW + f"Starting process for server: {server}")
        print(Fore.YELLOW + f"Processing folder: {folder}")
        print(Fore.YELLOW + f"Saving output to: {output}")
        
        # Get Wireshark traces
        cap_files = fetch(folder=folder, prefix=LOG_NET_COMPLETE, suffix=CAP)
        if not cap_files:
            print(Fore.RED + f"Error: No CAP files found in {folder}")
            sys.exit(3)
        else:
            print(Fore.CYAN + f"Successfully fetched CAP files from {folder}")

        # Get Streambot traces
        bot_files = fetch(folder=folder, prefix=LOG_BOT_COMPLETE, suffix=BOT)
        if not bot_files:
            print(Fore.RED + f"Error: No BOT files found in {folder}")
            sys.exit(4)
        else:
            print(Fore.CYAN + f"Successfully fetched BOT files from {folder}")

        # Get Weblogger traces
        har_files = fetch(folder=folder, prefix=LOG_HAR_COMPLETE, suffix=HAR)
        if not har_files:
            print(Fore.RED + f"Error: No HAR files found in {folder}")
            sys.exit(5)
        else:
            print(Fore.CYAN + f"Successfully fetched HAR files from {folder}")

        os.makedirs(output, exist_ok=True)

        for num, (cap, bot, har) in enumerate(zip(cap_files, bot_files, har_files), start=1):
            dst = os.path.join(output, f"test-{num}")
            os.makedirs(dst, exist_ok=True)

            os.system(f"{TSTAT_BINARY} -G {TSTAT_GLOBAL} -T {TSTAT_CONFIG} {cap} -s {dst} > /dev/null 2>&1")
            os.system(f"find {dst} -mindepth 2 -type f -exec mv -t {dst} {{}} +")
            os.system(f"find {dst} -type d -empty -exec rmdir {{}} +")

            # Copy logs
            for log in [bot, har]:
                name = os.path.basename(log).rsplit('-', 1)[0]
                os.system(f"cp {log} {os.path.join(dst, name)}")

        # Process logs and data
        for test in os.listdir(output):
            try:
                # Proceed with the processing and data manipulation
                bot_complete  = pandas.read_csv(os.path.join(output, test, LOG_BOT_COMPLETE), sep=" ")
                exp_timestamp = bot_complete.iloc[0]["abs"]
                
                # TCP
                tcp_complete = pandas.read_csv(os.path.join(output, test, LOG_TCP_COMPLETE), sep=" ")
                tcp_periodic = pandas.read_csv(os.path.join(output, test, LOG_TCP_PERIODIC), sep=" ")

                # UDP
                udp_complete = pandas.read_csv(os.path.join(output, test, LOG_UDP_COMPLETE), sep=" ")
                udp_periodic = pandas.read_csv(os.path.join(output, test, LOG_UDP_PERIODIC), sep=" ")
                
                # Define the list of columns by which selecting a flow
                id_columns = ["s_ip", "s_port", "c_ip", "c_port"]

                # Eliminate numbers in columns
                tcp_complete.columns = [re.sub(r'[#:0-9]', '', column) for column in tcp_complete.columns]
                tcp_periodic.columns = [re.sub(r'[#:0-9]', '', column) for column in tcp_periodic.columns]
                udp_complete.columns = [re.sub(r'[#:0-9]', '', column) for column in udp_complete.columns]
                udp_periodic.columns = [re.sub(r'[#:0-9]', '', column) for column in udp_periodic.columns]
                    
                # Generate an ID by which targeting each flow
                tcp_complete["id"] = tcp_complete[id_columns].astype(str).agg("-".join, axis=1)
                tcp_periodic["id"] = tcp_periodic[id_columns].astype(str).agg("-".join, axis=1)
                udp_complete["id"] = udp_complete[id_columns].astype(str).agg("-".join, axis=1)
                udp_periodic["id"] = udp_periodic[id_columns].astype(str).agg("-".join, axis=1)

                # Remove LAN traffic
                tcp_complete = tcp_complete[~tcp_complete["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_private)]
                udp_complete = udp_complete[~udp_complete["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_private)]    
                tcp_periodic = tcp_periodic[~tcp_periodic["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_private)]
                udp_periodic = udp_periodic[~udp_periodic["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_private)] 
                
                # Remove multicast traffic
                tcp_complete = tcp_complete[~tcp_complete["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_multicast)]
                udp_complete = udp_complete[~udp_complete["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_multicast)]    
                tcp_periodic = tcp_periodic[~tcp_periodic["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_multicast)]
                udp_periodic = udp_periodic[~udp_periodic["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_multicast)] 
                
                # Extract the name of the server the client was talking to in log_tcp_complete
                tcp_complete["cname"] = tcp_complete.apply(lambda r: extract_cname(record=r, protocol=Protocol.TCP), axis=1)
                # Extract the name of the server the client was talking to in log_udp_complete
                udp_complete["cname"] = udp_complete.apply(lambda r: extract_cname(record=r, protocol=Protocol.UDP), axis=1)
                
                # Insert the name of the server the client was talking to in log_tcp_periodic
                tcp_periodic = tcp_periodic.merge(tcp_complete[["id", "cname"]], on="id", how="left")
                # Insert the name of the server the client was talking to in log_udp_periodic
                udp_periodic = udp_periodic.merge(udp_complete[["id", "cname"]], on="id", how="left")
                
                # Rebase each flow in log_tcp_complete
                tcp_complete["ts"] = tcp_complete["first"] - exp_timestamp
                tcp_complete["te"] = tcp_complete["last"]  - exp_timestamp
                
                # Rebase each bin in log_tcp_periodic
                tcp_periodic["ts"] = tcp_periodic["time_abs_start"] - exp_timestamp
                tcp_periodic["te"] = tcp_periodic["time_abs_start"] - exp_timestamp + tcp_periodic["bin_duration"]
                
                # Rebase each flow in log_udp_complete
                udp_complete["ts"] = udp_complete["s_first_abs"] - exp_timestamp
                udp_complete["te"] = udp_complete["s_first_abs"] - exp_timestamp + (udp_complete["s_durat"] * 1000)
                
                # Rebase each bin in log_udp_periodic
                udp_periodic["ts"] = udp_periodic["time_abs_start"] - exp_timestamp
                udp_periodic["te"] = udp_periodic["time_abs_start"] - exp_timestamp + udp_periodic["bin_duration"]

                # Perform the HAR analysis
                har_complete = process_har(file=os.path.join(output, test, LOG_HAR_COMPLETE), server=server, start=exp_timestamp)
                
                # Save processed data
                har_complete.to_csv(os.path.join(output, test, LOG_HAR_COMPLETE), sep=" ", index=False)
                tcp_complete.to_csv(os.path.join(output, test, LOG_TCP_COMPLETE), sep=" ", index=False)
                udp_complete.to_csv(os.path.join(output, test, LOG_UDP_COMPLETE), sep=" ", index=False)
                tcp_periodic.to_csv(os.path.join(output, test, LOG_TCP_PERIODIC), sep=" ", index=False)
                udp_periodic.to_csv(os.path.join(output, test, LOG_UDP_PERIODIC), sep=" ", index=False)
                
                print(Fore.GREEN + f"Successfully processed test: {test}")
                
            except Exception as e:
                print(Fore.RED + f"Error processing test {test}: {e}")
                sys.exit(6)

    except Exception as e:
        print(Fore.RED + f"Unexpected error: {e}")
        sys.exit(7)

if __name__ == "__main__":
    main()
    print("\n")
