import os
import re
import pandas
import yaml
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

def find_bitrate(resource, templates: list[dict]):
    for template in templates:
        for key, value in template.items():
            if key in resource:
                return value.get("bitrate", None)
    return "-"

def process_har(start: float, har: pandas.DataFrame, server: str) -> pandas.DataFrame:
    try:
        # Load manifests and templates based on the server
        manifests = None
        templates = []
        if server == "dazn":
            manifests = [
                os.path.join("src", "general_processing", "res", "dazn", "mpd_manifest_a.yaml"),
                os.path.join("src", "general_processing", "res", "dazn", "mpd_manifest_b.yaml"),
            ]
        if server == "nowtv":
            # TODO
            pass
        if server == "netflix":
            # TODO
            pass

        # Generate the entries
        entries = pandas.json_normalize(har["log"]["entries"])

        # Get the timestamp when the request has started
        ts = pandas.to_datetime(entries["startedDateTime"])
        ts = ts.astype(int) / 10**6
        ts = ts - start

        # Generate the timings
        timings = entries.filter(regex="timings").fillna(0)
        timings[timings < 0] = 0
        offset = timings.sum(axis=1)

        # Get the timestamp when the request has finished
        te = ts + offset

        # Get the MIME types
        mime = entries.filter(regex="response.content.mimeType").iloc[:, 0]

        # Get the URLs
        urls = entries.filter(regex="request.url").iloc[:, 0]

        # Get the hostnames
        hostnames = urls.apply(lambda x: urlparse(x).hostname)

        # Get the requested resources
        resources = urls.apply(lambda x: urlparse(x).path)

        # Load templates from the manifest files
        for manifest in manifests:
            templates.append(load_yaml_file(path=manifest))

        # Get the rate of the requested resource (bitrate)
        bitrates = resources.apply(lambda x: find_bitrate(x, templates))

        # Generate the final frame
        frame = pandas.DataFrame({
            "ts": ts,
            "te": te,
            "hostname": hostnames,
            "resource": resources,
            "mime": mime,
            "rate": bitrates,
        })

        return frame 

    except Exception as e:
        print(Fore.RED + f"Error in processing HAR: {e}")
        sys.exit(1)

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

                # HTTP
                har_complete_file = pandas.read_json(os.path.join(output, test, LOG_HAR_COMPLETE))
                
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
                har_complete = process_har(exp_timestamp, har_complete_file, server)
                
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
