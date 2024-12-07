import os
import re
import pandas
import yaml
import ipaddress
import argparse

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

# Useful Tstat names
LAYER_7_PROTOCOL     = "con_t"
SNI_CLIENT_HELLO_TCP = "c_tls_SNI"
SNI_CLIENT_HELLO_UDP = "quic_SNI"
HTTP_SERVER_HOSTNAME = "http_hostname"
FULLY_QUALIFIED_NAME = "fqdn"

# Useful Tstat configureation files
TSTAT_BINARY = "tstat/tstat/tstat"
TSTAT_CONFIG = "tstat/tstat-conf/runtime.conf"
TSTAT_GLOBAL = "tstat/tstat-conf/globals.conf"

def load_yaml_file(path: str):
    with open(path, "r") as file:
        content = yaml.safe_load(file)
        return content
    return {}

def find_bitrate(resource, templates: list[dict]):
    for template in templates:
        for key, value in template.items():
            if key in resource:
                return value.get("bitrate", None)
    return "-"

def process_har(start: float, har: pandas.DataFrame, server: str) -> pandas.DataFrame:
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

    # Load manifests and templates based on the server
    manifests = None
    templates = []
    if server == "dazn":
        manifests = [
            "src/processing/res/dazn/mpd_manifest_a.yaml",
            "src/processing/res/dazn/mpd_manifest_b.yaml"
        ]
    if server == "nowtv":
        pass
    if server == "netflix":
        pass

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

def args():
    parser = argparse.ArgumentParser()
    
    # Add the arguments
    parser.add_argument("--folder", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--server", required=True, choices=SERVERS)

    # Get the arguments
    args = parser.parse_args()
    
    return (args.folder, args.output, args.server)

def extract_cname(record: pandas.Series, protocol: Protocol) -> str:
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


def main():
    # Get the arguments:
    # - the folder to be processed
    # - the folder where the output is going to be saved
    # - the Content Provider that is going to be processed
    folder, output, server = args()

    # Get Wireshark traces
    cap_files = fetch(folder=folder, prefix=LOG_NET_COMPLETE, suffix=CAP)
    
    # Get Streambot traces
    bot_files = fetch(folder=folder, prefix=LOG_BOT_COMPLETE, suffix=BOT)
    
    # Get Weblogger traces
    har_files = fetch(folder=folder, prefix=LOG_HAR_COMPLETE, suffix=HAR)
    
    # Remove any previous result in the output folder
    os.system(command=f"rm -rf  {output}")
    
    # Generate a new directory at the output location
    os.system(command=f"mkdir -p {output}")

    for num, (cap, bot, har) in enumerate(zip(cap_files, bot_files, har_files), start=1):
        # Define the folder of current test
        dst = os.path.join(output, f"test-{num}")
        
        # Run Tstat command
        cmd = f"{TSTAT_BINARY} -G {TSTAT_GLOBAL} -T {TSTAT_CONFIG} {cap} -s {dst}" 
        cmd = cmd + "> /dev/null 2>&1"
        res = os.system(command=cmd)
            
        # Move command
        cmd = f"find {dst} -mindepth 2 -type f -exec mv -t {dst} {{}} +"
        res = os.system(command=cmd)
        
        # Delete command
        cmd = f"find {dst} -type d -empty -exec rmdir {{}} +"
        res = os.system(command=cmd)
        
        # Save log_bot_complete in the test experiment
        name = os.path.basename(bot).rsplit('-', 1)[0]
        os.system(command=f"cp {bot} {os.path.join(dst, name)}")
        
        # Save log_har_complete in the test experiment
        name = os.path.basename(har).rsplit('-', 1)[0]
        os.system(command=f"cp {har} {os.path.join(dst, name)}")
        
    for test in os.listdir(output):
        # Define the folder of the current test
        test = os.path.join(output, test)
        
        # BOT
        bot_complete  = pandas.read_csv(os.path.join(test, LOG_BOT_COMPLETE), sep=" ")
        exp_timestamp = bot_complete.iloc[0]["abs"]
        
        # TCP
        tcp_complete = pandas.read_csv(os.path.join(test, LOG_TCP_COMPLETE), sep=" ")
        tcp_periodic = pandas.read_csv(os.path.join(test, LOG_TCP_PERIODIC), sep=" ")
        
        # UDP
        udp_complete = pandas.read_csv(os.path.join(test, LOG_UDP_COMPLETE), sep=" ")
        udp_periodic = pandas.read_csv(os.path.join(test, LOG_UDP_PERIODIC), sep=" ")
        
        # HTTP
        har_complete_file = pandas.read_json(os.path.join(test, LOG_HAR_COMPLETE))
        
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
        
        # Remove IPv6 link local traffic
        tcp_complete = tcp_complete[~tcp_complete["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_link_local)]
        udp_complete = udp_complete[~udp_complete["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_link_local)]    
        tcp_periodic = tcp_periodic[~tcp_periodic["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_link_local)]
        udp_periodic = udp_periodic[~udp_periodic["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_link_local)]  

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
        
        har_complete = process_har(start=exp_timestamp, har=har_complete_file, server=server)
        
        # Save log_tcp_complete
        tcp_complete.to_csv(os.path.join(test, LOG_TCP_COMPLETE), index=False, sep=" ")
        # Save log_tcp_periodic
        tcp_periodic.to_csv(os.path.join(test, LOG_TCP_PERIODIC), index=False, sep=" ")
        # Save log_udp_complete
        udp_complete.to_csv(os.path.join(test, LOG_UDP_COMPLETE), index=False, sep=" ")
        # Save log_udp_periodic
        udp_periodic.to_csv(os.path.join(test, LOG_UDP_PERIODIC), index=False, sep=" ")
        # Save log_har_complete
        har_complete.to_csv(os.path.join(test, LOG_HAR_COMPLETE), index=False, sep=" ")
        
if __name__ == "__main__":
    main()
