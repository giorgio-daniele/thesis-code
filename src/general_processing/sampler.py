import os
import numpy
import uuid
import yaml
import re
import pandas
import shutil
import argparse

from colorama import init, Fore

from lib import LOG_BOT_COMPLETE
from lib import LOG_TCP_COMPLETE
from lib import LOG_TCP_PERIODIC
from lib import LOG_UDP_COMPLETE
from lib import LOG_UDP_PERIODIC
from lib import LOG_HAR_COMPLETE
from lib import SERVERS

from lib import Protocol
from lib import streaming_periods
from lib import load_yaml_file

def matches(cname: str, expressions: list):
    if isinstance(cname, str):
        return any(regex.search(cname) for regex in expressions)
    return False

def load_regex_patterns(path: str):
    with open(path) as f:
        strings = [line.strip() for line in f if line.strip()]
    return [re.compile(string) for string in strings]

def merge_intervals(intervals: list[list[float]]) -> list[list[float]]:
    intervals.sort(key=lambda x: x[0])
    result = []
    for interval in intervals:
        if not result or interval[0] > result[-1][1]:
            result.append(interval)
        else:
            result[-1][1] = max(result[-1][1], interval[1])
    return result

def process_media_data(requests: pandas.DataFrame, mime_type: str, ts: float):
    # Filter the data based on mime type
    media = requests[requests["mime"].str.contains(mime_type)]

    # Initialize defaults
    mean = 0.0
    http = "-"

    # Process the media data if available
    if not media.empty:
        mean = media["rate"].dropna().astype(float).mean()

        http_list = []
        for _, record in media.iterrows():
            http_list.append(f"{record['ts'] - ts}#{record['ts'] - ts}#{record['rate']}")
        http = "_".join(http_list)
    
    return mean, http
     

def sample_bins(bins: pandas.DataFrame, 
                http: pandas.DataFrame, protocol: Protocol, ts: float, te: float, step: float) -> pandas.DataFrame: 
    # Define the list of records
    records = []
    
    # Define the schema for the features to be used
    schema = None
    if protocol == Protocol.TCP:
        path   = os.path.join("src", "general_processing", "res", "tcp_metrics.yaml")
        schema = load_yaml_file(path=path)
    if protocol == Protocol.UDP:
        path   = os.path.join("src", "general_processing", "res", "udp_metrics.yaml")
        schema = load_yaml_file(path=path)
    
    for ti in range(int(ts), int(te - step), int(step)):
        tj = ti + step
        
        # Select HTTP requests in this step
        meta = http[(http["ts"] <= tj) & (http["te"] >= ti)].copy()
        
        # Remove anything that is NaN
        meta = meta.dropna(subset=["mime"])
        
        # Select Tstat bins in this step
        data = bins[(bins["ts"] <= tj) & (bins["te"] >= ti)].copy()
        
        # Print current step interval and number of HTTP requests
        print(f"{Fore.WHITE}\tProcessing step: {ti} to {tj}")
        print(f"{Fore.WHITE}\t\tNumber of HTTP requests: {len(meta)}")
        
        # Print number of bins in the current step
        print(f"{Fore.WHITE}\t\tNumber of bins: {len(data)}")
        
        if data.empty:
            records.append([ti, tj, step] + [0] * (len(schema["columns"]) - 5) + ["-", "-"])
            continue
        
        # Compute the relative start and end of each bin
        data["rel_ts"] = numpy.maximum(data["ts"], ti)
        data["rel_te"] = numpy.minimum(data["te"], tj)
        data["factor"] = (data["rel_te"] - data["rel_ts"]) / (data["te"] - data["ts"]).replace(0, 1)
        
        # Temporal statistics
        ints = data[["rel_ts", "rel_te"]].values.tolist()
        span = sum(end - start for start, end in merge_intervals(ints))
        idle = step - span
        
        avg_span = (data["rel_te"] - data["rel_ts"]).mean()
        max_span = (data["rel_te"] - data["rel_ts"]).max()
        min_span = (data["rel_te"] - data["rel_ts"]).min()
        std_span = (data["rel_te"] - data["rel_ts"]).std()
        
        # Volumetric statistics
        if protocol == Protocol.TCP:
            s_bytes_uniq = float((data["s_bytes_uniq"] * data["factor"]).sum())  
            c_bytes_uniq = float((data["c_bytes_uniq"] * data["factor"]).sum())
            c_ack_cnt    = float((data["c_ack_cnt"]    * data["factor"]).sum())
            c_ack_cnt_p  = float((data["c_ack_cnt_p"]  * data["factor"]).sum())
            s_ack_cnt    = float((data["s_ack_cnt"]    * data["factor"]).sum())
            s_ack_cnt_p  = float((data["s_ack_cnt_p"]  * data["factor"]).sum())  
        c_bytes_all = float((data["c_bytes_all"] * data["factor"]).sum())
        s_bytes_all = float((data["s_bytes_all"] * data["factor"]).sum())
        
        # Ground-truth statistics
        avg_video, video_sequence = process_media_data(meta, "video", ts)
        avg_audio, audio_sequence = process_media_data(meta, "audio", ts)
        
        # Append the correct records
        if protocol == Protocol.TCP:
            records.append([
                ti, tj, idle, avg_span, std_span, max_span, min_span,
                c_ack_cnt, c_ack_cnt_p, c_bytes_all, c_bytes_uniq,
                s_ack_cnt, s_ack_cnt_p, s_bytes_all, s_bytes_uniq,
                avg_video, avg_audio, video_sequence, audio_sequence  
            ])
        
        if protocol == Protocol.UDP:
            records.append([
                ti, tj, idle, avg_span, std_span, max_span, min_span,
                c_bytes_all, s_bytes_all,
                avg_video, avg_audio, video_sequence, audio_sequence  
            ])         
    
    # Create a pandas DataFrame from the collected records
    samples = pandas.DataFrame(records, columns=schema["columns"])

    # Rescale each sample according to the first timestamp
    if not samples.empty:
        first_ts = float(samples["ts"].iloc[0])
        samples["ts"] -= first_ts
        samples["te"] -= first_ts
        
    return samples
        

def args():
    parser = argparse.ArgumentParser()
    
    # Add the arguments
    parser.add_argument("--folder", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--server", required=True, choices=SERVERS)

    # Get the arguments
    args = parser.parse_args()
    
    return (args.folder, args.output, args.server)


def main():
    # Define steps
    #steps = [i * 1000 for i in range(1, 11)]
    steps  = [1_000, 10_000]
    
    # Command line arguments
    folder, output, server = args()

    # Remove any previous result in the output folder
    shutil.rmtree(output, ignore_errors=True)
    print(Fore.YELLOW + f"Removed previous output in {output} folder.")

    for step in steps:
        # Generate TCP samples folder output
        tcp_folder = os.path.join(output, "tcp", str(step))
        os.makedirs(tcp_folder, exist_ok=True)
        print(Fore.CYAN + f"Created TCP folder: {tcp_folder}")

        # Generate UDP samples folder output
        udp_folder = os.path.join(output, "udp", str(step))
        os.makedirs(udp_folder, exist_ok=True)
        print(Fore.CYAN + f"Created UDP folder: {udp_folder}")
        
    expression = ""
    if server == "dazn":
        expression = load_regex_patterns(path=os.path.join("meta", server, "servers", "linear.rex"))
        print(Fore.GREEN + "Loaded regex patterns for DAZN.")
    if server == "nowtv":
        # TODO
        pass
    if server == "netflix":
        # TODO
        pass
    
    for test in os.listdir(folder):
        print(Fore.YELLOW + f"Processing test: {test} in {folder}...")
        
        # TCP
        tcp_complete = pandas.read_csv(os.path.join(folder, test, LOG_TCP_COMPLETE), sep=" ")
        tcp_periodic = pandas.read_csv(os.path.join(folder, test, LOG_TCP_PERIODIC), sep=" ")
        
        # UDP
        udp_complete = pandas.read_csv(os.path.join(folder, test, LOG_UDP_COMPLETE), sep=" ")
        udp_periodic = pandas.read_csv(os.path.join(folder, test, LOG_UDP_PERIODIC), sep=" ")
        
        # HTTP
        har_complete = pandas.read_csv(os.path.join(folder, test, LOG_HAR_COMPLETE), sep=" ")
        
        # Streaming periods
        intervals = streaming_periods(path=os.path.join(folder, test, LOG_BOT_COMPLETE))
        print(Fore.GREEN + f"Found {len(intervals)} streaming periods for test: {test}.")
        
        for interval in intervals:
            ts = interval[0]
            te = interval[1]
            
            # Filter log_har_complete, log_tcp_complete, and log_udp_complete according
            # to the current streaming period
            filtered_data = {
                "har": har_complete[(har_complete["ts"] <= te) & (har_complete["te"] >= ts)],
                "tcp": tcp_complete[(tcp_complete["ts"] <= te) & (tcp_complete["te"] >= ts)],
                "udp": udp_complete[(udp_complete["ts"] <= te) & (udp_complete["te"] >= ts)],
            }

            # Apply the matches function to filter flows associated with a HAS server
            tcp_has_flows = filtered_data["tcp"][filtered_data["tcp"]["cname"].apply(lambda cn: matches(cname=cn, expressions=expression))]
            udp_has_flows = filtered_data["udp"][filtered_data["udp"]["cname"].apply(lambda cn: matches(cname=cn, expressions=expression))]

            # Get corresponding periodic logs for TCP and UDP flows
            tcp_has_bins = tcp_periodic[tcp_periodic["id"].isin(tcp_has_flows["id"])]
            udp_has_bins = udp_periodic[udp_periodic["id"].isin(udp_has_flows["id"])]

            # Calculate total bins and ratio of TCP/UDP bins
            total_bins = len(tcp_has_bins) + len(udp_has_bins)

            if total_bins == 0:
                print(Fore.RED + f"No valid TCP/UDP bins found for period {ts} to {te}. Skipping...")
                continue
            
            tcp_ratio  = len(tcp_has_bins) / total_bins
            udp_ratio  = len(udp_has_bins) / total_bins
            
            bins     = None
            protocol = None
            if tcp_ratio >= 0.9:
                bins     = tcp_has_bins
                protocol = Protocol.TCP
            if udp_ratio >= 0.9:
                bins     = udp_has_bins
                protocol = Protocol.UDP
                
            for step in steps:
                if bins is None:
                    continue
                samples  = sample_bins(bins=bins, http=filtered_data["har"], protocol=protocol, step=step, ts=ts, te=te)
                title    = str(uuid.uuid4())
                if protocol == Protocol.TCP:
                    layer = "tcp"
                if protocol == Protocol.UDP:
                    layer = "udp"
                samples.to_csv(os.path.join(output, layer, str(step), str(title)), sep=" ", index=False)
                print(Fore.GREEN + f"Saved samples for protocol {layer}, step {step}, title {title}.")

    for step in steps:
        # Rename all TCP-based HAS samples
        tcp_folder = os.path.join(output, "tcp", str(step))
        for num, out in enumerate(os.listdir(tcp_folder)):
                src = os.path.join(tcp_folder, out)
                dst = os.path.join(tcp_folder, f"sample-{num}")
                os.rename(src, dst)
                print(Fore.YELLOW + f"Renamed TCP sample {out} to sample-{num}.")
                
    for step in steps:
        # Rename all UDP-based HAS samples
        udp_folder = os.path.join(output, "udp", str(step))
        for num, out in enumerate(os.listdir(udp_folder)):
                src = os.path.join(udp_folder, out)
                dst = os.path.join(udp_folder, f"sample-{num}")
                os.rename(src, dst)
                print(Fore.YELLOW + f"Renamed UDP sample {out} to sample-{num}.")
                
if __name__ == "__main__":
    main()
    print("\n")