import os
import time
import numpy
import uuid
import pandas
import argparse

from collections import Counter
from lib.generic import *

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


def format_time(milliseconds: float) -> str:
    ss = int(milliseconds // 1000)  # Convert milliseconds to full seconds
    ms = int(milliseconds % 1000)   # Remaining milliseconds
    return f"{ss}s;{ms}ms"

def process_media_data(requests: pandas.DataFrame, mime_type: str, ts: float):
    # Filter the data based on mime type
    media = requests[requests["mime"].str.contains(mime_type)]

    # Initialize defaults
    mean = 0.0
    http = "-"

    # Process the media data if available
    if not media.empty:
        mean = media[f"{mime_type}_rate"].dropna().astype(float).mean()
        http = "_".join(
            f"{record['ts'] - ts}#{record['ts'] - ts}#{record[f'{mime_type}_rate']}"
            for _, record in media.iterrows())
    
    return mean, http

def sample_tcp(data: pandas.DataFrame, 
               meta: pandas.DataFrame, ts: float, te: float, delta: float):
    
    records = []
    
    for ti in range(int(ts), int(te - delta), int(delta)):
        tj = ti + delta
        
        # Select all bins that overlap [ti; tj]
        bins = data[(data["ts"] <= tj) & (data["te"] >= ti)].copy()

        # Select all reqs that overlap [ti; tj]
        reqs = meta[(meta["ts"] <= tj) & (meta["te"] >= ti)].copy()

        # Remove anything not well specified
        reqs = reqs.dropna(subset=["mime"])

        if bins.empty:
            records.append([ti, tj, delta] + [0] * (len(TCP_METRICS) - 5) + ["-", "-"])
            continue

        # Compute the relative start of each bin
        bins["rel_ts"] = numpy.maximum(bins["ts"], ti)
        
        # Compute the relative end of each bin
        bins["rel_te"] = numpy.minimum(bins["te"], tj)

        # Compute the overlap percentage
        bins["factor"] = (bins["rel_te"] - bins["rel_ts"]) / (bins["te"] - bins["ts"]).replace(0, 1)

        """
        =============================
        =      Temporal stats       =
        =============================
        """
        # Compute total time span in this delta (merged intervals)
        ints = bins[["rel_ts", "rel_te"]].values.tolist()
        span = sum(end - start for start, end in merge_intervals(ints))

        # Compute idle time (the amount of delta time not filled)
        idle = delta - span

        # Compute stats for bin durations
        avg_span = (bins["rel_te"] - bins["rel_ts"]).mean()
        max_span = (bins["rel_te"] - bins["rel_ts"]).max()
        min_span = (bins["rel_te"] - bins["rel_ts"]).min()
        std_span = (bins["rel_te"] - bins["rel_ts"]).std()

        """
        =============================
        =     Volumetric stats      =
        =============================
        """
        # Volumetric statistics (scaled by factor)
        c_ack_cnt    = float((bins["c_ack_cnt"]    * bins["factor"]).sum())
        c_ack_cnt_p  = float((bins["c_ack_cnt_p"]  * bins["factor"]).sum())
        c_bytes_all  = float((bins["c_bytes_all"]  * bins["factor"]).sum())
        c_bytes_uniq = float((bins["c_bytes_uniq"] * bins["factor"]).sum())
        # c_packs_retx = float((bins["c_packs_retx"] * bins["factor"]).sum())
        # c_bytes_retx = float((bins["c_bytes_retx"] * bins["factor"]).sum())

        s_ack_cnt    = float((bins["s_ack_cnt"]    * bins["factor"]).sum())
        s_ack_cnt_p  = float((bins["s_ack_cnt_p"]  * bins["factor"]).sum())
        s_bytes_all  = float((bins["s_bytes_all"]  * bins["factor"]).sum())
        s_bytes_uniq = float((bins["s_bytes_uniq"] * bins["factor"]).sum())
        # s_packs_retx = float((bins["s_packs_retx"] * bins["factor"]).sum())
        # s_bytes_retx = float((bins["s_bytes_retx"] * bins["factor"]).sum())

        """
        =============================
        =     Ground Truth (A/V)    =
        =============================
        """
        # Process media data (video/audio ground truth)
        avg_video, video_sequence = process_media_data(reqs, "video", ts)
        avg_audio, audio_sequence = process_media_data(reqs, "audio", ts)
            
        # Append the computed values for the current delta (ti, tj)
        records.append([
            ti, tj, idle,
            
            # Tstat data
            avg_span, 
            std_span, 
            max_span, 
            min_span,
            
            # Server to client side
            c_ack_cnt, c_ack_cnt_p, c_bytes_all, c_bytes_uniq,
            
            # Client to server side
            s_ack_cnt, s_ack_cnt_p, s_bytes_all, s_bytes_uniq,
            
            # Ground Truth audio
            avg_video, avg_audio,
            # Ground Truth video
            video_sequence, audio_sequence
        ])
        
    # Create a pandas DataFrame from the collected records
    metrics = pandas.DataFrame(records, columns=TCP_METRICS)

    if not metrics.empty:
        first_ts = metrics["ts"].iloc[0]
        metrics["ts"] -= first_ts
        metrics["te"] -= first_ts
        
    return metrics


def sample_udp(data: pandas.DataFrame, 
               meta: pandas.DataFrame, ts: float, te: float, delta: float):
    
    records = []
    
    for ti in range(int(ts), int(te - delta), int(delta)):
        tj = ti + delta
        
        # Select all bins that overlap [ti; tj]
        bins = data[(data["ts"] <= tj) & (data["te"] >= ti)].copy()

        # Select all reqs that overlap [ti; tj]
        reqs = meta[(meta["ts"] <= tj) & (meta["te"] >= ti)].copy()

        # Remove anything not well specified
        reqs = reqs.dropna(subset=["mime"])

        if bins.empty:
            records.append([ti, tj, delta] + [0] * (len(UDP_METRICS) - 5) + ["-", "-"])
            continue

        # Compute the relative start of each bin
        bins["rel_ts"] = numpy.maximum(bins["ts"], ti)
        
        # Compute the relative end of each bin
        bins["rel_te"] = numpy.minimum(bins["te"], tj)

        # Compute the overlap percentage
        bins["factor"] = (bins["rel_te"] - bins["rel_ts"]) / (bins["te"] - bins["ts"]).replace(0, 1)

        """
        =============================
        =      Temporal stats       =
        =============================
        """
        # Compute total time span in this delta (merged intervals)
        ints = bins[["rel_ts", "rel_te"]].values.tolist()
        span = sum(end - start for start, end in merge_intervals(ints))

        # Compute idle time (the amount of delta time not filled)
        idle = delta - span

        # Compute stats for bin durations
        avg_span = (bins["rel_te"] - bins["rel_ts"]).mean()
        max_span = (bins["rel_te"] - bins["rel_ts"]).max()
        min_span = (bins["rel_te"] - bins["rel_ts"]).min()
        std_span = (bins["rel_te"] - bins["rel_ts"]).std()

        """
        =============================
        =     Volumetric stats      =
        =============================
        """
        # Volumetric statistics (scaled by factor)
        c_bytes_all  = float((bins["c_bytes_all"]  * bins["factor"]).sum())
        
        s_bytes_all  = float((bins["s_bytes_all"]  * bins["factor"]).sum())

        """
        =============================
        =     Ground Truth (A/V)    =
        =============================
        """
        # Process media data (video/audio ground truth)
        avg_video, video_sequence = process_media_data(reqs, "video", ts)
        avg_audio, audio_sequence = process_media_data(reqs, "audio", ts)
            
        # Append the computed values for the current delta (ti, tj)
        records.append([
            ti, tj, idle,
            
            # Tstat data
            avg_span, 
            std_span, 
            max_span, 
            min_span,
            
            # Server to client side
            c_bytes_all,
            
            # Client to server side
            s_bytes_all,
            
            # Ground Truth audio
            avg_video, avg_audio,
            # Ground Truth video
            video_sequence, audio_sequence
        ])
        
    # Create a pandas DataFrame from the collected records
    metrics = pandas.DataFrame(records, columns=UDP_METRICS)

    if not metrics.empty:
        first_ts = metrics["ts"].iloc[0]
        metrics["ts"] -= first_ts
        metrics["te"] -= first_ts
        
    return metrics
        

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", required=True)
    parser.add_argument("--server", required=True, choices=SERVERS)

    # Get the arguments
    return parser.parse_args().folder, parser.parse_args().server

def main():
    folder, server = args()

    # Define the steps
    steps = [i * 1000 for i in range(1, 11, 1)]

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

    # Remove any previous output
    dir = os.path.join(folder, "samples")
    cmd = f"""rm -rf {dir}"""
    os.system(cmd)

    # Create a new output for TCP
    for step in steps:
        dir = os.path.join(folder, "samples", "tcp", str(step))
        cmd = f"""mkdir -p {dir}"""
        os.system(cmd)

    # Create a new output for UDP
    for step in steps:
        dir = os.path.join(folder, "samples", "udp", str(step))
        cmd = f"""mkdir -p {dir}"""
        os.system(cmd)

    # Load regexs for filtering linear streams
    regexs = load_regex_patterns(path=os.path.join(METADATA, "servers", "linear.rex"))
    
    for test_name in os.listdir(os.path.join(folder, "tests")):
        dir = os.path.join(folder, "tests")
        har_file = os.path.join(dir, test_name, LOG_HAR_COMPLETE)
        bot_file = os.path.join(dir, test_name, LOG_BOT_COMPLETE)
        tcp_file = os.path.join(dir, test_name, LOG_TCP_PERIODIC)
        udp_file = os.path.join(dir, test_name, LOG_UDP_PERIODIC)

        tcp = pandas.read_csv(tcp_file, sep=" ")
        udp = pandas.read_csv(udp_file, sep=" ")
        har = pandas.read_csv(har_file, sep=" ")

        evs = periods(path=bot_file)
        
        for evn in evs:
            ts, te = evn[0], evn[1]
            
            print(f"Processing period from {int(ts / 1000):2d} seconds to {int(te / 1000):2d} seconds")
            
            """
            =============================
            =    Processing TCP Layer   =
            =============================
            """

            # Select all bins that overlap [ts; te]
            tcp_filtered = tcp[(tcp["ts"] <= te) & (tcp["te"] >= ts)]

            # Select all bins associated to linear flows
            tcp_filtered = tcp_filtered[tcp_filtered["cn"].apply(lambda cn: matches(cname=cn, expressions=regexs))]

            """
            =============================
            =    Processing UDP Layer   =
            =============================
            """
        
            # Select all bins that overlap [ts; te]
            udp_fitered = udp[(udp["ts"] <= te) & (udp["te"] >= ts)]
            
            # Select all bins associated to linear flows
            udp_fitered = udp_fitered[udp_fitered["cn"].apply(lambda cn: matches(cname=cn, expressions=regexs))]
            
            # Compute the total
            tot = len(tcp_filtered) + len(udp_fitered)
            if tot == 0:
                continue
            
            """
            ==================================
            =       Choose if TCP based      =
            ==================================
            """

            # TCP-based streaming period
            if len(tcp_filtered) / tot >= 0.85:
                for step in steps:
                    print(f"\tUsing {step} as stepping interval")
                    frame  = sample_tcp(data=tcp_filtered, meta=har, ts=ts, te=te, delta=step)
                    title  = str(uuid.uuid4())
                    output = os.path.join(folder, "samples", "tcp", str(step), str(title))
                    frame.to_csv(output, sep=" ", index=False)
                    
            """
            ==================================
            =       Choose if UDP based      =
            ==================================
            """

            # UDP-based streaming period
            if len(udp_fitered) / tot >= 0.85:
                for step in steps:
                    print(f"\tUsing {step} as stepping interval")
                    frame  = sample_udp(data=udp_fitered, meta=har, ts=ts, te=te, delta=step)
                    title  = str(uuid.uuid4())
                    output = os.path.join(folder, "samples", "udp", str(step), str(title))
                    frame.to_csv(output, sep=" ", index=False)
    
    for step in steps:
        dir = os.path.join(folder, "samples", "tcp", str(step))
        # Rename all TCP samples
        for num, out in enumerate(os.listdir(dir)):
            src = os.path.join(dir, out)
            dst = os.path.join(dir, f"sample-{num}")
            cmd = f"mv {src} {dst}"
            os.system(cmd)

        # Rename all UDP samples
        dir = os.path.join(folder, "samples", "udp", str(step))
        for num, out in enumerate(os.listdir(dir)):
            src = os.path.join(dir, out)
            dst = os.path.join(dir, f"sample-{num}")
            cmd = f"mv {src} {dst}"
            os.system(cmd)
            
    #print()

if __name__ == "__main__":
    #print("-" * 50)
    main()
    #print("-" * 50)