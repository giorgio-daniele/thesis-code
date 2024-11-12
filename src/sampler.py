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

def process_tcp(data: pandas.DataFrame, 
                meta: pandas.DataFrame, ts: float, te: float, delta: float):
    
    records = []
    
    for ti in range(ts, te, delta):
        tj = ti + delta
        
        # Select all bins that overlap [ts; te]
        bins = data[(data["ts"] <= tj) & (data["te"] >= ti)].copy()

        # Select all reqs that overlap [ts; te]
        reqs = meta[(meta["ts"] <= tj) & (meta["te"] >= ti)].copy()

        # Remove anything not well specified
        reqs = reqs.dropna(subset=["mime"])

        if bins.empty:
            records.append([ti, tj, delta] + [0] * (len(TCP_METRICS) - 3))
            continue

        # Redefine each bin lower and upper bound
        bins["rel_ts"] = numpy.maximum(bins["ts"], ti)
        bins["rel_te"] = numpy.minimum(bins["te"], tj)
        bins["factor"] = ((bins["rel_te"] - bins["rel_ts"]) / (bins["te"] - bins["ts"]).replace(0, 1))

        """
        =============================
        =      Temporal stats       =
        =============================
        """
        
        # Have the bins as intervals
        ints = bins[["rel_ts", "rel_te"]].values.tolist()

        # Sum the overall time within delta
        span = sum(end - start for start, end in merge_intervals(ints))

        # Compute idle time (that how much delta time is filled)
        idle = delta - span


        # Compute the average  span of bins
        avg_span = (bins["rel_te"] - bins["rel_ts"]).mean()

        # Compute the maximum span of bins
        max_span = (bins["rel_te"] - bins["rel_ts"]).max()

        # Compute the minimum span of bins
        min_span = (bins["rel_te"] - bins["rel_ts"]).min()

        # Compute the standard span of bins
        std_span = (bins["rel_te"] - bins["rel_ts"]).std()

        """
        =============================
        =     Volumetric stats      =
        =============================
        """

        # Client side (volumetric statistics)
        c_ack_cnt   = float((bins["c_ack_cnt"]   * bins["factor"]).sum())  
        c_ack_cnt_p = float((bins["c_ack_cnt_p"] * bins["factor"]).sum())  
        c_bytes_all = float((bins["c_bytes_all"] * bins["factor"]).sum())  

        # Server side (volumetric statistics)
        s_ack_cnt   = float((bins["s_ack_cnt"]   * bins["factor"]).sum())  
        s_ack_cnt_p = float((bins["s_ack_cnt_p"] * bins["factor"]).sum())  
        s_bytes_all = float((bins["s_bytes_all"] * bins["factor"]).sum()) 

        """
        =============================
        =   Ground Truth (Video)    =
        =============================
        """

        # Get video-related info
        video = reqs[reqs["mime"].str.contains("video")]

        avg_video       = 0
        video_sequence  = []

        if video is not None:
            # Replace NaN values with 0
            video = video.fillna(0)

            # Get the average resolution
            avg_video = video["video_rate"].astype(float).mean()
                        
            # Generate a list of tuples in which there are
            # (ts, te, video_rate), when the request has
            # started, when the request has finished and
            # the value of such request in string format
            
            sequences = []
            for num, rec in video.iterrows():
                sequence = str(rec["ts"]) + "-" + str(rec["te"]) + "=" + str(rec["video_rate"])
                sequences.append(sequence)
            video_sequence = " ".join(sequences)
            

        """
        =============================
        =   Ground Truth (Audio)    =
        =============================
        """

        # Get video-related info
        audio = reqs[reqs["mime"].str.contains("audio")]

        avg_audio       = 0
        audio_sequence  = []

        if audio is not None:
            # Replace NaN values with 0
            audio = audio.fillna(0)

            # Get the average resolution
            avg_audio = audio["audio_rate"].astype(float).mean()
            
            # Generate a list of tuples in which there are
            # (ts, te, video_rate), when the request has
            # started, when the request has finished and
            # the value of such request in string format
            
            sequences = []
            for num, rec in audio.iterrows():
                sequence = str(rec["ts"]) + ":" + str(rec["te"]) + "=" + str(rec["audio_rate"])
                sequences.append(sequence)
            audio_sequence = " ".join(sequences)
            

        records.append([
            ti, tj, idle,
            # Collected metrics from Tstat
            avg_span, std_span,
            max_span, min_span,
            c_ack_cnt, c_ack_cnt_p, c_bytes_all,
            s_ack_cnt, s_ack_cnt_p, s_bytes_all,
            # Groud truth
            avg_video, avg_audio,
            video_sequence,
            audio_sequence
        ])

    metrics = pandas.DataFrame(records, columns=TCP_METRICS)

    if not metrics.empty:
        first_ts = metrics["ts"].iloc[0]
        metrics["ts"] -= first_ts
        metrics["te"] -= first_ts
            
    return metrics

def process_udp(data: pandas.DataFrame, 
                meta: pandas.DataFrame, ts: float, te: float, delta: float):

    records = []
    
    for ti in range(ts, te, delta): 
        tj = ti + delta
        
        # Select all bins that overlap [ts; te]
        bins = data[(data["ts"] <= tj) & (data["te"] >= ti)].copy()

        # Select all reqs that overlap [ts; te]
        reqs = meta[(meta["ts"] <= tj) & (meta["te"] >= ti)].copy()

        # Remove anything not well specified
        reqs = reqs.dropna(subset=["mime"])

        if bins.empty:
            records.append([ti, tj, delta] + [0] * (len(UDP_METRICS) - 3))
            continue

        # Redefine each bin lower and upper bound
        bins["rel_ts"] = numpy.maximum(bins["ts"], ti)
        bins["rel_te"] = numpy.minimum(bins["te"], tj)
        bins["factor"] = ((bins["rel_te"] - bins["rel_ts"]) / (bins["te"] - bins["ts"]).replace(0, 1))

        """
        =============================
        =      Temporal stats       =
        =============================
        """
        
        # Have the bins as intervals
        ints = bins[["rel_ts", "rel_te"]].values.tolist()

        # Sum the overall time within delta
        span = sum(end - start for start, end in merge_intervals(ints))

        # Compute idle time (that how much delta time is filled)
        idle = delta - span


        # Compute the average  span of bins
        avg_span = (bins["rel_te"] - bins["rel_ts"]).mean()

        # Compute the maximum span of bins
        max_span = (bins["rel_te"] - bins["rel_ts"]).max()

        # Compute the minimum span of bins
        min_span = (bins["rel_te"] - bins["rel_ts"]).min()

        # Compute the standard span of bins
        std_span = (bins["rel_te"] - bins["rel_ts"]).std()

        """
        =============================
        =     Volumetric stats      =
        =============================
        """

        # Client side (volumetric statistics)
        c_bytes_all = float((bins["c_bytes_all"] * bins["factor"]).sum())  

        # Server side (volumetric statistics)
        s_bytes_all = float((bins["s_bytes_all"] * bins["factor"]).sum()) 


        """
        =============================
        =   Ground Truth (Video)    =
        =============================
        """

        # Get video-related info
        video = reqs[reqs["mime"].str.contains("video/mp4")]
    
        avg_video      = 0
        video_sequence = []
        
        if video is not None:
            # Replace NaN values with 0
            video = video.fillna(0)

            # Get the average resolution
            avg_video = video["video_rate"].astype(float).mean()
            
            # Generate a list of tuples in which there are
            # (ts, te, video_rate), when the request has
            # started, when the request has finished and
            # the value of such request in string format
            
            sequences = []
            for num, rec in video.iterrows():
                sequence = str(rec["ts"]) + "-" + str(rec["te"]) + "=" + str(rec["video_rate"])
                sequences.append(sequence)
            video_sequence = " ".join(sequences)
    

        """
        =============================
        =   Ground Truth (Audio)    =
        =============================
        """

        # Get video-related info
        audio = reqs[reqs["mime"].str.contains("audio/mp4")]

        avg_audio      = 0
        audio_sequence = []

        if audio is not None:
            # Replace NaN values with 0
            audio = audio.fillna(0)

            # Get the average resolution
            avg_audio = audio["audio_rate"].astype(float).mean()
            
            # Generate a list of tuples in which there are
            # (ts, te, video_rate), when the request has
            # started, when the request has finished and
            # the value of such request in string format
            
            sequences = []
            for num, rec in audio.iterrows():
                sequence = str(rec["ts"]) + ":" + str(rec["te"]) + "=" + str(rec["audio_rate"])
                sequences.append(sequence)
            audio_sequence = " ".join(sequences)
            

        records.append([
            ti, tj, idle,
            # Collected metrics from Tstat
            avg_span, std_span,
            max_span, min_span,
            c_bytes_all,
            s_bytes_all,
            # Groud truth
            avg_video, avg_audio,
            video_sequence,
            audio_sequence
        ])

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

#############################
#           MAIN            #
#############################

def main():
    folder, server = args()

    print(f"SAMPLER is running on [{folder}] for service [{server}]...")

    # Define the steps
    #steps = [i * 1000 for i in range(1, 3, 1)]
    steps  = [i * 1000 for i in range(1, 11, 1)]

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
            
            if udp.empty:
                print("UDP DataFrame is empty after filtering on 'ts' and 'te'")

            # Select all bins associated to linear flows
            udp_fitered = udp_fitered[udp_fitered["cn"].apply(lambda cn: matches(cname=cn, expressions=regexs))]
            
            if len(tcp_filtered) + len(udp_fitered) == 0:
                continue
            
            """
            ==================================
            =       Choose if TCP based      =
            ==================================
            """


            # TCP-based streaming period
            if len(tcp_filtered) / (len(tcp_filtered) + len(udp_fitered)) >= 0.9:
                for step in steps:
                    res = process_tcp(data=tcp_filtered, meta=har, ts=ts, te=te, delta=step)
                    tsp = str(uuid.uuid4())
                    out = os.path.join(folder, "samples", "tcp", str(step), str(tsp))
                    res.to_csv(out, sep=" ", index=False)
                    
            """
            ==================================
            =       Choose if UDP based      =
            ==================================
            """

            # UDP-based streaming period
            if len(udp_fitered) / (len(tcp_filtered) + len(udp_fitered)) >= 0.9:
                for step in steps:
                    tsp = str(uuid.uuid4())
                    res = process_udp(data=udp_fitered, meta=har, ts=ts, te=te, delta=step)
                    out = os.path.join(folder, "samples", "udp", str(step), str(tsp))
                    res.to_csv(out, sep=" ", index=False)
                    
    for step in steps:
        # Rename all TCP samples
        dir = os.path.join(folder, "samples", "tcp", str(step))
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
            
    print()

if __name__ == "__main__":
    print("-" * 50)
    main()
    print("-" * 50)