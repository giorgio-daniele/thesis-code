# import os
# import re
# import pandas

# # Define file path and regex patterns
# PATH:     str              = "dazn/long-play/1500kbits/tests/test-22/log_tcp_complete"
# regexs:   list[str]        = [r'\b[a-zA-Z0-9-]*dazn[a-zA0-9-]*\.(com|net)\b']
# patterns: list[re.Pattern] = re.compile('|'.join(regexs))

# def match_row(row: pandas.Series) -> bool:
#     return bool(patterns.search(str(row["cn"])))

# def extract_cname(record: dict) -> str:
#     conn_type = record.get("con_t", "-")
#     if conn_type == 8912:
#         return record.get("c_tls_SNI", "-")
#     elif conn_type == 1:
#         return record.get("http_hostname", "-")
#     else:
#         return record.get("fqdn", "-")

# # Generate the frame
# frame: pandas.DataFrame = pandas.read_csv(PATH, sep=" ")

# # Add canonical name column
# frame["cn"] = frame.apply(extract_cname, axis=1)

# # Filter matching rows
# matches: pandas.DataFrame = frame[frame.apply(match_row, axis=1)].sort_values(by="ts").reset_index(drop=True)

# # Filter for playback related flows
# cores: pandas.DataFrame = matches[matches["cn"].str.contains("drm|api.playback", regex=True, case=True)].sort_values(by="ts")

# # Define time delta for grouping
# delta = 10 * 1000  # 10 seconds in milliseconds

# # Grouping flows by time proximity (within delta)
# curr:    float = None
# periods: list[pandas.DataFrame] = []
# records: list[pandas.Series]    = []

# for _, record in cores.iterrows():
#     if curr is None:
#         curr = float(record["ts"])
#         records.append(record)
#         continue

#     if float(record["ts"]) - curr <= delta:
#         records.append(record)
#         curr = float(record["ts"])
#     else:
#         periods.append(pandas.DataFrame(records))
#         records = [record]
#         curr = float(record["ts"])

# # Don't forget to append the last group if there are remaining records
# if records:
#     periods.append(pandas.DataFrame(records))

# # Create a list of streaming periods based on timestamp comparisons
# streamings: list[pandas.DataFrame] = []
# for period in periods:
#     current = period["ts"].astype(float).min()
#     streaming = matches[matches["ts"] >= current]
#     streamings.append(streaming)

# # Precompute the minimum ts for each streaming period
# min_values = [float(streamings[i]["ts"].min()) for i in range(1, len(streamings))]

# # Filter out rows in each streaming frame based on the next frame's minimum ts value
# for i in range(len(streamings) - 1):
#     limit = min_values[i]
#     streamings[i] = streamings[i][streamings[i]["ts"] < limit]

# # Set display options to align columns and show all rows
# pandas.set_option("display.max_rows", None)

# # Print filtered streaming frames with aligned columns
# for streaming in streamings:
#     # Align the values after = (columns) for clean output
#     print(streaming)
#     print()

import re
import argparse
import pandas as pd

regexs: list[str]   = [r"\b[a-zA-Z0-9-]*dazn[a-zA0-9-]*\.(com|net)\b"]
pattern: re.Pattern = re.compile("|".join(regexs))

delta = 10 * 1000

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", required=True)
    parser.add_argument("--log_tcp_complete", required=True)
    parser.add_argument("--log_udp_complete", required=True)

    args = parser.parse_args()

    # Get the arguments
    return args.folder, args.log_tcp_complete, args.log_udp_complete

def main():
    
    folder, log_tcp_complete, log_udp_complete = args()
    
    # Generate the frames
    tcom_frame = pd.read_csv(log_tcp_complete, sep=" ")
    ucom_frame = pd.read_csv(log_udp_complete, sep=" ")
    
    # Sort flows by starting
    s_tcom_frame = tcom_frame.sort_values(by="ts", kind="quicksort")
    s_ucom_frame = ucom_frame.sort_values(by="ts", kind="quicksort")
    
    # Select DAZN flows
    f_tcom_frame = s_tcom_frame[s_tcom_frame["cn"].str.contains(pattern, regex=True, na=True)]
    f_ucom_frame = s_ucom_frame[s_ucom_frame["cn"].str.contains(pattern, regex=True, na=True)]
    
    # Select flows that have been observed to signal a playback
    delimiters = f_tcom_frame[f_tcom_frame["cn"].str.contains("drm|api.playback", regex=True, case=True)]
    
    current: float = None
    periods: list[pd.DataFrame] = []
    records: list[pd.Series]    = []
    
    for num, record in delimiters.iterrows():
        if current is None:
            current = float(record["ts"])
            records.append(record)
            continue

        if float(record["ts"]) - current <= delta:
            records.append(record)
            current = float(record["ts"])
        else:
            periods.append(pd.DataFrame(records))
            records = [record]
            current = float(record["ts"])
    if records:
        periods.append(pd.DataFrame(records))
        

    streaming_flows: list[(pd.DataFrame, pd.DataFrame)] = []
    
    for period in periods:
        current = period["ts"].astype(float).min()
        tcp_flows = f_tcom_frame[f_tcom_frame["ts"] >= current]
        udp_flows = f_ucom_frame[f_ucom_frame["ts"] >= current]
        streaming_flows.append((tcp_flows, udp_flows))
        
    min_values = [(
            float(streaming_flows[i][0]["ts"].min()),  # Minimum ts for tcp
            float(streaming_flows[i][1]["ts"].min())   # Minimum ts for udp
        ) for i in range(1, len(streaming_flows))]

    for i in range(len(streaming_flows) - 1):
        tcp_limit, udp_limit = min_values[i]
        tcp_flows, udp_flows = streaming_flows[i]
        streaming_flows[i] = (
            tcp_flows[tcp_flows["ts"] < tcp_limit],
            udp_flows[udp_flows["ts"] < udp_limit]
        )
        
    # Set display options to align columns and show all rows
    pd.set_option("display.max_rows", None)

    for tcp_flows, udp_flows in streaming_flows:

        # Add a protocol column to each DataFrame
        tcp_flows = tcp_flows.assign(proto="tcp")
        udp_flows = udp_flows.assign(proto="udp")

        # Select and concatenate the relevant columns
        result = pd.concat([tcp_flows[["proto", "c_ip", "c_port", "s_ip", "s_port", "cn", "ts", "te"]],
                            udp_flows[["proto", "c_ip", "c_port", "s_ip", "s_port", "cn", "ts", "te"]]])
        
        output = result.sort_values(by="ts")
        
        print(output.to_string(index=False))
        print("\n\n")
    return

main()

        # # Have frames
        # tcom_frame = pd.read_csv(tcom_files_chunk[i], sep=" ", compression="gzip")
        # ucom_frame = pd.read_csv(ucom_files_chunk[i], sep=" ", compression="gzip")
        
        # # Get the CNAMEs
        # tcom_frame[CNAME] = tcom_frame.apply(extract_cname_tcp_flow)
        # ucom_frame[CNAME] = ucom_frame.apply(extract_cname_udp_flow)

        # # Sort flows by starting
        # s_tcom_frame = tcom_frame.sort_values(by=TCP_TS, kind="quicksort")
        # s_ucom_frame = ucom_frame.sort_values(by=UDP_TS, kind="quicksort")

        # # Select DAZN flows
        # f_tcom_frame = s_tcom_frame[s_tcom_frame[CNAME].str.contains(pattern, regex=True, na=True)]
        # f_ucom_frame = s_ucom_frame[s_ucom_frame[CNAME].str.contains(pattern, regex=True, na=True)]
        
        # with file_lock:
        #     with open(output, "a") as f:
        #         message = f"log_tcp_complete at {os.path.basename(tcom_files_chunk[i])}"
        #         f.write(f_tcom_frame)
        #         message = f"log_udp_complete at {os.path.basename(ucom_files_chunk[i])}"
        #         f.write(u_tcom_frame)

# def main():
#     print("Initializing process...")
#     pandas.set_option("display.max_rows", None)

#     months = ["01-Jan", "02-Feb", "03-Mar", "04-Apr", "05-May", 
#               "06-Jun", "07-Jul", "08-Aug", "09-Sep", "10-Oct", "11-Nov"]
    
#     year = os.path.basename(ROOT)
    
#     ###########################
#     #  Process Pool Executor  #
#     ###########################
#     with ProcessPoolExecutor(max_workers=200) as executor:
#         futures = []
        
#         # Loop through months and submit tasks
#         for month in months[3:5]:
#             path = os.path.join(ROOT, month)
#             outs = os.listdir(path)
#             for num, out in enumerate(outs):
#                 file = os.path.join(path, out, LOG_TCP_COMPLETE)
#                 print(f"Submitting file: {file}, month = {month}, year = {year}")
#                 futures.append(executor.submit(read_frame, file))
        
#         ############################
#         #  Handling Future Results #
#         ############################
#         print("Waiting for tasks to complete...")
#         for future in as_completed(futures):
#             try:
#                 # Process the result
#                 streamings = future.result()
#                 if len(streamings) == 0:
#                     continue
#                 name = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
#                 for streaming in streamings:
#                     # Writing a file
#                     print_file(frame=streaming, name=name)
#             except Exception as e:
#                 print(f"Error processing the future: {e}")

#     print("All tasks completed.")