import os
import re
import pandas

# Define file path and regex patterns
PATH:     str              = "dazn/long-play/1500kbits/tests/test-22/log_tcp_complete"
regexs:   list[str]        = [r'\b[a-zA-Z0-9-]*dazn[a-zA0-9-]*\.(com|net)\b']
patterns: list[re.Pattern] = re.compile('|'.join(regexs))

def match_row(row: pandas.Series) -> bool:
    return bool(patterns.search(str(row["cn"])))

def extract_cname(record: dict) -> str:
    conn_type = record.get("con_t", "-")
    if conn_type == 8912:
        return record.get("c_tls_SNI", "-")
    elif conn_type == 1:
        return record.get("http_hostname", "-")
    else:
        return record.get("fqdn", "-")

# Generate the frame
frame: pandas.DataFrame = pandas.read_csv(PATH, sep=" ")

# Add canonical name column
frame["cn"] = frame.apply(extract_cname, axis=1)

# Filter matching rows
matches: pandas.DataFrame = frame[frame.apply(match_row, axis=1)].sort_values(by="ts").reset_index(drop=True)

# Filter for playback related flows
cores: pandas.DataFrame = matches[matches["cn"].str.contains("drm|api.playback", regex=True, case=True)].sort_values(by="ts")

# Define time delta for grouping
delta = 10 * 1000  # 10 seconds in milliseconds

# Grouping flows by time proximity (within delta)
curr:    float = None
periods: list[pandas.DataFrame] = []
records: list[pandas.Series]    = []

for _, record in cores.iterrows():
    if curr is None:
        curr = float(record["ts"])
        records.append(record)
        continue

    if float(record["ts"]) - curr <= delta:
        records.append(record)
        curr = float(record["ts"])
    else:
        periods.append(pandas.DataFrame(records))
        records = [record]
        curr = float(record["ts"])

# Don't forget to append the last group if there are remaining records
if records:
    periods.append(pandas.DataFrame(records))

# Create a list of streaming periods based on timestamp comparisons
streamings: list[pandas.DataFrame] = []
for period in periods:
    current = period["ts"].astype(float).min()
    streaming = matches[matches["ts"] >= current]
    streamings.append(streaming)

# Precompute the minimum ts for each streaming period
min_values = [float(streamings[i]["ts"].min()) for i in range(1, len(streamings))]

# Filter out rows in each streaming frame based on the next frame's minimum ts value
for i in range(len(streamings) - 1):
    limit = min_values[i]
    streamings[i] = streamings[i][streamings[i]["ts"] < limit]

# Set display options to align columns and show all rows
pandas.set_option("display.max_rows", None)

# Print filtered streaming frames with aligned columns
for streaming in streamings:
    # Align the values after = (columns) for clean output
    print(streaming)
    print()
