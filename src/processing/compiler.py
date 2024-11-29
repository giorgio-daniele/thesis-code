import os
import re
import json
import shutil
import pandas
import datetime
import argparse
import urllib.parse

from lib.generic import *
from lib.dazn    import DAZN_MANIFEST_VERSION_A
from lib.dazn    import DAZN_MANIFEST_VERSION_B

def archive_to_frame(start: float, har: str, server: str) -> pandas.DataFrame:
    records = []

    with open(har, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    # Define the manifest templates
    templates = {
        "dazn": {
            "version_a": DAZN_MANIFEST_VERSION_A,
            "version_b": DAZN_MANIFEST_VERSION_B
        },
        "sky": {
            # TODO
        },
        "now": {
            # TODO
        }
    }
    
    template_a = templates[server]["version_a"]
    template_b = templates[server]["version_b"]
        
    template = {
        "video": {**template_a["video"], **template_b["video"]},
        "audio": {**template_a["audio"], **template_b["audio"]}
    }
    
    for entry in data["log"]["entries"]:
        # Get the timestamp when the request has started
        ft = "%Y-%m-%dT%H:%M:%S.%fZ"
        ux = datetime.datetime(1970, 1, 1)
        ts = (datetime.datetime.strptime(entry["startedDateTime"], ft) - ux).total_seconds() * 1000
        ts = ts - (float(start))
        
        # Get the timestamp when the request has completed
        ds = ["blocked", "dns", "send", "wait", "receive", "ssl"]
        te = ts + sum(max(0, entry["timings"].get(k, 0)) for k in ds)
        
        # Get the MIME type
        mime = entry.get("response", {}).get("content", {}).get("mimeType", "")
        
        # Jump to next request if not an audio or video request
        if "video" not in mime and "audio" not in mime:
            continue
        
        # Parse the URL
        data = urllib.parse.urlparse(entry.get("request", {}).get("url", "-"))
        
        # Get information about the server from which
        # the client is downloading the content and
        # the path of the resource
        machine  = data.netloc
        resource = data.path
        
        video_rate = 0
        audio_rate = 0
        media_type = ""

        media = "video"
        if media in resource:
            for key in template[media]:
                if key in resource:
                    video_rate = str(template[media][key]["bitrate"])
                    media_type = media
                    
        media = "audio"
        if media in resource:
            for key in template[media]:
                if key in resource:
                    audio_rate = str(template[media][key]["bitrate"])
                    media_type = media
                    
        record = [ts, te, machine, video_rate, audio_rate, media_type]
        records.append(record)
        
    return pandas.DataFrame(records, columns=["ts", "te", "server", "video_rate", "audio_rate", "mime"])

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", required=True)
    parser.add_argument("--server", required=True, choices=SERVERS)

    # Get the arguments
    args = parser.parse_args()

    # Get the parameters
    return (args.folder, args.server)

def extract_cname_tcp(record: dict) -> str:
    conn_type = record.get("con_t", "-")
    if conn_type == 8912:
        return record.get("c_tls_SNI", "-")
    if conn_type == 1:
        return record.get("http_hostname", "-")
    return record.get("fqdn", "-")
    
def extract_cname_udp(record: dict) -> str:
    conn_type = record.get("con_t", "-")
    if conn_type == 27:
        return record.get("quic_SNI", "-")
    return record.get("fqdn", "-")

#############################
#           MAIN            #
#############################

def main():
    folder, server = args()

    print(f"COMPILER is running on [{folder}] for service [{server}]...")

    # Get Wireshark traces
    cap_files = fetch(folder=folder, prefix=LOG_NET_COMPLETE, suffix=CAP)
    # Get Streambot traces
    bot_files = fetch(folder=folder, prefix=LOG_BOT_COMPLETE, suffix=BOT)
    # Get Weblogger traces
    har_files = fetch(folder=folder, prefix=LOG_HAR_COMPLETE, suffix=HAR)

    print(f"    Data fetched from disk:")
    print(f"        .cap files: {len(cap_files)}")
    print(f"        .csv files: {len(bot_files)}")
    print(f"        .har files: {len(har_files)}")

    # Delete any previous tests
    os.system(command=f"rm -rf {os.path.dirname(folder)}/tests")

    # Have new directory for tests
    os.system(command=f"mkdir -p {os.path.dirname(folder)}/tests")

    for num, (cap, bot, har) in enumerate(zip(cap_files, bot_files, har_files), start=1):
        out = os.path.join(os.path.dirname(folder), "tests", f"test-{num}")

        # Run Tstat
        cmd = f"""{TSTAT_BINARY}              \
                -G {TSTAT_GLOBAL}             \
                -T {TSTAT_CONFIG} {cap}       \
                -s {out} > /dev/null"""
        os.system(cmd)

        cmd = f"""find {out} -mindepth 2      \
                -type f -exec mv -t {out} {{}} +"""
        os.system(cmd)

        cmd = f"""find "{out}"                \
                -type d -empty -exec rmdir {{}} +"""
        os.system(cmd)

        # Save Strambot file in out
        nme = os.path.basename(bot).rsplit('-', 1)[0]
        cmd = f"cp {bot} {os.path.join(out, nme)}"
        os.system(cmd)

        # Save Weblogger file in out
        nme = os.path.basename(har).rsplit('-', 1)[0]
        cmd = f"cp {har} {os.path.join(out, nme)}"
        os.system(cmd)

    for test in os.listdir(os.path.join(os.path.dirname(folder), "tests")):
        dir = os.path.join(os.path.dirname(folder), "tests", test)

        bot = pandas.read_csv(os.path.join(dir, LOG_BOT_COMPLETE), sep=" ")
        now = bot.iloc[0]["abs"]

        """
        =============================
        =    Processing TCP Layer   =
        =============================
        """

        com = pandas.read_csv(os.path.join(dir, LOG_TCP_COMPLETE), sep=" ")
        per = pandas.read_csv(os.path.join(dir, LOG_TCP_PERIODIC), sep=" ")

        # Rename coherently all the columns
        com.columns = [re.sub(r'[#:0-9]', '', col) for col in com.columns]
        per.columns = [re.sub(r'[#:0-9]', '', col) for col in per.columns]

        # Filter out LAN traffic
        com = com[~com["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_private or ipaddress.ip_address(ip).is_multicast)]
        per = per[~per["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_private or ipaddress.ip_address(ip).is_multicast)]

        # Rebase the timestamps
        com["ts"] = com["first"] - now
        com["te"] = com["last"]  - now

        # Generate a unique ID
        com["id"] = com[["s_ip", "s_port", "c_ip", "c_port"]].astype(str).agg("-".join, axis=1)

        # Generate server CNAME
        com["cn"] = com.apply(extract_cname_tcp, axis=1)

        # Rebase the timestamps
        per["ts"] = per["time_abs_start"] - now
        per["te"] = per["time_abs_start"] - now + per["bin_duration"]

        # Generate a unique ID
        per["id"] = per[["s_ip", "s_port", "c_ip", "c_port"]].astype(str).agg("-".join, axis=1)

        # Generate server CNAME
        per = per.merge(com[["id", "cn"]], on="id", how="left")


        # Save processed data
        com.to_csv(os.path.join(dir, LOG_TCP_COMPLETE), index=False, sep=" ")
        per.to_csv(os.path.join(dir, LOG_TCP_PERIODIC), index=False, sep=" ")


        """
        =============================
        =    Processing UDP Layer   =
        =============================
        """

        com = pandas.read_csv(os.path.join(dir, LOG_UDP_COMPLETE), sep=" ")
        per = pandas.read_csv(os.path.join(dir, LOG_UDP_PERIODIC), sep=" ")

        # Rename coherently all the columns
        com.columns = [re.sub(r'[#:0-9]', '', col) for col in com.columns]
        per.columns = [re.sub(r'[#:0-9]', '', col) for col in per.columns]

        # Filter out LAN traffic
        com = com[~com["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_private or ipaddress.ip_address(ip).is_multicast)]
        per = per[~per["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_private or ipaddress.ip_address(ip).is_multicast)]

        # Rebase the timestamps
        com["ts"] = com["s_first_abs"] - now
        com["te"] = com["s_first_abs"] - now + (com["s_durat"] * 1000)

        # Generate a unique ID
        com["id"] = com[["s_ip", "s_port", "c_ip", "c_port"]].astype(str).agg("-".join, axis=1)

        # Generate server CNAME
        com["cn"] = com.apply(extract_cname_udp, axis=1)

        # Rebase the timestamps
        per["ts"] = per["time_abs_start"] - now
        per["te"] = per["time_abs_start"] - now + per["bin_duration"]

        # Generate a unique ID
        per["id"] = per[["s_ip", "s_port", "c_ip", "c_port"]].astype(str).agg("-".join, axis=1)

        # Generate server CNAME
        per = per.merge(com[["id", "cn"]], on="id", how="left")

        # Save processed data
        com.to_csv(os.path.join(dir, LOG_UDP_COMPLETE), index=False, sep=" ")
        per.to_csv(os.path.join(dir, LOG_UDP_PERIODIC), index=False, sep=" ")
        
        """
        =============================
        =   Processing HTTP Layer   =
        =============================
        """

        com = archive_to_frame(start=now, har=os.path.join(dir, LOG_HAR_COMPLETE), server=server)

        # Save processed data
        com.to_csv(os.path.join(dir, LOG_HAR_COMPLETE), index=False, sep=" ")

        # Remove not necessary files
        # and exit the loop
        files = [LOG_BOT_COMPLETE,
                 LOG_TCP_COMPLETE,
                 LOG_TCP_PERIODIC,
                 LOG_UDP_COMPLETE,
                 LOG_UDP_PERIODIC,
                 LOG_HAR_COMPLETE]
        
        for file in os.listdir(dir):
            if file not in files:
                cmd = f"rm {os.path.join(dir, file)}"
                os.system(cmd)

if __name__ == "__main__":
    print("-" * 50)
    main()
    print("-" * 50)
