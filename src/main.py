import os
import re
import json
import shutil
import pandas
import urllib.parse
import datetime
import argparse

from lib.dazn import *


PATH = "dazn/long-play/1500kbits/data/log_har_complete-1.har"

with open(PATH, "r", encoding="utf-8") as f:
    data = json.load(f)
    
    # Get all entries in the HAR file
    entries  = data["log"]["entries"]
    
    for entry in entries:
        # Get the timestamp when the request has started
        ts = entry["startedDateTime"]
        ts = datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
        ts = ts.timestamp()
        
        # Get the timestamp when the request has completed
        ds = ["blocked", "dns", "send", "wait", "receive", "ssl"]
        te = ts + sum(max(0, entry["timings"].get(k, 0)) for k in ds)
        
        # Select all requests  with video/mp4 or audio/mp4 as MIME
        mime = entry.get("response", {}).get("content", {}).get("mimeType", "")
        
        # Jump to next request if not an audio or video request
        if "video" not in mime and "audio" not in mime:
            continue
        
        # Parse the URL
        data = urllib.parse.urlparse(entry.get("request", {}).get("url", "-"))
        
        # Get information about the server from which
        # the client is downloading the content and
        # the path of the resource
        server   = data.netloc
        resource = data.path
            
        if "video" in resource:
            print(f"This is a video request on {server}")
            # Look for a match in the manifest template that are
            # available on this server
            
        