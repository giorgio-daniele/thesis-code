import os
import pandas


from urllib.parse import urlparse
from collections  import Counter
from tabulate     import tabulate

def find_video_mp4(file: str):
    
    # Generate a new frame
    frame = pandas.read_json(file)
    
    # Generate the entries
    entries = pandas.json_normalize(frame["log"]["entries"])
    
    # Find anything that is mp4
    mimes = ["video/mp4"]
    
    # Get the matching requests
    requests = entries[entries["response.content.mimeType"].isin(mimes)].copy()
    
    # Get the CNAME
    requests["cname"] = requests["request.url"].apply(lambda url: urlparse(url).netloc)

    # Return the result
    return requests.groupby("cname").size().reset_index(name="count")


if __name__ == "__main__":
    
    counter = Counter()

    sources = [
        "dazn/fast_watchings/2024-11-27_15-53-45/data",
        "dazn/fast_watchings/2024-11-27_15-53-59/data"]
    
    for source in sources:
        files = [os.path.join(source, file) for file in os.listdir(source) if "har_complete" in file]
        for file in files:
            result = find_video_mp4(file=file)
            counter.update(dict(zip(result["cname"], result["count"])))
    
    data = [{"Server Name": key, "Request Count": value} for key, value in counter.items()]
    print(tabulate(data, headers="keys", tablefmt="grid"))