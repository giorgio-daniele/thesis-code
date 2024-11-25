import os
import time
import numpy
import pandas
import argparse

from collections import Counter
from lib.generic import *

def volume_formatter(num_bytes: float) -> str:
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size  = float(num_bytes)

    for unit in units:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} {units[-1]}"

def merge_data(coarse_samples_file: str, fine_samples_file: str, ratio: int):
    records = []

    # Generate a DataFrame for fine samples (high-frequency)
    fine = pandas.read_csv(fine_samples_file, sep=" ")

    # Generate a DataFrame for coarse samples (low-frequency)
    coarse = pandas.read_csv(coarse_samples_file, sep=" ")

    # Initialize the index for the fine samples
    index = 0

    # Iterate through each coarse sample
    for _, record in coarse.iterrows():
        # Get the next `ratio` fine samples
        matches = fine.iloc[index:index + ratio]
        
        # Update the index to point to the next set of fine samples
        index += ratio
        
        # Combine the coarse sample with the fine samples
        combined_values = list(record.values) + matches.values.flatten().tolist()

        # Create labels for the fine samples with the appropriate suffix
        labels = []
        for num in range(ratio):
            labels.extend([f"{col}_#{num}" for col in fine.columns])

        # Create labels for the coarse sample
        combined_labels = list(coarse.columns) + labels
        
        # Add the combined record to the list
        records.append(dict(zip(combined_labels, combined_values)))

    # Return a new DataFrame with the combined data
    return pandas.DataFrame(records)


def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", required=True)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    # Get the arguments
    return args.folder, args.output

def main():
    folder, output = args()

    print(f"MERGER is running on [{folder}] for service...")
    
    corse = 10_000
    fine  = 1_000
    ratio = int(corse / fine)
    
    # Fine samples
    dir = os.path.join(folder, "samples", "tcp", str(fine))
    fine_sample_files  = sorted([os.path.join(dir, f) for f in os.listdir(dir)])

    # Coarse samples
    dir = os.path.join(folder, "samples", "tcp", str(corse))
    coarse_sample_files = sorted([os.path.join(dir, f) for f in os.listdir(dir)])
         
    # Create the output directory if it does not exist yet
    out = os.path.join(output, "TCP", os.path.basename(folder))
    if not os.path.exists(out):
        cmd = f"mkdir -p {out}"
        os.system(cmd)

    for num, (fine_samples_file, coarse_samples_file) in enumerate(zip(fine_sample_files, coarse_sample_files)):
        frame = merge_data(coarse_samples_file=coarse_samples_file, fine_samples_file=fine_samples_file, ratio=ratio)
        frame.to_csv(os.path.join(out, f"sample-{num}"), index=False, sep=" ")

    #############################
    #           UDP             #
    #############################
        
    # Fine samples
    dir = os.path.join(folder, "samples", "udp", str(fine))
    fine_sample_files  = sorted([os.path.join(dir, f) for f in os.listdir(dir)])

    # Coarse samples
    dir = os.path.join(folder, "samples", "udp", str(corse))
    coarse_sample_files = sorted([os.path.join(dir, f) for f in os.listdir(dir)])
         
    # Create the output directory if it does not exist yet
    out = os.path.join(output, "UDP", os.path.basename(folder))
    if not os.path.exists(out):
        cmd = f"mkdir -p {out}"
        os.system(cmd)

    for num, (fine_samples_file, coarse_samples_file) in enumerate(zip(fine_sample_files, coarse_sample_files)):
        frame = merge_data(coarse_samples_file=coarse_samples_file, fine_samples_file=fine_samples_file, ratio=ratio)
        frame.to_csv(os.path.join(out, f"sample-{num}"), index=False, sep=" ")


if __name__ == "__main__":
    print("-" * 50)
    main()
    print("-" * 50)