import os
import time
import numpy
import pandas
import argparse

from collections import Counter
from lib.generic import *

def merge_data(hf: str, lf: str):

    l = pandas.read_csv(lf, sep=" ")
    h = pandas.read_csv(hf, sep=" ")

    # Define the list of the record that are gonna be
    # used in the dataframe
    records = []

    for index, record in l.iterrows():

        #####################################################
        #    Loop over the high frequency sampled file      #
        #    and get all the records. For each one,         #
        #    associate the rows from the low frequency      #
        #    sampled file                                   #
        #####################################################

        values = h[(h["te"] <= record["te"]) & (h["ts"] >= record["ts"])] 
        
        if not values.empty:
            
            # Flatten matching rows into a single row
            rows = values.values.flatten()

            # Create new column names for the matching rows
            cols = [f"{col}_#{i}" for i in range(1, len(values) + 1) for col in values.columns]

        else:
            # If no rows match, create a zero-filled 
            # array with the same number of columns as 
            # values would have
            rows = numpy.zeros(len(h.columns))
            cols = [f"{col}_#1" for col in h.columns]

        # Combine record with flattened rows (either matching or zero-filled)
        combined_record = pandas.DataFrame([record.tolist() + rows.tolist()], columns=list(l.columns) + cols)

        # Append the combined row to the final records
        records.append(combined_record)
        
    frame = pandas.concat(records, ignore_index=True)
    return frame

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", required=True)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    # Get the arguments
    return args.folder, args.output

#############################
#           MAIN            #
#############################

def main():
    folder, output = args()

    print(f"MERGER is running on [{folder}] for service...")
    
    #############################
    #           TCP             #
    #############################
    
    # High frequency files
    dir = os.path.join(folder, "samples", "tcp", str(1000))
    hff = sorted([os.path.join(dir, f) for f in os.listdir(dir)])

    # Low frequency files
    dir = os.path.join(folder, "samples", "tcp", str(10_000))
    lff = sorted([os.path.join(dir, f) for f in os.listdir(dir)])
         
    # Create the output directory if it does not exist yet
    out = os.path.join(output, "tcp_data", os.path.basename(folder))
    if not os.path.exists(out):
        cmd = f"mkdir -p {out}"
        os.system(cmd)

    for num, (hf, lf) in enumerate(zip(hff, lff)):
        frame = merge_data(hf=hf, lf=lf)
        frame.to_csv(os.path.join(out, f"sample-{num}"), index=False, sep=" ")

    #############################
    #           UDP             #
    #############################
        
    # High frequency files
    dir = os.path.join(folder, "samples", "udp", str(1000))
    hff = sorted([os.path.join(dir, f) for f in os.listdir(dir)])

    # Low frequency files
    dir = os.path.join(folder, "samples", "udp", str(10_000))
    lff = sorted([os.path.join(dir, f) for f in os.listdir(dir)])
    
    # Create the output directory if it does not exist yet
    out = os.path.join(output, "udp_data", os.path.basename(folder))
    if not os.path.exists(out):
        cmd = f"mkdir -p {out}"
        os.system(cmd)

    for num, (hf, lf) in enumerate(zip(hff, lff)):
        frame = merge_data(hf=hf, lf=lf)
        frame.to_csv(os.path.join(out, f"sample-{num}"), index=False, sep=" ")


if __name__ == "__main__":
    print("-" * 50)
    main()
    print("-" * 50)