import os
import pandas
import argparse


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
    return parser.parse_args()

def main():
    # Extract the arguments
    arguments = args()

    # Access the argument values
    folder = arguments.folder
    output = arguments.output
    
    ten_secs = 10_000
    one_secs = 1_000
    
    protocol = "tcp"
    
    # Create the output folders
    tcp_folder = os.path.join(output, "tcp")
    udp_folder = os.path.join(output, "udp")
    
    os.makedirs(tcp_folder, exist_ok=True)
    os.makedirs(udp_folder, exist_ok=True)
    
    for protocol in ["tcp", "udp"]: 
        # Get 10 seconds samples
        ten_secs_folder = os.path.join(folder, protocol, str(ten_secs))
        ten_secs_files  = sorted([os.path.join(ten_secs_folder, f) for f in os.listdir(ten_secs_folder)])
        
        # Get 1  seconds samples
        one_secs_folder = os.path.join(folder, protocol, str(one_secs))
        one_secs_files  = sorted([os.path.join(one_secs_folder, f) for f in os.listdir(one_secs_folder)])

        for num, (one_secs_file, ten_secs_file) in enumerate(zip(one_secs_files, ten_secs_files)):
            frame = merge_data(coarse_samples_file=ten_secs_file, fine_samples_file=one_secs_file, ratio=int(ten_secs/one_secs))
            if protocol == "tcp":
                frame.to_csv(os.path.join(tcp_folder, f"sample-{num}"), index=False, sep=" ")
            if protocol == "udp":
                frame.to_csv(os.path.join(udp_folder, f"sample-{num}"), index=False, sep=" ")

if __name__ == "__main__":
    main()