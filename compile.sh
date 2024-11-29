#!/source/bash

# Python executable
python=".venv/bin/python"

# Network bandwidths
rates=("1500kbits" "3000kbits" "4500kbits" "6000kbits" "7500kbits") # "50000kbits")

# Folder to be processed
folder="dazn/long_watchings/sandra"

for rate in "${rates[@]}"; do
    rm -rf "$folder/$rate/samples/"
    rm -rf "$folder/$rate/tests/"
done

# Compile supervised experiments
for rate in "${rates[@]}"; do
    source="src/processing/compiler.py"
    "$python" "$source" --folder="$folder/$rate/data" --server="dazn" &
done
wait

# Sample the HAS streaming flows in supervised experiments
for rate in "${rates[@]}"; do
    source="src/processing/sampler.py"
    "$python" "$source" --folder="$folder/$rate" --server="dazn" &
done
wait

# Generate the records to be used when using classification models
for rate in "${rates[@]}"; do
    source="src/processing/merger.py"
    "$python" "$source" --folder="$folder/$rate" --output=src &
done
wait


# Generate CNAMEs report for supervised experiments
# for rate in "${rates[@]}"; do
#     source="src/profiler.py"
#     "$python" "$source" --folder="$folder/$rate" --server="dazn"
# done



# Short-duration streaming periods
# source="src/processing/compiler.py"
# folder="dazn/fast_watchings/2024-11-27_15-53-59"
# "$python" "$source" --folder="$folder/data" --server="dazn"

# source="src/processing/profiler.py"
# folder="dazn/fast_watchings/2024-11-27_15-53-59"
# "$python" "$source" --folder="$folder" --server="dazn"

# source="src/processing/compiler.py"
# folder="dazn/fast_watchings/2024-11-27_15-53-45"
# "$python" "$source" --folder="$folder/data" --server="dazn"

# source="src/processing/profiler.py"
# folder="dazn/fast_watchings/2024-11-27_15-53-45"
# "$python" "$source" --folder="$folder" --server="dazn"


# for rate in "${rates[@]}"; do
#     source="src/fuzzy/merge_samples.py"
#     "$python" "$source" --first="src/tcp_data_1/$rate" --second="src/tcp_data_2/$rate" --destination="src/ml/decision_tree/tcp/$rate"
#     "$python" "$source" --first="src/udp_data_1/$rate" --second="src/udp_data_2/$rate" --destination="src/ml/decision_tree/udp/$rate"
# done