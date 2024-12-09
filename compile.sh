#!/source/bash

# Python executable
python=".venv/bin/python"

# DAZN CNAMEs Profiling
folder="dazn/fast_watchings/2024-11-27_15-53-59"
python src/general_processing/compiler.py --folder="$folder/data" --output="$folder/tests" --server="dazn"
python src/general_processing/profiler.py --folder="$folder/tests" --server="dazn"

folder="dazn/fast_watchings/2024-11-27_15-53-45"
python src/general_processing/compiler.py --folder="$folder/data" --output="$folder/tests" --server="dazn"
python src/general_processing/profiler.py --folder="$folder/tests" --server="dazn"

# # DAZN Primary Flows Sampling
# bitrates=("1500kbits" "3000kbits" "4500kbits" "6000kbits" "7500kbits" "50000kbits")

# ######## GIORGIO
# for bitrate in "${bitrates[@]}"; do
#   folder="dazn/long_watchings/giorgio/$bitrate"
#   python src/general_processing/compiler.py --folder="$folder/data" --output="$folder/tests" --server="dazn"
# done

# for bitrate in "${bitrates[@]}"; do
#   folder="dazn/long_watchings/giorgio/$bitrate"
#   python src/general_processing/sampler.py --folder="$folder/tests" --output="$folder/samples" --server="dazn" &
# done
# #wait

# # for bitrate in "${bitrates[@]}"; do
# #   folder="dazn/long_watchings/giorgio/$bitrate"
# #   python src/general_processing/merger.py --folder="$folder/samples" --output="source_1/$bitrate"
# # done

# ######## SANDRA
# for bitrate in "${bitrates[@]}"; do
#   folder="dazn/long_watchings/sandra/$bitrate"
#   python src/general_processing/compiler.py --folder="$folder/data" --output="$folder/tests" --server="dazn"
# done

# for bitrate in "${bitrates[@]}"; do
#   folder="dazn/long_watchings/sandra/$bitrate"
#   python src/general_processing/sampler.py --folder="$folder/tests" --output="$folder/samples" --server="dazn" &
# done

#wait 
# for bitrate in "${bitrates[@]}"; do
#   folder="dazn/long_watchings/sandra/$bitrate"
#   python src/general_processing/merger.py --folder="$folder/samples" --output="source_2/$bitrate"
# done


# destination="source"
# for bitrate in "${bitrates[@]}"; do
#   folder_1="source_1/$bitrate"
#   folder_2="source_2/$bitrate"
#   python src/mixture_processing/folder_merger.py --first="$folder_1/tcp" --second="$folder_2/tcp" --destination="$destination/$bitrate/tcp"
#   python src/mixture_processing/folder_merger.py --first="$folder_1/tcp" --second="$folder_2/udp" --destination="$destination/$bitrate/udp"
# done


## Copy the data in the thesis-blog
# dst="/home/giorgiodaniele/Desktop/thesis-blog/dazn/dataset1/tests"
# src="/home/giorgiodaniele/Desktop/thesis-code/dazn/fast_watchings/2024-11-27_15-53-45/tests"
# rm -rf $dst && cp -r $src $dst

# dst="/home/giorgiodaniele/Desktop/thesis-blog/dazn/dataset2"
# rm -rf $dst && mkdir $dst
# for bitrate in "${bitrates[@]}"; do
#   src="/home/giorgiodaniele/Desktop/thesis-code/dazn/long_watchings/giorgio/$bitrate/tests"
#   dst="/home/giorgiodaniele/Desktop/thesis-blog/dazn/dataset2/$bitrate"
#   mkdir -p $dst && cp -r $src $dst

#   src="/home/giorgiodaniele/Desktop/thesis-code/dazn/long_watchings/giorgio/$bitrate/samples"
#   dst="/home/giorgiodaniele/Desktop/thesis-blog/dazn/dataset2/$bitrate"
#   mkdir -p $dst && cp -r $src $dst
# done


