#!/usr/bin/env python
# coding: utf-8
# test merge lots of parquet files 
import pyarrow.parquet as pq
import glob
import time
import argparse
import os
out_path = '/gpfs/data1/vclgp/xiongl/IS2global/result/'
def merge_folder(tile =  'X10_Y0'):
    # Record the start time
    start_time = time.time()
    out_parquet = out_path + tile + '.parquet'
    #if os.path.exists(out_parquet): return
    print('Writing to a file:', out_parquet)
    files = glob.glob(out_path + tile + '/*.parquet')
    print('Number of files:',len(files))
    schema = pq.ParquetFile(files[0]).schema_arrow
    with pq.ParquetWriter(out_parquet, schema=schema) as writer:
        for file in files:
            writer.write_table(pq.read_table(file, schema=schema))
    # Record the end time
    end_time = time.time()
    # Calculate the running time
    running_time = end_time - start_time
    print(f"Running time: {running_time} seconds")
if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Example script with command-line argument.")
    parser.add_argument('--name', type=str, help='Specify tile name.', required=False)
    # Parse the command-line arguments
    args = parser.parse_args()
    ##### folders 
    # Specify the path to the directory you want to list
    if args.name: 
        merge_folder(args.name)
    else:
        # Get a list of folders in the specified directory
        folders = [folder for folder in os.listdir(out_path) if os.path.isdir(os.path.join(out_path, folder))]
        for folder in folders:
            merge_folder(folder)
    
#use
#python Step2_merge.py --name X10_Y1
#python Step2_merge.py 