#!/usr/bin/env python
# coding: utf-8
# Find all files with a .tif extension
import glob
input_files = glob.glob('/gpfs/data1/vclgp/xiongl/IS2global/result/X*.tif')
# Write the list of files to a text file
with open("tiff_list.txt", "w") as file:
    for tiff_file in input_files:
        file.write(f"{tiff_file}\n")
# gdal_merge.py -o mosaic.tif --optfile tiff_list.txt
import subprocess
# Specify the gdal_merge.py command as a list of strings
gdal_merge_command = [
    'gdal_merge.py',
    '-o', 'mosaic_is2.tif',
    '--optfile', 'tiff_list.txt'
]
# Execute the command using subprocess
try:
    subprocess.run(gdal_merge_command, check=True)
    print("gdal_merge.py completed successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error running gdal_merge.py: {e}")