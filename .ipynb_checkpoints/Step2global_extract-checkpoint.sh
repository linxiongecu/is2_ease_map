#!/bin/bash
#module unload gdal
#conda activate /gpfs/data1/vclgp/xiongl/env/ih3/
# gedi  "rh_098 >= 5 and rh_098 < 120" 
folder="/gpfs/data1/vclgp/xiongl/IS2global/ease_tiles"
# Loop through each file in the folder
for file in "$folder"/*
do
    if [ -f "$file" ]; then  # Check if it's a regular file
        echo "Processing file: $file"
    basename=$(basename "$file")
    # Remove the ".gpkg" extension
    reg="${basename%.gpkg}"  
    ih3_extract_shots -o /gpfs/data1/vclgp/xiongl/IS2global/result/$reg -r $file --atl08 land_segments/canopy/h_canopy_20m --geo -q_20m '`land_segments/canopy/h_canopy_20m` >= 5 and `land_segments/canopy/h_canopy_20m` < 200' -n 15  --strong_beam # --merge  
    echo "#### $reg is done!!!"
    sleep 5
    fi
done