#!/bin/bash
# activate the environment.
# | l2a       | rh_098                 | float   |
# rh_098  is in cm or m?
ih3_extract_shots -o /gpfs/data1/vclgp/xiongl/IS2global/result/X10_Y1 -r /gpfs/data1/vclgp/xiongl/IS2global/ease_tiles/X10_Y1.gpkg --atl08 land_segments/canopy/h_canopy_20m --geo -q_20m '`land_segments/canopy/h_canopy_20m` >5 and `land_segments/canopy/h_canopy_20m` < 200' -n 15  --strong_beam # --merge 