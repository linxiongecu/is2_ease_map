#!/bin/bash
# GMT script to plot the USA
# gridding example 
# input argument xmin, xmax, ymin, ymax, grid_x, grid_y
xmin=$1
xmax=$2
ymin=$3
ymax=$4
grid_x=$5
grid_y=$6
R="-R${xmin}/${xmax}/${ymin}/${ymax}"
#J='-JX5i/5i'
I='-I1000'
grd="/gpfs/data1/vclgp/xiongl/IS2global/result/X${grid_x}_Y${grid_y}.nc"
median="/gpfs/data1/vclgp/xiongl/IS2global/result/X${grid_x}_Y${grid_y}.txt"
tif="/gpfs/data1/vclgp/xiongl/IS2global/result/X${grid_x}_Y${grid_y}.tif"
csv="/gpfs/data1/vclgp/xiongl/IS2global/result/X${grid_x}_Y${grid_y}.csv" 
#echo $csv
gmt blockmedian $csv  $R $I -h  > $median # (x,y,z) triples
gmt xyz2grd $median $R $I -G$grd
#gmt nearneighbor $median $R $I -G$grd -S3000 -N8/2
#gdal_translate NETCDF:$grd $tif
gdal_translate -of GTiff -a_srs EPSG:6933 NETCDF:$grd $tif
# how to use
# bash gmt_gridding.sh -8683722.0 -5789119.2 -2438228.8 -36.4  4 4