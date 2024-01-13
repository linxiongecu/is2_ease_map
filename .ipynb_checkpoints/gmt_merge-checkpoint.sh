#!/bin/bash
# GMT script to plot the USA
# gridding example 
#plot_path='/gpfs/data1/vclgp/xiongl/IS2global/plot/'
# -17367530.44	7314540.83 ease 2.0
# -17367000, 7314000 # [-180, 180]
# 7229000 # N80
# -6149000 # S57
# merge all grids 
# grdblend - Blend several partially over-lapping grids into one large grid
#gmt grdblend ../result/X*.nc -G${plot_path}blend.nc -R-17367000/17367000/-6149000/7229000 -I1000 -V
# grdpaste - Join two grids along their common edge
# grdraster - Extract subregion from a binary raster and save as a GMT grid

# for gedi 
gmt grdblend /gpfs/data1/vclgp/xiongl/GEDIglobal/result/X*.nc -G/gpfs/data1/vclgp/xiongl/GEDIglobal/plot/gedi_blend.nc -R-17367000/17367000/-6149000/7229000 -I1000 -V