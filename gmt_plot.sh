#!/bin/bash
# GMT script to plot the USA
# gridding example 
plot_path='/gpfs/data1/vclgp/xiongl/IS2global/plot/'
# -17367530.44	7314540.83 ease 2.0
# -17367000, 7314000 # [-180, 180]
# 7229000 # N80
# -6149000 # S57
# merge all grids 
#gmt grdblend ../result/X*.nc -G${plot_path}blend.nc -R-17367000/17367000/-6149000/7229000 -I1000 -V
# GE	2 1	1 if A >= B, else 0
# MUL	2 1	A * B
# NAN	2 1	NaN if A == B, else A
gmt grdmath -V ${plot_path}blend.nc 5 GE  ${plot_path}blend.nc MUL 0 NAN = ${plot_path}blend_ge_5m.nc      
gmt makecpt -Crainbow -T0/30/1 -Z -F -D > ${plot_path}elevation.cpt
# aspect ratio is maintained.
gmt grdimage ${plot_path}blend_ge_5m.nc  -JX10.4i/4i -I+   -C${plot_path}elevation.cpt    -V -K > ${plot_path}global_is2.ps # -Baf -BWSen
gmt psscale -C${plot_path}elevation.cpt   -Dx5.2i/-0.2c+w6c/0.3c+jTC+h -Bxaf -By+lm -O >> ${plot_path}global_is2.ps
gmt psconvert ${plot_path}global_is2.ps -Tj -A
#xdg-open  ${plot_path}global.jpg