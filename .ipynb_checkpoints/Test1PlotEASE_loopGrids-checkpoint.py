#!/usr/bin/env python
# coding: utf-8

# import libraries
import os
import h5py
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy
from pyproj import Transformer
import glob
import os
import numpy
import subprocess
import sys
from dask.distributed import Client, progress
import dask
import argparse
# idea
# step 1: read l2a to csv, with EASE x and y. filename:X158_Y108_GEDI....h5
# step 2: merge csv files in each 72km grid
# step 3: csv to tif file gridding.


directory_path = '/gpfs/data1/vclgp/data/iss_gedi/umd/ease2_grid_t072km/'
data_folder = '/gpfs/data1/vclgp/xiongl/GEDIglobal/ease_data/'



def getCmdArgs():
    p = argparse.ArgumentParser(description = "Extracting or Grdding GEDI data")  
    p.add_argument("-e", "--extraction", dest="extraction", required=False, action='store_true', help="extraction gedi data")
    p.add_argument("-g", "--gridding", dest="gridding", required=False, action='store_true', help="gridding csv files")
    cmdargs = p.parse_args()
    return cmdargs


@dask.delayed
def get_df_gedi(L2A='/gpfs/data1/vclgp/data/iss_gedi/umd/ease2_grid_t072km/X158/Y108/GEDI02_A_MW023MW026_X158Y108_02_003_02_T072KM.h5'):
        #print('# processing: ', L2A)
        gediL2A = h5py.File(L2A, 'r')  # Read file using h5py    
        beamNames = [g for g in gediL2A.keys() if g.startswith('BEAM')]
        res = []
        for beamName in beamNames:
                latslons = pd.DataFrame()
                latslons['ShotNumber'] = gediL2A[f'{beamName}/shot_number'][()]
                latslons['Longitude'] = gediL2A[f'{beamName}/geolocation/lon_lowestmode_a1'][()]
                latslons['Latitude'] = gediL2A[f'{beamName}/geolocation/lat_lowestmode_a1'][()]
                latslons['QualityFlag'] = gediL2A[f'{beamName}/geolocation/quality_flag_a1'][()]
                latslons['rh98']= gediL2A[f'{beamName}/geolocation/rh_a1'][:,98]/100 # 0---100 2d array
                latslons['sensitivity'] = gediL2A[f'{beamName}/geolocation/sensitivity_a1'][()]
                latslons.insert(0,'Beam', beamName)
                lon, lat, shot, quality, beam, rh98, sensitivity = [], [], [], [], [], [], []  # Set up lists to store data
                # We define forest as land cover with tree canopy height ≥ 5 m,
                latslons = latslons[(latslons['rh98'] > 4.9) & (latslons['QualityFlag'] > 0) & (latslons['sensitivity'] > 0.95)]
                if len(latslons) >0:
                        res.append(latslons)
        if len(res) == 0: return
        result_df = pd.concat(res, ignore_index=True)
        return result_df

@dask.delayed
def write_data_2_csv(folder):# '/gpfs/data1/vclgp/data/iss_gedi/umd/ease2_grid_t072km/X001/Y030'
        # save folder 
        i=int(folder.split('/')[-2][1:]) # this is x001
        j=int(folder.split('/')[-1][1:]) # this is y030.....
        name = 'GEDI02_A_X' + str(i) + '_Y' + str(j) + '.csv'
        if os.path.exists(data_folder + name): return
        fo_i = '{:03d}'.format(i) # return a string.
        fo_j = '{:03d}'.format(j) 
        folder_path = directory_path + 'X'+ fo_i + '/Y' +   fo_j
        l2a_files = glob.glob(folder_path + '/GEDI02_A_MW*.h5')
        if len(l2a_files) < 1: return
        print('\n # number of l2a files: ', len(l2a_files), ' in X', i, 'Y', j )
        delays_72km = []
        for l2a in l2a_files:
            delays_72km.append(get_df_gedi(l2a))
        res_72km = dask.compute(delays_72km) # return a tuple.
        res_72km = res_72km[0] # This is a list.
        # Remove None values from the list
        res_72km = [df for df in res_72km if df is not None]
        if len(res_72km) == 0: 
            print('\n # no quality shots in this 72km cell.')
            return
        res_72km = pd.concat(res_72km, ignore_index=True)
        print('\n # grid csv finish.')
        res_72km[['X', 'Y']] = res_72km.apply(transform_coordinates, axis=1)
        print('\n # grid to ease x y finish.')
        # Save the DataFrame to a CSV file
        res_72km.to_csv(data_folder + name, index=False)
# X_001 ---X_482
# Y_001 --- Y_203
def get_grid_x_y(grid_x = 1, grid_y = 1):
        tilesize = 72
        ease2_origin = -17367530.445161499083042, 7314540.830638599582016
        ease2_nbins = int(34704 / tilesize + 0.5), int(14616 / tilesize + 0.5)
        ease2_binsize = 1000.895023349556141*tilesize, 1000.895023349562052*tilesize
        x_up_left = ease2_origin[0] + (grid_x - 1)*ease2_binsize[0]
        y_up_left = ease2_origin[1] - (grid_y - 1)*ease2_binsize[1]
        xmin = x_up_left
        xmax = x_up_left + ease2_binsize[0]
        ymin = y_up_left - ease2_binsize[1]
        ymax = y_up_left
        return round(xmin/1000)*1000, round(xmax/1000)*1000, round(ymin/1000)*1000,  round(ymax/1000)*1000# to integer

# Beam,ShotNumber,Longitude,Latitude,QualityFlag,rh98,sensitivity
def transform_coordinates(row):
    transformer = Transformer.from_crs('epsg:4326', 'epsg:6933', always_xy=True)
    lon, lat = row['Longitude'], row['Latitude']
    x, y = transformer.transform(lon, lat)
    return pd.Series({'X': x, 'Y': y})

# get all folders
def get_all_grids():
    grids_all = []
    for i in range(1, 483): #  1 - 482
            for j in range(1, 204): # 1- 203
                fo_i = '{:03d}'.format(i) # return a string.
                fo_j = '{:03d}'.format(j) 
                folder_path = directory_path + 'X'+fo_i + '/Y' +  fo_j
                if os.path.exists(folder_path):
                    grids_all.append(folder_path)
    return grids_all


@dask.delayed
def get_72km_tif(csvf): # '/gpfs/data1/vclgp/xiongl/GEDIglobal/ease_data/GEDI02_A_X175_Y101.csv'
        i = int(os.path.basename(csvf)[:-4].split('_')[-2][1:])# x175
        j = int(os.path.basename(csvf)[:-4].split('_')[-1][1:]) # y101
        name = 'GEDI02_A_X' + str(i) + '_Y' + str(j) + '.csv'
        tif=f"/gpfs/data1/vclgp/xiongl/GEDIglobal/result/X_{i}_Y_{j}.tif"
        if not os.path.exists(tif):
                #print(i, j, csvf)
                xmin, xmax, ymin, ymax = get_grid_x_y(i, j)
                # Run the shell script
                subprocess.run(['bash', 'gmt_plot.sh', str(xmin), str(xmax), str(ymin),str(ymax), str(i), str(j)])

if __name__ == "__main__":
        args = getCmdArgs()
        print('# start client')
        client = Client(n_workers=15, threads_per_worker=1) # Client(n_workers=2, threads_per_worker=4) 
        print(f'# dask client opened at: {client.dashboard_link}')
        if args.extraction: 
                print('# get all 72km folders...')
                folders = get_all_grids()
                # folders[1] = '/gpfs/data1/vclgp/data/iss_gedi/umd/ease2_grid_t072km/X001/Y030'
                cmds = [write_data_2_csv(folder) for folder in folders]
                _ = dask.persist(*cmds)
                progress(_)
                del _
                print('') 
        if args.gridding:
                print('# grid csv files...')
                name = 'GEDI02_A_X*.csv'
                csvfiles = glob.glob(data_folder + name)
                cmds = [get_72km_tif(csvf) for csvf in csvfiles]
                _ = dask.persist(*cmds)
                progress(_)
                del _
                print('')
        client.close() # test if restart is better than close. 
        sys.exit('## -- DONE')
# python 1PlotEASE_loopGrids.py --extraction   24 hours.
# python 1PlotEASE_loopGrids.py --gridding