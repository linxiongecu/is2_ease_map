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
import re
# idea
# step 1: read l2a to csv, with EASE x and y. filename:X158_Y108_GEDI....h5
# step 2: merge csv files in each 72km grid
# step 3: csv to tif file gridding.


directory_path = '/gpfs/data1/vclgp/data/iss_gedi/umd/ease2_grid_t072km/'
data_folder = '/gpfs/data1/vclgp/xiongl/GEDIglobal/ease_data/'

@dask.delayed
def get_df_gedi(L2A='/gpfs/data1/vclgp/data/iss_gedi/umd/ease2_grid_t072km/X158/Y108/GEDI02_A_MW023MW026_X158Y108_02_003_02_T072KM.h5'):
        #print('# processing: ', L2A)
        grid_x=re.search(r'X\d+', L2A).group()
        grid_y=re.search(r'Y\d+', L2A).group()
        base_file=os.path.basename(L2A)[:-2]+'csv'
        out_csv=data_folder+grid_x+'_'+grid_y+'_'+base_file#X158_Y108_GEDI02_A*.h5
        if os.path.exists(out_csv): return
        gediL2A = h5py.File(L2A, 'r')  # Read file using h5py    
        beamNames = [g for g in gediL2A.keys() if g.startswith('BEAM')]
        res = []
        for beamName in beamNames:
                d_lats = gediL2A[f'{beamName}/geolocation/lat_lowestmode_a1'][()]
                d_lons = gediL2A[f'{beamName}/geolocation/lon_lowestmode_a1'][()]
                d_shots = gediL2A[f'{beamName}/shot_number'][()]
                d_quality = gediL2A[f'{beamName}/geolocation/quality_flag_a1'][()]
                d_rh = gediL2A[f'{beamName}/geolocation/rh_a1'][()] # 0---100 2d array
                d_sens = gediL2A[f'{beamName}/geolocation/sensitivity_a1'][()]
                lon, lat, shot, quality, beam, rh98, sensitivity = [], [], [], [], [], [], []  # Set up lists to store data
                # Take every 100th shot and append to list
                for i in range(len(d_shots)):
                        shot.append(str(d_shots[i]))
                        lon.append(d_lons[i])
                        lat.append(d_lats[i])
                        quality.append(d_quality[i])
                        beam.append(beamName)
                        rh98.append(d_rh[i,98]/100) # cm to m
                        sensitivity.append(d_sens[i])
                # Write all of the  shots to a dataframe
                latslons = pd.DataFrame({'Beam': beam, 'ShotNumber': shot, 'Longitude': lon, 'Latitude': lat,
                                         'QualityFlag': quality, 'rh98': rh98, 'sensitivity': sensitivity})
                # We define forest as land cover with tree canopy height ≥ 5 m,
                latslons = latslons[(latslons['rh98'] > 4.9) & (latslons['QualityFlag'] > 0) & (latslons['sensitivity'] > 0.95)]
                if len(latslons) >0:
                        res.append(latslons)
        if len(res) == 0: return
        result_df = pd.concat(res, ignore_index=True)
        result_df[['X', 'Y']] = result_df.apply(transform_coordinates, axis=1)
        result_df.to_csv(out_csv, index=False)
        return result_df


def write_data_2_grid(i, j):
        # save folder 
        name = 'GEDI02_A_X' + str(i) + '_Y' + str(j) + '.csv'
        if os.path.exists(data_folder + name): return
        fo_i = '{:03d}'.format(i) # return a string.
        fo_j = '{:03d}'.format(j) 
        #folder_path = directory_path + 'X'+ fo_i + '/Y' +   fo_j
        #X158_Y108_GEDI02_A*.h5
        folder_path = data_folder+ 'X'+ fo_i + '_Y' +   fo_j+'_'+'GEDI02_A*.csv'
        l2a_csvs = glob.glob(folder_path)
        if len(l2a_csvs) < 1: return
        print('\n # number of l2a files: ', len(l2a_csvs), ' in X', i, 'Y', j )
        res_72km = []
        for l2a in l2a_csvs:
            res_72km.append(pd.read_csv(l2a))
        res_72km = pd.concat(res_72km, ignore_index=True)
        #res_72km[['X', 'Y']] = res_72km.apply(transform_coordinates, axis=1)
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

def get_all_l2a_files():
    l2a_all = []
    for i in range(1, 483): #  1 - 482
            for j in range(1, 204): # 1- 203
                fo_i = '{:03d}'.format(i) # return a string.
                fo_j = '{:03d}'.format(j) 
                folder_path = directory_path + 'X'+fo_i + '/Y' +  fo_j
                if os.path.exists(folder_path):
                    l2a_in_grids = glob.glob(folder_path+'/GEDI02_A_MW*.h5')
                    if len(l2a_in_grids) >0:
                        l2a_all.append(l2a_in_grids)
    flattened_list = [element for sublist in l2a_all for element in sublist]
    return flattened_list

@dask.delayed
def get_72km_tif(i,j):
    fo_i = '{:03d}'.format(i) # return a string.
    fo_j = '{:03d}'.format(j) 
    # /gpfs/data1/vclgp/data/iss_gedi/umd/ease2_grid_t072km/X101/Y175
    folder_path = directory_path + 'X'+fo_i + '/Y' +  fo_j
    if os.path.exists(folder_path):
        write_data_2_grid(i, j)
        # check if csv exist
        data_folder = '/gpfs/data1/vclgp/xiongl/GEDIglobal/ease_data/'
        name = 'GEDI02_A_X' + str(i) + '_Y' + str(j) + '.csv'
        if os.path.exists(data_folder + name):                
            xmin, xmax, ymin, ymax = get_grid_x_y(i, j)
            # Run the shell script
            subprocess.run(['bash', 'gmt_plot.sh', str(xmin), str(xmax), str(ymin),str(ymax), str(i), str(j)])

if __name__ == "__main__":
        print('# get all l2a files...')
        # fs = get_all_l2a_files()
        # print(len(fs))
        # Open the file in read mode
        with open('t72km_files.txt', 'r') as file:
            # Read each line from the file into a list
            lines = file.readlines()
        # Optionally, you may want to remove newline characters from each line
        fs = [line.strip() for line in lines]
        print(len(fs))
        print('# start client')
        client = Client()
        print(f'# dask client opened at: {client.dashboard_link}')
        print('# writing gedi h5 to csv...')        
        cmds = [get_df_gedi(f) for f in fs]
        _ = dask.persist(*cmds)
        progress(_)
        del _
        print('')         
        print('# gridding to tif files...')   
        cmds=[]
        for i in range(1, 483): #  1 - 482
            for j in range(1, 204): # 1- 203
                # test
                #if i == 310 and j == 86: 
                    cmds.append(get_72km_tif(i,j))    
        _ = dask.persist(*cmds)
        progress(_)
        del _
        print('') 
        client.close() # test if restart is better than close. 
        sys.exit('## -- DONE')