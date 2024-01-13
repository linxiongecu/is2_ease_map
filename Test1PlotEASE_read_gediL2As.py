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
# step 1: read gedi l2a to csv, with EASE x and y.GEDI l2a ,...h5
# step 2: merge
# Step 3: gridding

data_folder = '/gpfs/data1/vclgp/xiongl/GEDIglobal/l2a_data/'
@dask.delayed
def get_df_gedi(L2A):
        base_file=os.path.basename(L2A)[:-2]+'parquet'
        out_parquet=data_folder+base_file#GEDI02_A*.h5
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
                latslons = latslons[(latslons['rh98'] >= 5) & (latslons['QualityFlag'] > 0) & (latslons['sensitivity'] > 0.95)]
                if len(latslons) >0:
                        res.append(latslons)
        if len(res) == 0: return
        result_df = pd.concat(res, ignore_index=True)
        result_df[['X', 'Y']] = result_df.apply(transform_coordinates, axis=1)
        #result_df.to_csv(out_csv, index=False)
        result_df.to_parquet(out_parquet, index=False)
        return result_df

# Beam,ShotNumber,Longitude,Latitude,QualityFlag,rh98,sensitivity
def transform_coordinates(row):
    transformer = Transformer.from_crs('epsg:4326', 'epsg:6933', always_xy=True)
    lon, lat = row['Longitude'], row['Latitude']
    x, y = transformer.transform(lon, lat)
    return pd.Series({'X': x, 'Y': y})


if __name__ == "__main__":
        print('# get all gedi l2a files...')
        all_l2a_files = glob.glob('/gpfs/data1/vclgp/data/iss_gedi/soc/*/*/GEDI02_A*.h5')
        print('# start client')
        client = Client()
        print(f'# dask client opened at: {client.dashboard_link}')  
        cmds = [get_df_gedi(f) for f in all_l2a_files]
        _ = dask.persist(*cmds)
        progress(_)
        del _
        print('')         
        client.close() # test if restart is better than close. 
        sys.exit('## -- DONE')