#!/usr/bin/python3

import os
import glob
import re
import sys
from netCDF4 import Dataset
import numpy as np
import pandas as pd
input_dir = r'../../../datasets/OCO2/nc4/'
output_dir = r'../../../datasets/OCO2/csv/'
dir = os.path.realpath(input_dir)
print(dir)

# Loop over years from 2014
for year in range(14, 20):
    year = str(year)
    print('\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n')
    print('Switch to year', year)

    # Get the file list in directory
    nc4_list = glob.glob(input_dir + "oco2_LtCO2_"+year+"*.nc4")

    # Initialize array to store data
    sounding_array = []
    latitude_array = []
    longitude_array = []
    xco2_array = []
    xco2_uncert_array = []
    orbit_array = []

    # Loop over the files
    for one_file in nc4_list:
        print(one_file)

        # Open the file
        try:
            file_nc = Dataset(one_file, 'r')
        except:
            print('ERROR reading', one_file)
            continue
        # Extract information
        latitude = file_nc.variables['latitude'][:]
        longitude = file_nc.variables['longitude'][:]
        sounding_id = file_nc.variables['sounding_id'][:]  # YYYYMMDDhhmmssmf
        orbit = file_nc.groups['Sounding'].variables['orbit'][:]
        # height = file_nc.groups['Sounding'].variables['altitude'][:]
        xco2 = file_nc.variables['xco2'][:]     #ppm
        flag = file_nc.variables['xco2_quality_flag'][:] # 0=Good, 1=Bad
        xco2_uncert = file_nc.variables['xco2_uncertainty'][:]

        # Filter the data to remove bad quality data
        for index in range( len(latitude) ):
            # Don't read bad data
            if flag[index] :
                # Skip storing for bad data
                continue
            # Store good data
            sounding_array.append(sounding_id[index])
            latitude_array.append(latitude[index])
            longitude_array.append(longitude[index])
            xco2_array.append(xco2[index])
            xco2_uncert_array.append(xco2_uncert[index])
            orbit_array.append(orbit[index])
    # Save this year to disk
    print("End of", year, 'saving to disk...')
    np_table = np.column_stack((sounding_array,latitude_array,longitude_array,xco2_array,xco2_uncert_array,orbit_array))
    df = pd.DataFrame(np_table, columns=['sounding_id', 'latitude', 'longitude', 'xco2', 'xco2_uncert', 'orbit'])
    # using dictionary to convert specific columns (https://www.geeksforgeeks.org/change-data-type-for-one-or-more-columns-in-pandas-dataframe/)
    convert_dict = {'sounding_id': int, 
                    'orbit': int
                } 
    df = df.astype(convert_dict) 
    df.to_csv(output_dir + 'oco2_20'+year+'.csv', sep=';', index=False)
    del df
