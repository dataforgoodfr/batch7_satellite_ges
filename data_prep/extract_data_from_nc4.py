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

years_months = []
for year in range(14, 20+1):
    for month in range(1,12+1):
        years_months.append(str(year)+str(month).zfill(2))

#print(years_months)
# Loop over years from 2014
for year_month in years_months:
    #year = str(year)
    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
    print('Switch to year/month', year_month)
    # Get the file list in directory
    nc4_list = glob.glob(input_dir + "oco2_LtCO2_"+year_month+"*.nc4")
    # # Initialize array to store data
    month_data = np.empty((0,9))
    # Loop over the files
    for one_file in nc4_list:
        print('Reading', one_file)
        # Open the file
        try:
            file_nc = Dataset(one_file, 'r')
        except:
            print('ERROR reading', one_file)
            continue
        np_table = np.column_stack((file_nc.variables['xco2_quality_flag'],file_nc.variables['sounding_id'],file_nc.variables['latitude'],file_nc.variables['longitude'],
            file_nc.variables['xco2'],file_nc.variables['xco2_uncertainty'],file_nc.groups['Sounding'].variables['orbit'], file_nc.groups['Meteorology']['windspeed_u_met'], file_nc.groups['Meteorology']['windspeed_v_met']))
        month_data = np.concatenate((month_data, np_table), axis=0)
    if(month_data.size == 0):
        continue
    # Save this year to disk
    print("End of", year_month, 'saving to disk...')

    df = pd.DataFrame(month_data, columns=['flag','sounding_id', 'latitude', 'longitude', 'xco2', 'xco2_uncert', 'orbit', 'windspeed_u', 'windspeed_v'])
    # using dictionary to convert specific columns (https://www.geeksforgeeks.org/change-data-type-for-one-or-more-columns-in-pandas-dataframe/)
    convert_dict = {'sounding_id': int, 
                    'orbit': int
                } 
    df = df.astype(convert_dict) 
    # Remove bad quality
    df=df[df['flag']==0]
    # Remove flag
    df.drop(['flag'], axis=1, inplace=True)
    df.to_csv(output_dir + 'oco2_'+year_month+'.csv', sep=';', index=False)
    del df
    #break