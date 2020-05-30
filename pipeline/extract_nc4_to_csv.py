#!/usr/bin/python3

"""
This script read all the NC4 files from NASA, extract the features we need, and save them in CSV.
"""

from oco2peak import nc4_convert
import pandas as pd

# We use Ray framework to process file in parallel
import ray
ray.init(num_cpus=16)
@ray.remote
def process_files(input_dir, output_dir, years_months):
    #print('Processing', years_months)
    nc4_convert.process_files(input_dir, output_dir, years_months)
futures = []
# For 2014 to end of 2019, we took the V9 files
input_dir = r'/media/NAS-Divers/dev/datasets/OCO2/nc4-v9/'
output_dir = r'/media/data-nvme/dev/datasets/OCO2/csv-v9/'
years_months = nc4_convert.get_pattern_yearmonth()
futures = [process_files.remote(input_dir, output_dir, [year_month]) for year_month in years_months]

# For 2020, we took the V10
input_dir = r'/media/NAS-Divers/dev/datasets/OCO2/nc4-v10/'
output_dir = r'/media/data-nvme/dev/datasets/OCO2/csv-v10/'
years_months = []
year = 2020
for month in range(1,12+1):
    years_months.append(str(year)+str(month).zfill(2))
for year_month in years_months:
    futures.append(process_files.remote(input_dir, output_dir, [year_month]))
print('Total processes to run :',len(futures))
print(ray.get(futures))