#!/usr/bin/python3

"""
This script upload al the JSON containing peak souding to the cloud, Open Stack Swift for the moment.
"""

from oco2peak.datasets import Datasets
import glob
import shutil
config = '../configs/config.json'
datasets = Datasets(config)

# We use Ray framework to process file in parallel
import ray
ray.init(num_cpus=32)
@ray.remote
def process_files(file, done_dest):
    datasets.upload(file, "/datasets/oco-2/peaks-detected-details/", 'application/json')
    shutil.move(file, done_dest)
    print(file, 'uploaded.')

# For 2014 to end of 2019, we took the V9 files
input = r'/mnt/datasets/OCO2/peaks-v9/*.json'
files = glob.glob(input, recursive=False)
print('Files to process for V9:', len(files))
futures = [process_files.remote(f, '/mnt/datasets/OCO2/peaks-v9-done/') for f in files]


# For 2020, we took the V10
input = r'/mnt/datasets/OCO2/peaks-v10/*.json'
for f in glob.glob(input, recursive=False):
    futures.append(process_files.remote(f, '/mnt/datasets/OCO2/peaks-v10-done/'))
print('Total processes to run :',len(futures))
ray.get(futures)