import ray
#from tqdm import tqdm
#import math
import glob
import os
import pandas as pd
from oco2peak import find_peak


ray.init(num_cpus=16)


@ray.remote
def found_peaks(file, output_dir):
    year_month = file[file.find('oco2_')+5 : file.find('.csv')]
    print('Processing',file, 'for', year_month)
    df = pd.read_csv(file, sep=";")
    df = find_peak.compute_distance(df)
    #peak_founds = gaussian_fit_on_df(df,'oco2_'+year_month, dir)
    find_peak.gaussian_fit_on_df(df, input_name='oco2_'+year_month, output_dir=output_dir, output_peak=True,
                                     output_csv=True, implement_filters=True)


### V9
input_dir = r'/media/data-nvme/dev/datasets/OCO2/csv-v9/'
output_dir = r'/media/data-nvme/dev/datasets/OCO2/peaks-v9/'
dir = os.path.realpath(input_dir)
print(dir)
files = []
for file in glob.glob(dir + "/oco2_1*.csv*", recursive=False):
    files.append(file)
files.sort()
print('Files to process for V9:', len(files))
futures = [found_peaks.remote(f, output_dir) for f in files]

### V10
input_dir = r'/media/data-nvme/dev/datasets/OCO2/csv-v10/'
output_dir = r'/media/data-nvme/dev/datasets/OCO2/peaks-v10/'
dir = os.path.realpath(input_dir)
print(dir)
files = []
for file in glob.glob(dir + "/oco2_20*.csv*", recursive=False):
    files.append(file)
    futures.append(found_peaks.remote(file, output_dir))
files.sort()
print('Files to process for V10:', len(files))
    
print('Total processes to run :',len(futures))
print(ray.get(futures))