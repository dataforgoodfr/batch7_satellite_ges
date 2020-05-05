import ray
from tqdm import tqdm
import math
import glob
import os
import pandas as pd
from pipeline import find_peak #.gaussian_fit_on_df


input_dir = r'/media/data-nvme/dev/datasets/OCO2/csv/'

# @@ -105,7 +106,8 @@ def gaussian_fit_on_df(df_full, input_name='', output_dir=''):
#              - math.radians(longitude_origin))/2)**2))
#          df_orbit = df_orbit.sort_values(by=['distance']).reindex()
#          # Loop over the souding id's
# -        for i, orbit_index in tqdm(enumerate(df_orbit.index), desc='Souding', total=len(df_orbit)):
# +        #for i, orbit_index in tqdm(enumerate(df_orbit.index), desc='Souding', total=len(df_orbit)):
# +        for i, orbit_index in enumerate(df_orbit.index):
#              try:
#                  # Work only each n soundings (15 seems good)
#                  if i % 15 != 0: continue
# @@ -116,11 +118,12 @@ def gaussian_fit_on_df(df_full, input_name='', output_dir=''):
#              except RuntimeError: 
#                  print('WARNING : Failed for orbit', orbit, 'and index', orbit_index)
#          if peak_found_number==0:
# -            print('NO PEAK FOUND for orbit', orbit)
# +            #print('NO PEAK FOUND for orbit', orbit)
# +            None
#          else:
#              # Save at every orbit, but with same name because we do not empty peak_founds
#              filename = 'result_for_' + input_name + '.csv'
# -            print('Saving to', os.path.join(output_dir, filename))
# +            #print('Saving to', os.path.join(output_dir, filename))
#              df = pd.DataFrame(peak_founds)
#              df.to_csv(os.path.join(output_dir, filename))
#              peak_found_number = 0

ray.init(num_cpus=16)

dir = os.path.realpath(input_dir)
print(dir)
#-data_1808 = pd.read_csv(os.path.join(dir, "oco2_1808.csv"), sep=";")
#-peak_founds = gaussian_fit_on_df(data_1808,'oco2_1808', dir)
@ray.remote
def found_peaks(file):
    year_month = file[file.find('oco2_')+5 : file.find('.csv')]
    print('Processing',file, 'for', year_month)
    df = pd.read_csv(file, sep=";")
    df = find_peak.compute_distance(df)
    #peak_founds = gaussian_fit_on_df(df,'oco2_'+year_month, dir)
    find_peak.gaussian_fit_on_df(df, input_name='oco2_'+year_month, output_dir=dir, output_peak=True,
                                     output_csv=True, implement_filters=True)
files = []
process_all = True
if process_all:
    for file in glob.glob(dir + "/oco2_*.xz", recursive=False):
        files.append(file)
else:
    data_1808 = pd.read_csv(os.path.join(dir, "oco2_1808.csv.xz"), sep=";")
    data_1808 = find_peak.compute_distance(data_1808)
    peak_founds = find_peak.gaussian_fit_on_df(data_1808, input_name='oco2_1808', output_dir=dir, output_peak=True,
                                     output_csv=True, implement_filters=True)

print('Files to process:', files)
futures = [found_peaks.remote(f) for f in files]
print(ray.get(futures))