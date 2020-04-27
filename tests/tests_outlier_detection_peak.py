import pipeline.outlier_detection_peak as out_detect
import pandas as pd
import numpy as np

# TODO: test this detection without parameter implement_filters set to True in gaussian_fit_on_df
# TODO: implement function design_features to add features to LOF based on Charlotte Delgot notebook
data_1808_25 = out_detect.extract_orbit_date("dataset/oco2_1808.csv", 25)
print(data_1808_25.shape)
df_full = out_detect.compute_distance(data_1808_25)
peaks_found = out_detect.gaussian_fit_on_df(df_full, input_name='', output_dir='', output_peak=False, output_csv=False)
peaks = pd.DataFrame(peaks_found)
print(peaks.shape)  # (108, 12)
peaks = out_detect.detect_outliers_lof(peaks, neighbors=10, features=["latitude", "longitude", "slope", "intercept",
                                                                      "amplitude", "sigma", "delta", "R"])
np.random.seed(18)
out_detect.compare_peaks(df_full, peaks)
