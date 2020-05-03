import pipeline.outlier_detection_peak as od
import pipeline.find_peak as fp
import pandas as pd
import numpy as np

STORE = "https://storage.gra.cloud.ovh.net/v1/AUTH_2aaacef8e88a4ca897bb93b984bd04dd/oco2//datasets/oco-2/soudings/oco2_"

# Test read from OVH storage TODO replace future storage with these paths
# df = pd.read_csv(STORE + "1409.csv.xz", compression="xz")

# Detect outliers on all peaks detected one month
peaks_detected = pd.read_csv("http://courty.fr/OCO2/result_for_oco2_1808-no_delta")
df = pd.read_csv("dataset/oco2_1808.csv", sep=";")  # to be replaced with path OVH
df = df.loc[(df['orbit'].isin(list(peaks_detected.orbit.unique()))), :]
df_dis = fp.compute_distance(df)
peaks_trans = od.add_features(peaks_detected, df_dis)
peaks_out = peaks_trans.copy()
peaks_out["y_class_lof"], peaks_out["outlier_score_lof"] = od.detect_outliers_lof(peaks_trans, neighbors=10,
                                                                                  features=["latitude", "longitude",
                                                                                            "slope", "intercept",
                                                                                            "amplitude", "sigma",
                                                                                            "delta", "R", "surf_pres"])
peaks_out.y_class_lof.value_counts()
#  1    2690
# -1      49
np.random.seed(18)
od.compare_peaks(df_dis, peaks_out, "y_class_lof")

peaks_out["y_class_lof_only_gaussian_param"], _ = od.detect_outliers_lof(peaks_trans, neighbors=10,
                                                                                  features=["slope", "intercept",
                                                                                            "amplitude", "sigma",
                                                                                            "delta", "R"])
peaks_out.y_class_lof_only_gaussian_param.value_counts()
#  1    2688
# -1      51
np.random.seed(123)
od.compare_peaks(df_dis, peaks_out, "y_class_lof_only_gaussian_param")

# Issue DBSCAN :very dependent on eps and nmin
peaks_out["y_class_dbscan"] = od.detect_outliers_dbscan(peaks_trans,
                                                 features=["latitude", "longitude", "slope", "intercept",
                                                           "amplitude", "sigma", "delta", "R", "surf_pres"])
peaks_out.y_class_dbscan.value_counts()
# Out[12]:
#  1    1964
# -1     775
np.random.seed(18)
od.compare_peaks(df_dis, peaks_out, "y_class_dbscan")

peaks_out["y_class_dbscan_only_gaussian_param"] = od.detect_outliers_dbscan(peaks_trans,
                                                 features=["slope", "intercept",
                                                           "amplitude", "sigma", "delta", "R"])
peaks_out.y_class_dbscan_only_gaussian_param.value_counts()
# Out[12]:
#  1    1964
# -1     775
np.random.seed(18)
od.compare_peaks(df_dis, peaks_out, "y_class_dbscan_only_gaussian_param")

# 4 methods: LOF using all features, LOF using only gaussian param features, DBSCAN using all features,
# DBSCAN using only param features

peaks_out.drop(columns="Unnamed: 0").to_csv("dataset/output/" + "peaks_out_1808", index=False)
