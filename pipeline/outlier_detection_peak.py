# modules
import dask.dataframe as dd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn import preprocessing
from sklearn.neighbors import LocalOutlierFactor
from sklearn.cluster import DBSCAN
from .find_peak import gaussian


# from pipeline.find_peak import peak_detection, gaussian, cannot import because tests implemented directly in
# script, perhaps separate them in tests folder ?


def extract_orbit_date(file_name, day):
    """
    Function to extract one day of OCO2 data to test functions, using dask
    :param file_name: str, name of the csv file containing one month of data
    :param day: int, day you wish to extract data for
    :return: pandas DataFrame with data restricted to selected day
    """
    data_1808 = dd.read_csv(file_name, sep=";")
    data_1808["date"] = data_1808["sounding_id"].map_partitions(pd.to_datetime, format='%Y%m%d%H%M%S%f',
                                                                errors="coerce")
    df = data_1808.loc[(data_1808['date'].dt.day == day), :].compute()
    return df


def add_features(peaks_detected, df):
    peaks = pd.merge(peaks_detected, df.loc[:, ["sounding_id", "surface_pressure", "surface_pressure_apriori",
                                                "land_water_indicator", "land_fraction"]],
                     on="sounding_id", how="left")
    peaks["surf_pres"] = peaks["surface_pressure_apriori"] - peaks["surface_pressure"]
    return peaks


def detect_outliers_lof(peaks, features, neighbors=10):
    """
    Functions that implements Local Outlier Factor to determine abnormal fitted curves in peaks
    :param features: List, list of columns to determine neighbors
    :param peaks: data containing peak parameters (distinct peaks per row)
    :param neighbors: number of neigbors to implement LOF
    :return: pandas DataFrame, input with added y_class variable indicating if -1, abnormality of peak
    with respect to neighbor peaks, and if 1 normality of peak
    """
    x = peaks.loc[:, features].values
    std_scale = preprocessing.StandardScaler().fit(x)
    x = std_scale.transform(x)

    clf = LocalOutlierFactor(neighbors)
    peaks["y_class"] = clf.fit_predict(x)
    x_scores = clf.negative_outlier_factor_
    peaks['outlier_score'] = x_scores
    return peaks["y_class"], peaks["outlier_score"]


def detect_outliers_dbscan(peaks, features, epsilon=1, nmin=5):
    """
    Functions that implements DBSCAN to determine abnormal fitted curves in peaks
    :param nmin: parameter min_sample in DBSCAN
    :param epsilon: parameter eps in DBSCAN
    :param features: List, list of columns to determine neighbors
    :param peaks: data containing peak parameters (distinct peaks per row)
    :return: pandas DataFrame, input with added y_class variable indicating if -1, abnormality of peak
    with respect to neighbor peaks, and if 1 normality of peak
    """
    x = peaks.loc[:, features].values
    std_scale = preprocessing.StandardScaler().fit(x)
    x = std_scale.transform(x)
    clustering = DBSCAN(eps=epsilon, min_samples=nmin).fit(x)
    peaks["y_class"] = np.where(clustering.labels_ == -1, -1, 1)
    # print(peaks["y_class"].value_counts())
    return peaks["y_class"]


def graph_peak(df_full, peak):
    window = 200
    df_orbit = df_full[df_full['orbit'] == peak['orbit']]

    km_start = df_orbit.set_index("sounding_id").loc[peak['sounding_id'], 'distance']
    # Slice back because our input point is the middle of the peak
    df_slice = df_orbit.query('distance >= (@km_start-@window/2) and distance <= (@km_start + @window/2)').copy()

    x = df_slice['distance'] - km_start
    y = df_slice['xco2']
    plt.scatter(x, y, c=y, s=3, label='data')
    plt.plot(x, gaussian(x, m=peak["slope"], b=peak["intercept"], A=peak["amplitude"], sig=peak["sigma"]), 'r',
             label='fit')
    plt.legend()
    plt.xlabel('Distance')
    plt.ylabel('CO²')
    return None


def compare_peaks(df_full, peaks, col = "y_class"):
    abnormal_peak = peaks.loc[peaks[col] == -1, :]
    normal_peak = peaks.loc[peaks[col] == 1, :]
    ab_peak = np.random.choice(abnormal_peak.index)
    norm_peak = np.random.choice(normal_peak.index)
    plt.figure(figsize=(8, 8))
    plt.subplot(2, 1, 1)
    plt.title("Abormal peak")
    graph_peak(df_full, abnormal_peak.loc[ab_peak, :])
    plt.subplot(2, 1, 2)
    plt.title("Normal peak")
    graph_peak(df_full, normal_peak.loc[norm_peak, :])
    plt.tight_layout()
    plt.show()
    return None
