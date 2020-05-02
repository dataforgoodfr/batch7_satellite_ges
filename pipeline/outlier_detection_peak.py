# modules
import os
import pandas as pd
import dask.dataframe as dd
import math
from sklearn.neighbors import LocalOutlierFactor
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt
from tqdm import tqdm
import numpy as np

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


def compute_haversine_formula(long, long_origin, lat, lat_origin, earth_radius=6367):
    """
    Function to compute Haversine formula from longitude, latitude, latitude origin and longitude
    origin
    :param earth_radius: int, indicating earth radius
    :param long: pandas Series, with longitude data of trace
    :param long_origin: pandas Series, with longitude origin of trace
    :param lat: pandas Series, with latitude data of trace
    :param lat_origin: pandas Series, with latitude origin of trace
    :return: pandas Series, with distance calculated from input using the Haversine formula
    """
    h = earth_radius * 2 * np.arcsin(np.sqrt(np.sin((np.radians(lat) - math.radians(lat_origin)) / 2) ** 2 +
                                             math.cos(math.radians(lat_origin)) * np.cos(np.radians(lat)) *
                                             np.sin((np.radians(long) - math.radians(long_origin)) / 2) ** 2))
    return h


def compute_distance(data_1808_25):
    orbit_long = data_1808_25.groupby("orbit")["longitude"].first().rename("longitude_orig")
    orbit_lat = data_1808_25.groupby("orbit")["latitude"].first().rename("latitude_orig")
    data_1808_25 = pd.concat([data_1808_25.set_index("orbit"), orbit_lat, orbit_long], axis=1).reset_index()
    data_1808_25["distance"] = data_1808_25.apply(lambda df: compute_haversine_formula(df["longitude"],
                                                                                       df["longitude_orig"],
                                                                                       df["latitude"],
                                                                                       df["latitude_orig"]),
                                                  axis=1)
    df_full = data_1808_25.sort_values(by=['orbit', 'distance']).reindex()
    return df_full


def gaussian(x, m, b, A, sig):
    """
    Function used to fit gaussian in peak_detection
    :param x: float, input data for curve
    :param m: float, slope of the data
    :param b: float, intercept of the data
    :param A: float, curve amplitude
    :param sig: float, standard deviation of curve
    :return: float
    """
    return m * x + b + A / (sig * (2 * np.pi) ** 0.5) * np.exp(-x ** 2 / (2 * sig ** 2))


def peak_detection(df_orbit, orbit_number, orbit_index, output_dir, implement_filters, window=200,
                   output_peak=True):
    """
    Function to determine peak from orbit, with option to implement Frederic Chevallier filters or not
    to restrict peaks found
    Gaussian Fit based on
    scipy.optimize.curve_fit
    scipy.optimize.curve_fit(f, xdata, ydata, p0=None, sigma=None, absolute_sigma=default_return, check_finite=True,
    bounds=(-inf, inf), method=None, jac=None, **kwargs)[source], where :
    p0 = Initial guess for the parameters (length N).
    sigma : Determines the uncertainty in ydata.
    ftol=0.5, xtol=0.5 to speed up
    :param window: int, parameter to indicate window in km to compute the trace for each potential peak
    :param df_orbit: pandas DataFrame
    :param orbit_number: int, orbit value corresponding to orbit_index
    :param orbit_index: int, index in input data for orbit data
    :param output_dir: str, directory to store json files for peaks
    :param implement_filters: Boolean, if True Frederic Chevallier filters are applied to filter out
    peaks judged insufficiently good
    :param output_peak: Boolean, if True, outputs the data around peak to json in specified path
    :return:
    """
    default_return = {}
    km_start = df_orbit.loc[orbit_index, 'distance']
    # Slice back because our input point is the middle of the peak
    df_slice = df_orbit.query('distance >= (@km_start-@window/2) and distance <= (@km_start + @window/2)').copy()
    # Skip if too few data
    if len(df_slice) < 400:
        return default_return
    med_temp = np.median(df_slice['xco2'])
    df_slice['xco2_enhancement'] = df_slice['xco2'] - med_temp

    # Base parameters for : m, b, A, sig
    p0 = (0., med_temp, 30 * df_slice.loc[orbit_index, 'xco2_enhancement'], 10.)
    d_centered = df_slice['distance'] - km_start
    popt, _ = curve_fit(f=gaussian, xdata=d_centered, ydata=df_slice['xco2'], sigma=df_slice['xco2_uncert'], p0=p0,
                        maxfev=20000, ftol=0.5, xtol=0.5)
    sig = abs(popt[3])  # sigma of the Gaussian (km)
    delta = popt[2] / (popt[3] * (2 * np.pi) ** 0.5)  # height of the peak (ppm)

    if implement_filters:
        if sig < 2:
            return default_return  # too narrow
        if 3 * sig > window / 2.:
            return default_return  # too large
        if delta < 0:
            return default_return  # depletion
        if len(df_slice[(d_centered >= -1 * sig) & (d_centered <= 0)]) < int(sig):
            return default_return
        if len(df_slice[(d_centered <= 1 * sig) & (d_centered >= 0)]) < int(sig):
            return default_return
        if len(df_slice[(d_centered >= -3 * sig) & (d_centered <= -2 * sig)]) < int(sig):
            return default_return
        if len(df_slice[(d_centered <= 3 * sig) & (d_centered >= 2 * sig)]) < int(sig):
            return default_return

    # check the quality of the fit
    d_peak = df_slice[(d_centered >= -4 * sig) & (d_centered <= 4 * sig)]
    d_peak_distance = d_peak['distance'] - df_slice.loc[orbit_index, 'distance']
    R = np.corrcoef(gaussian(d_peak_distance, *popt), d_peak['xco2'])
    if R[0, 1] ** 2 < 0.25:
        return default_return
    # TODO: Add filename of input to be able to load it later
    peak = {
        'sounding_id': df_slice.loc[orbit_index, 'sounding_id'],
        'latitude': df_slice.loc[orbit_index, 'latitude'],
        'longitude': df_slice.loc[orbit_index, 'longitude'],
        'orbit': orbit_number,
        'slope': popt[0],
        'intercept': popt[1],
        'amplitude': popt[2],
        'sigma': popt[3],
        'delta': delta,
        'R': R[0, 1],
        'windspeed_u': df_slice.loc[orbit_index, 'windspeed_u'],
        'windspeed_v': df_slice.loc[orbit_index, 'windspeed_v']
    }
    # Save sounding data around peak
    if output_peak:
        df_slice['distance'] = df_slice['distance'] - df_orbit.loc[orbit_index, 'distance']
        filename = 'peak_data-si_' + str(df_slice.loc[orbit_index, 'sounding_id']) + '.json'
        df_slice.to_json(os.path.join(output_dir, filename), orient='records')
    return peak


def gaussian_fit_on_df(df_full, input_name='', output_dir='', output_peak=True, implement_filters=True,
                       output_csv=True):
    """
    Function used to apply peak_detection to oco2 data
    :param output_csv: Boolean, if True, outputs csv with peaks all peaks detected
    :param implement_filters: Boolean, if True, implements Frederic Chevallier filters on peak detection
    :param output_peak: Boolean, if True outputs peak data to different json files
    :param df_full: pandas DataFrame, containing information for one or several orbits
    :param input_name: str, not implemented for now
    :param output_dir: str, directory for output data
    :return: list of dictionaries, where each element of list is peak data contained in a dictionary (returned value
    of peak_detection)
    """
    peak_found_number = 0
    peak_founds = []
    for orbit in tqdm(df_full['orbit'].unique(), desc='Orbit'):
        df_orbit = df_full[df_full['orbit'] == orbit].copy()
        if len(df_orbit) < 500:
            continue
        # Loop over the sounding id's
        for i, orbit_index in tqdm(enumerate(df_orbit.index), desc='Sounding', total=len(df_orbit)):
            try:
                # Work only each n soundings (15 seems good)
                if i % 15 != 0:  # perhaps implement random sample instead of fixed param
                    continue
                peak = peak_detection(df_orbit, orbit, orbit_index, output_dir, implement_filters=implement_filters,
                                      window=200, output_peak=output_peak)
                if peak:
                    peak_found_number += 1
                    peak_founds.append(peak)
            except RuntimeError:
                print('WARNING : Failed for orbit', orbit, 'and index', orbit_index)
        if peak_found_number == 0:
            print('NO PEAK FOUND for orbit', orbit)
        elif output_csv:
            # Save at every orbit, but with same name because we do not empty peak_founds
            filename = 'result_for_' + input_name + '.csv'
            print('Saving to', os.path.join(output_dir, filename))
            df = pd.DataFrame(peak_founds)
            df.to_csv(os.path.join(output_dir, filename))
        peak_found_number = 0
    return peak_founds


def add_features(peaks):



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
    clf = LocalOutlierFactor(neighbors)
    peaks["y_class"] = clf.fit_predict(x)
    x_scores = clf.negative_outlier_factor_
    peaks['outlier_score'] = x_scores
    return peaks


def graph_peak(df_full, peak, window=200):
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


def compare_peaks(df_full, peaks):
    abnormal_peak = peaks.loc[peaks["y_class"] == -1, :]
    normal_peak = peaks.loc[peaks["y_class"] == 1, :]
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
