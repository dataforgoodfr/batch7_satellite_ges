# modules
import os
import pandas as pd
import dask.dataframe as dd
import math
from sklearn.neighbors import LocalOutlierFactor
import matplotlib.pyplot as plt
# from tqdm import notebook as tqdm
import tqdm
import numpy as np

# from pipeline.find_peak import peak_detection, gaussian, cannot import because tests implemented directly in
# script, perhaps separate them in tests folder ?

# parameters
earth_radius = 6367


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


data_1808_25 = extract_orbit_date("dataset/oco2_1808.csv", 25)
print(data_1808_25.shape)


def compute_haversine_formula(earth_radius, long, long_origin, lat, lat_origin):
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
    data_1808_25["distance"] = data_1808_25.apply(lambda df: compute_haversine_formula(earth_radius, df["longitude"],
                                                                                       df["longitude_orig"],
                                                                                       df["latitude"],
                                                                                       df["latitude_orig"]),
                                                  axis=1)
    df_full = data_1808_25.sort_values(by=['orbit', 'distance']).reindex()
    return df_full


def peak_detection(df_orbit, orbit_number, orbit_index, output_dir, implement_filters=True):
    """
    Function to determine peak from orbit, with option to implement Frederic Chevalier filters or not
    :param df_orbit: pandas DataFrame
    :param orbit_number:
    :param orbit_index:
    :param output_dir:
    :param implement_filters:
    :return:
    """
    default_return = {}
    window = 200  # in km
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
    '''
    Gaussian Fit
    scipy.optimize.curve_fit
    scipy.optimize.curve_fit(f, xdata, ydata, p0=None, sigma=None, absolute_sigma=default_return, check_finite=True, 
    bounds=(-inf, inf), method=None, jac=None, **kwargs)[source]¶
    p0 = Initial guess for the parameters (length N).
    sigma : Determines the uncertainty in ydata.
    '''
    popt, _ = curve_fit(f=gaussian, xdata=d_centered, ydata=df_slice['xco2'], sigma=df_slice['xco2_uncert'], p0=p0,
                        maxfev=20000, ftol=0.5, xtol=0.5)  # ftol=0.5, xtol=0.5 to speed up
    sig = abs(popt[3])  # sigma of the Gaussian (km)
    # print(sig)
    if sig < 2: return default_return  # too narrow
    if 3 * sig > window / 2.: return default_return  # too large
    delta = popt[2] / (popt[3] * (2 * np.pi) ** 0.5)  # height of the peak (ppm)
    if delta < 0: return default_return  # depletion
    # d_plume = df_slice[(d_centered >= -2*sig) & (d_centered <= 2*sig)]
    # d_backg = df_slice[(d_centered < -2*sig) | (d_centered > 2*sig)]

    # we want at least 1 1-km-sounding per km on average on both sides of the peak within 2 sigmas and between 2 and 3 sigmas
    if len(df_slice[(d_centered >= -1 * sig) & (d_centered <= 0)]) < int(sig): return default_return
    if len(df_slice[(d_centered <= 1 * sig) & (d_centered >= 0)]) < int(sig): return default_return
    if len(df_slice[(d_centered >= -3 * sig) & (d_centered <= -2 * sig)]) < int(sig): return default_return
    if len(df_slice[(d_centered <= 3 * sig) & (d_centered >= 2 * sig)]) < int(sig): return default_return
    # check the quality of the fit
    d_peak = df_slice[(d_centered >= -4 * sig) & (d_centered <= 4 * sig)]
    d_peak_distance = d_peak['distance'] - df_slice.loc[orbit_index, 'distance']
    R = np.corrcoef(gaussian(d_peak_distance, *popt), d_peak['xco2'])
    if R[0, 1] ** 2 < 0.25: return default_return
    # print('orbit_index',orbit_index, 'Number of good fit',good_find, 'Sigma:', sig, 'Ampleur de l\'émission de CO²:',delta,'Coef de coreflation',R[0,1])
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
    # Save souding data around peak
    df_slice['distance'] = df_slice['distance'] - df_orbit.loc[orbit_index, 'distance']
    filename = 'peak_data-si_' + str(
        df_slice.loc[orbit_index, 'sounding_id']) + '.json'  # f_' + input_name + '-o_' + str(orbit_number) + '-
    df_slice.to_json(os.path.join(output_dir, filename), orient='records')
    return peak


def gaussian_fit_on_df(df_full, input_name='', output_dir=''):
    # spatial window for the detection (km)
    peak_found_number = 0
    peak_founds = []
    for orbit in tqdm(df_full['orbit'].unique(), desc='Orbit'):
        df_orbit = df_full[df_full['orbit'] == orbit].copy()
        if len(df_orbit) < 500:
            continue
        # Loop over the souding id's
        for i, orbit_index in tqdm(enumerate(df_orbit.index), desc='Souding', total=len(df_orbit)):
            try:
                # Work only each n soundings (15 seems good)
                if i % 15 != 0:
                    continue
                peak = peak_detection(input_name, df_orbit, orbit, orbit_index, output_dir)
                if peak:
                    peak_found_number += 1
                    peak_founds.append(peak)
            except RuntimeError:
                print('WARNING : Failed for orbit', orbit, 'and index', orbit_index)
        if peak_found_number == 0:
            print('NO PEAK FOUND for orbit', orbit)
        else:
            # Save at every orbit, but with same name because we do not empty peak_founds
            filename = 'result_for_' + input_name + '.csv'
            print('Saving to', os.path.join(output_dir, filename))
            df = pd.DataFrame(peak_founds)
            # df.to_csv(os.path.join(output_dir, filename))
            peak_found_number = 0
    return peak_founds


test = gaussian_fit_on_df(df_full, input_name='', output_dir='')
peaks = pd.DataFrame(test)
print(peaks.shape)  # (108, 12)

X = peaks.loc[:, ["latitude", "longitude", "slope", "intercept", "amplitude", "sigma", "delta", "R",
                  "windspeed_u", "windspeed_v"]].values
clf = LocalOutlierFactor()
y_pred = clf.fit_predict(X)
X_scores = clf.negative_outlier_factor_
peaks['outlier_score'] = X_scores
peaks["y_class"] = peaks['outlier_score'] < clf.offset_
# aucun pic anormal dans la détection peak_detection de l'article pour le 25/08
peak = peaks.iloc[0]


def show_peak(df_full, peak, window=200):
    df_orbit = df_full[df_full['orbit'] == peak['orbit']]
    km_start = df_orbit.loc[df_orbit["sounding_id"] == peak['sounding_id'], 'distance']
    km_end = km_start + window / 2
    # Slice back because our input point is the middle of the peak
    df_slice = df_full.loc[(df_full["distance"] >= km_start.iloc[0]) &
                           (df_full["distance"] <= km_end.iloc[0]) &
                           (df_full["orbit"] == peak['orbit']), :]
    x = df_slice['distance'] - km_start.iloc[0]
    y = df_slice['xco2']
    plt.scatter(x, y, c=y, s=3, label='data')
    plt.plot(x, gaussian(x, m=peak["slope"], b=peak["intercept"], A=peak["amplitude"], sig=peak["sigma"]), 'r',
             label='fit')
    plt.legend()
    plt.title('OCO 2 data')
    plt.xlabel('Distance')
    plt.ylabel('CO²')
    plt.show()


show_peak(df_full, peak)
