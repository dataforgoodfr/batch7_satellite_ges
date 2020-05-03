#!/usr/bin/python3

import pandas as pd
import numpy as np
from scipy.optimize import curve_fit
from tqdm import tqdm
import os


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
    h = earth_radius * 2 * np.arcsin(np.sqrt(np.sin((np.radians(lat) - np.radians(lat_origin)) / 2) ** 2 +
                                             np.cos(np.radians(lat_origin)) * np.cos(np.radians(lat)) *
                                             np.sin((np.radians(long) - np.radians(long_origin)) / 2) ** 2))
    return h


# compute_haversine_formula([1,2,3], [1,2,3], [1,1,1], [1,1,1], earth_radius=6367)

def compute_distance(df):
    orbit_long = df.groupby("orbit")["longitude"].first().rename("longitude_orig")
    orbit_lat = df.groupby("orbit")["latitude"].first().rename("latitude_orig")
    df = pd.concat([df.set_index("orbit"), orbit_lat, orbit_long], axis=1).reset_index()
    # data_1808_25["distance"] = data_1808_25.apply(lambda df: compute_haversine_formula(df["longitude"],
    #                                                                                    df["longitude_orig"],
    #                                                                                    df["latitude"],
    #                                                                                    df["latitude_orig"]),
    #                                               axis=1)
    df["distance"] = compute_haversine_formula(df["longitude"], df["longitude_orig"], df["latitude"],
                                               df["latitude_orig"])
    df = df.sort_values(by=['orbit', 'distance']).reindex()
    return df


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


if __name__ == "__main__":
    input_dir = r'../../../datasets/OCO2/csv/'
    dir = os.path.realpath(input_dir)
    print(dir)
    data_1808 = pd.read_csv(os.path.join(dir, "oco2_1808.csv"), sep=";")
    data_1808 = compute_distance(data_1808)
    peaks_found = gaussian_fit_on_df(data_1808, input_name='oco2_1808', output_dir=dir, output_peak=True,
                                     output_csv=True, implement_filters=True)
