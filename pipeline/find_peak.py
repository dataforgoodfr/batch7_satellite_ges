#!/usr/bin/python3

import pandas as pd
import numpy as np
from scipy.optimize import curve_fit
from tqdm import tqdm
import math
import os

input_dir = r'../../../datasets/OCO2/csv/'

'''
x : the data input value
m : the slope of the data
b : the intercep of the data
A : Amplitude de la courbe
sig : sigma / écart type de la courbe
'''
def gaussian(x, m, b, A, sig):
    return m * x + b + A / (sig * (2 * np.pi)**0.5) * np.exp(-x**2 / (2*sig**2))

def peak_detection(input_name, df_orbit, orbit_number, orbit_index, output_dir):
    default_return = {}
    window = 200 # in km
    km_start = df_orbit.loc[orbit_index, 'distance']
    # Slice back because our input point is the middle of the peak
    df_slice = df_orbit.query('distance >= (@km_start-@window/2) and distance <= (@km_start + @window)').copy()
    # Skip if too few data
    if len(df_slice)<400:
        #print('ERROR : Not enought data')
        return default_return
    med_temp = np.median(df_slice['xco2'])
    # std_temp = np.std(df_slice['xco2']) # Not used
    df_slice['xco2_enhancement'] = df_slice['xco2'] - med_temp
    # Base parameters for : m, b, A, sig
    p0 = (0.,med_temp,30*df_slice.loc[orbit_index,'xco2_enhancement'],10.) 
    #print('Estimated parameters:', p0)
    d_centered = df_slice['distance'] - km_start
    '''
    Gaussian Fit
    scipy.optimize.curve_fit
    scipy.optimize.curve_fit(f, xdata, ydata, p0=None, sigma=None, absolute_sigma=default_return, check_finite=True, bounds=(-inf, inf), method=None, jac=None, **kwargs)[source]¶
    p0 = Initial guess for the parameters (length N).
    sigma : Determines the uncertainty in ydata.
    '''
    popt, _ = curve_fit(f=gaussian, xdata=d_centered, ydata=df_slice['xco2'], sigma = df_slice['xco2_uncert'], p0 = p0, maxfev=20000, ftol=0.5, xtol=0.5) # ftol=0.5, xtol=0.5 to speed up
    sig = abs(popt[3])  # sigma of the Gaussian (km)
    #print(sig)
    if sig < 2 : return default_return  # too narrow
    if 3*sig > window / 2.: return default_return  # too large
    delta = popt[2]/(popt[3]*(2 * np.pi)**0.5)  # height of the peak (ppm)
    if delta < 0: return default_return  # depletion
    #d_plume = df_slice[(d_centered >= -2*sig) & (d_centered <= 2*sig)]
    #d_backg = df_slice[(d_centered < -2*sig) | (d_centered > 2*sig)]

    # we want at least 1 1-km-sounding per km on average on both sides of the peak within 2 sigmas and between 2 and 3 sigmas
    if len(df_slice[(d_centered >= -1*sig) & (d_centered <= 0)]) < int(sig): return default_return
    if len(df_slice[(d_centered <= 1*sig) & (d_centered >= 0)]) < int(sig): return default_return
    if len(df_slice[(d_centered >= -3*sig) & (d_centered <= -2*sig)]) < int(sig): return default_return
    if len(df_slice[(d_centered <= 3*sig) & (d_centered >= 2*sig)]) < int(sig): return default_return
    # check the quality of the fit
    d_peak = df_slice[(d_centered >= -4*sig) & (d_centered <= 4*sig)]
    d_peak_distance = d_peak['distance'] - df_slice.loc[orbit_index, 'distance']
    R = np.corrcoef(gaussian(d_peak_distance,*popt), d_peak['xco2'])
    if R[0,1]**2 < 0.25 : return default_return
    #print('orbit_index',orbit_index, 'Number of good fit',good_find, 'Sigma:', sig, 'Ampleur de l\'émission de CO²:',delta,'Coef de coreflation',R[0,1])
    # TODO: Add filename of input to be able to load it later
    peak = {
        'sounding_id' : df_slice.loc[orbit_index, 'sounding_id'],
        'latitude' : df_slice.loc[orbit_index, 'latitude'],
        'longitude' : df_slice.loc[orbit_index, 'longitude'],
        'orbit' : orbit_number,
        'slope' : popt[0],
        'intercept' : popt[1],
        'amplitude' : popt[2],
        'sigma': popt[3],
        'delta': delta,
        'R' : R[0,1],
        'windspeed_u' : df_slice.loc[orbit_index, 'windspeed_u'],
        'windspeed_v' : df_slice.loc[orbit_index, 'windspeed_v']
    }
    # Save souding data around peak
    df_slice['distance'] = df_slice['distance'] - df_orbit.loc[orbit_index, 'distance']
    filename = 'peak_data' + input_name + '-o' + str(orbit_number) + '-i' + str(orbit_index) + '.json'
    df_slice.to_json(os.path.join(output_dir, filename), orient='records')
    return peak



def gaussian_fit_on_df(df_full, input_name='', output_dir=''):
    # spatial window for the detection (km)
    peak_found_number = 0
    peak_founds = []
    for orbit in tqdm(df_full['orbit'].unique(), desc='Orbit'):
    #for orbit in tqdm([22061, 21928, 16134, 21715], desc='Orbit'):
        df_orbit = df_full[df_full['orbit'] == orbit].copy()
        if len(df_orbit) < 500:
            continue
        # Compute sounding distance from the begining of the orbit
        latitude_origin = df_orbit.iloc[0]['latitude']
        longitude_origin = df_orbit.iloc[0]['longitude']
        df_orbit['distance'] = 6367 * 2 * np.arcsin(np.sqrt(np.sin((np.radians(df_orbit['latitude'])
            - math.radians(latitude_origin))/2)**2 + math.cos(math.radians(latitude_origin))
            * np.cos(np.radians(df_orbit['latitude'])) * np.sin((np.radians(df_orbit['longitude'])
            - math.radians(longitude_origin))/2)**2))
        df_orbit = df_orbit.sort_values(by=['distance']).reindex()
        # Loop over the souding id's
        for i, orbit_index in tqdm(enumerate(df_orbit.index), desc='Souding', total=len(df_orbit)):
            try:
                # Work only each n soundings (15 seems good)
                if i % 15 != 0: continue
                peak = peak_detection(input_name, df_orbit, orbit, orbit_index, output_dir)
                if peak:
                    peak_found_number += 1
                    peak_founds.append(peak)
            except RuntimeError: 
                print('WARNING : Failed for orbit', orbit, 'and index', orbit_index)
        if peak_found_number==0:
            print('NO PEAK FOUND for orbit', orbit)
        else:
            # Save at every orbit, but with same name because we do not empty peak_founds
            filename = 'result_for_' + input_name + '.csv'
            print('Saving to', os.path.join(output_dir, filename))
            df = pd.DataFrame(peak_founds)
            df.to_csv(os.path.join(output_dir, filename))
            peak_found_number = 0
    return peak_founds

dir = os.path.realpath(input_dir)
print(dir)
data_1808 = pd.read_csv(os.path.join(dir, "oco2_1808.csv"), sep=";")
peak_founds = gaussian_fit_on_df(data_1808,'oco2_1808', dir)