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

def gaussian_fit_on_df(df_full, output_filename='', output_dir=''):
    # spatial window for the detection (km)
    window = 200
    peak_found_number = 0
    peak_founds = []
    for orbit in tqdm(df_full['orbit'].unique(), desc='Orbit'):
    #for orbit in tqdm([22061, 21928, 16134, 21715], desc='Orbit'):
        df_orbit = df_full[df_full['orbit'] == orbit]
        if len(df_orbit) < 500:
            continue
        latitude_origin = df_orbit.iloc[0]['latitude']
        longitude_origin = df_orbit.iloc[0]['longitude']
        df_orbit['distance'] = 6367 * 2 * np.arcsin(np.sqrt(np.sin((np.radians(df_orbit['latitude'])
            - math.radians(latitude_origin))/2)**2 + math.cos(math.radians(latitude_origin))
            * np.cos(np.radians(df_orbit['latitude'])) * np.sin((np.radians(df_orbit['longitude'])
            - math.radians(longitude_origin))/2)**2))
        df_orbit = df_orbit.sort_values(by=['distance']).reindex()

        latitude_origin = df_orbit.iloc[0]['latitude']
        longitude_origin = df_orbit.iloc[0]['longitude']

        try:
            # Loop over the souding id's
            for i, j in tqdm(enumerate(df_orbit.index), desc='Souding', total=len(df_orbit)):
                # Work only each n soundings (15 seems good)
                if i % 15 != 0: continue
                ## !!!!!!
                #j = 2070068
                km_start = df_orbit.loc[j, 'distance']
                km_end = km_start + window
                # Slice back because our input point is the middle of the peak
                df_slice = df_orbit.query('distance >= (@km_start-@window/2) and distance <= @km_end')
                # Skip if too few data
                if len(df_slice)<400:
                    #print('ERROR : Not enought data')
                    continue
                med_temp = np.median(df_slice['xco2'])
                std_temp = np.std(df_slice['xco2'])
                df_slice['xco2_enhancement'] = df_slice['xco2'] - med_temp
                # Base parameters for : m, b, A, sig
                p0 = (0.,med_temp,30*df_slice.loc[j,'xco2_enhancement'],10.) 
                #print('Estimated parameters:', p0)
                d_centered = df_slice['distance'] - km_start
                '''
                Gaussian Fit
                scipy.optimize.curve_fit
                scipy.optimize.curve_fit(f, xdata, ydata, p0=None, sigma=None, absolute_sigma=False, check_finite=True, bounds=(-inf, inf), method=None, jac=None, **kwargs)[source]¶
                p0 = Initial guess for the parameters (length N).
                sigma : Determines the uncertainty in ydata.
                '''
                popt, pcov = curve_fit(f=gaussian, xdata=d_centered, ydata=df_slice['xco2'], sigma = df_slice['xco2_uncert'], p0 = p0, maxfev=20000)
        #         print('Best m, b, A, sig = ', popt)
        #         plt.plot(d_centered, gaussian(x = d_centered, m=popt[0], b=popt[1], A=popt[2], sig=popt[3]), 'r', label='fit')
        #         plt.scatter(x=d_centered, y=df_slice['xco2']) 
                sig = abs(popt[3])  # sigma of the Gaussian (km)
                #print(sig)
                if sig < 2 : continue  # too narrow
                if 3*sig > window / 2.: continue  # too large
                delta = popt[2]/(popt[3]*(2 * np.pi)**0.5)  # height of the peak (ppm)
                if delta < 0: continue  # depletion
                d_plume = df_slice[(d_centered >= -2*sig) & (d_centered <= 2*sig)]
                d_backg = df_slice[(d_centered < -2*sig) | (d_centered > 2*sig)]
                d_peak = df_slice[(d_centered >= -4*sig) & (d_centered <= 4*sig)]
                d_peak_distance = d_peak['distance'] - df_slice.loc[j, 'distance']
                # we want at least 1 1-km-sounding per km on average on both sides of the peak within 2 sigmas and between 2 and 3 sigmas
                if len(df_slice[(d_centered >= -1*sig) & (d_centered <= 0)]) < int(sig): continue
                if len(df_slice[(d_centered <= 1*sig) & (d_centered >= 0)]) < int(sig): continue
                if len(df_slice[(d_centered >= -3*sig) & (d_centered <= -2*sig)]) < int(sig): continue
                if len(df_slice[(d_centered <= 3*sig) & (d_centered >= 2*sig)]) < int(sig): continue
                # check the quality of the fit
                R = np.corrcoef(gaussian(d_peak_distance,*popt), d_peak['xco2'])
                if R[0,1]**2 < 0.25 : continue
                peak_found_number += 1
                #print('index',j, 'Number of good fit',good_find, 'Sigma:', sig, 'Ampleur de l\'émission de CO²:',delta,'Coef de coreflation',R[0,1])
                # TODO: Add filename of input to be able to load it later
                peak = {
                    'sounding_id' : df_slice.loc[j, 'sounding_id'],
                    'latitude' : df_slice.loc[j, 'latitude'],
                    'longitude' : df_slice.loc[j, 'longitude'],
                    'orbit' : orbit,
                    'slope' : popt[0],
                    'intercept' : popt[1],
                    'amplitude' : popt[2],
                    'sigma': popt[3],
                    'delta': delta,
                    'R' : R[0,1],
                    'windspeed_u' : df_slice.loc[j, 'windspeed_u'],
                    'windspeed_v' : df_slice.loc[j, 'windspeed_v']
                    # TODO : Vent
                }
                #print(peak)
                peak_founds.append(peak)
                #break
            if peak_found_number==0:
                print('NO PEAK FOUND for orbit', orbit)
            else:
                # Save at every orbit, but with same name because we do not empty peak_founds
                filename = 'result_d' + output_filename + '-o' + str(orbit) + '.csv'
                print('Saving to', os.path.join(dir, filename))
                df = pd.DataFrame(peak_founds)
                df.to_csv(os.path.join(dir, filename))
                peak_found_number = 0
        except RuntimeError: 
          # curve_fit failed 
          print('LOST orbit', orbit, 'j', j)
    return peak_founds

dir = os.path.realpath(input_dir)
print(dir)
data_1808 = pd.read_csv(os.path.join(dir, "oco2_1808.csv"), sep=";")
peak_founds = gaussian_fit_on_df(data_1808,'oco2_1808', dir)