import pipeline.find_peak as fp
import numpy as np
from scipy.stats import norm
import matplotlib.pyplot as plt
import pandas as pd
from sklearn import mixture
import itertools



def select_peak(df, peaks, sounding_chosen):
    peak = peaks.loc[peaks["sounding_id"] == sounding_chosen, :]
    orbit_target = peak['orbit'].astype("int64")
    df_orbit = df.loc[df['orbit'] == orbit_target.iloc[0], :]
    df_orbit = fp.compute_distance(df_orbit)
    return df_orbit, peak


def preprocess_for_fit(df_orbit, peak, N_sample=10000, N_quantiles=200, window=200, window_rolling_avg=10):

    km_start = df_orbit.set_index("sounding_id").loc[peak['sounding_id'], 'distance']
    df_slice = df_orbit.loc[(df_orbit["distance"] >= km_start.iloc[0] - window/2) &
                            (df_orbit["distance"] <= km_start.iloc[0] + window/2), :]
    x = df_slice['distance'] - km_start.iloc[0]
    y = df_slice['xco2']
    X = pd.concat([x, y], axis=1)
    X = X.sort_values("distance")
    #plt.scatter(x, y, c=y, s=3, label='Observed data')
    # On réduit le bruit en prenant des bins
    X["bins"] = pd.qcut(x, N_quantiles)
    X = X.groupby("bins").agg({"distance": "mean", "xco2": "mean"})
    # then take rolling average to smooth curve
    X = X.rolling(window=window_rolling_avg).mean()
    X = X.dropna()
    plt.scatter(X.distance, X.xco2, c=X.xco2, s=3, label='Binned data')
    X = X.values
    # fit du gaussian mixture
    distance, co2 = X[:, 0], X[:, 1]
    n_distance = distance
    min_co2 = min(co2)
    n_co2 = (co2 - min_co2)/(sum(co2-min_co2))
    assert round(n_co2.sum()) == 1

    # Generate a distribution of points matcthing the curve
    line_distribution = np.random.choice(a=n_distance, size=N_sample, p=n_co2)
    number_points = len(line_distribution)
    return line_distribution, number_points, n_distance, n_co2


def fit_gaussian_mixture(df_orbit, peak, N_sample=10000, N_quantiles=200, window=200, k=5):
    line_distribution, number_points, n_distance, n_co2 = preprocess_for_fit(df_orbit, peak, N_sample=N_sample,
                                                                             N_quantiles=N_quantiles,
                                                                             window=window)
    # Run the fit
    gmm = mixture.GaussianMixture(n_components=k, random_state=12)
    gmm.fit(np.reshape(line_distribution, (number_points, 1)))
    gauss_mixt = np.array([p * norm.pdf(n_distance, mu, sd) for mu, sd, p in zip(gmm.means_.flatten(),
                                                                                 np.sqrt(gmm.covariances_.flatten()),
                                                                                 gmm.weights_)])
    gauss_mixt_t = np.sum(gauss_mixt, axis=0)

    # Plot the data
    fig, axis = plt.subplots(1, 1, figsize=(10, 12))
    axis.plot(n_distance, n_co2, label='Smoothed observed data')
    axis.plot(n_distance, gauss_mixt_t, label='Components fit')

    for i in range(len(gauss_mixt)):
        axis.plot(n_distance, gauss_mixt[i], label='Gaussian '+str(i))

    axis.set_xlabel('Distance')
    axis.set_ylabel('co2')
    axis.set_title('Sklearn fit GM fit')

    axis.legend()
    plt.show()
    return None


def fit_gaussian_mixture_model_selection(df_orbit, peak, N_sample=10000, N_quantiles=200, window=200):
    line_distribution, number_points, n_distance, n_co2 = preprocess_for_fit(df_orbit, peak, N_sample=N_sample,
                                                                             N_quantiles=N_quantiles,
                                                                             window=window)
    # Run the fit
    lowest_bic = np.infty
    bic = []
    n_components_range = range(1, 7)
    cv_types = ['spherical', 'tied', 'diag', 'full']
    for cv_type in cv_types:
        for n_components in n_components_range:
            # Fit a Gaussian mixture with EM
            gmm = mixture.GaussianMixture(n_components=n_components,
                                          covariance_type=cv_type)
            gmm.fit(np.reshape(line_distribution, (number_points, 1)))
            bic.append(gmm.bic(np.reshape(line_distribution, (number_points, 1))))
            if bic[-1] < lowest_bic:
                lowest_bic = bic[-1]
                best_gmm = gmm

    bic = np.array(bic)
    color_iter = itertools.cycle(['navy', 'turquoise', 'cornflowerblue',
                                  'darkorange'])
    clf = best_gmm
    bars = []
    # Plot the BIC scores
    plt.figure(figsize=(8, 6))
    spl = plt.subplot(2, 1, 1)
    for i, (cv_type, color) in enumerate(zip(cv_types, color_iter)):
        xpos = np.array(n_components_range) + .2 * (i - 2)
        bars.append(plt.bar(xpos, bic[i * len(n_components_range):
                                      (i + 1) * len(n_components_range)],
                            width=.2, color=color))
    plt.xticks(n_components_range)
    plt.ylim([bic.min() * 1.01 - .01 * bic.max(), bic.max()])
    plt.title('BIC score per model')
    xpos = np.mod(bic.argmin(), len(n_components_range)) + .65 + \
           .2 * np.floor(bic.argmin() / len(n_components_range))
    plt.text(xpos, bic.min() * 0.97 + .03 * bic.max(), '*', fontsize=14)
    spl.set_xlabel('Number of components')
    spl.legend([b[0] for b in bars], cv_types)

    gauss_mixt = np.array([p * norm.pdf(n_distance, mu, sd) for mu, sd, p in zip(clf.means_.flatten(),
                                                                                 np.sqrt(clf.covariances_.flatten()),
                                                                                 clf.weights_)])
    gauss_mixt_t = np.sum(gauss_mixt, axis=0)

    # Plot the data
    fig, axis = plt.subplots(1, 1, figsize=(10, 12))
    axis.plot(n_distance, n_co2, label='Smoothed observed data')
    axis.plot(n_distance, gauss_mixt_t, label='Components fit')

    for i in range(len(gauss_mixt)):
        axis.plot(n_distance, gauss_mixt[i], label='Gaussian ' + str(i))

    axis.set_xlabel('Distance')
    axis.set_ylabel('co2')
    axis.set_title('Sklearn fit GM fit')

    axis.legend()
    plt.show()
    mu = clf.means_.flatten()
    sd = np.sqrt(clf.covariances_.flatten())
    p = clf.weights_
    return None


if __name__ == "__main__":
    # Parametres
    month_chosen = "1608"

    STORE = "https://storage.gra.cloud.ovh.net/v1/AUTH_2aaacef8e88a4ca897bb93b984bd04dd/oco2//datasets/oco-2/"

    # Import
    df = pd.read_csv(STORE + "soudings/oco2_" + month_chosen + ".csv.xz", compression="xz", sep=";")
    peaks = pd.read_csv(STORE + "peaks-detected/result_for_oco2_" + month_chosen + ".csv").drop(columns="Unnamed: 0")
    np.random.seed(123)
    # test with johannesburg peak
    sounding_chosen = 2016083111560007
    df_orbit, peak = select_peak(df, peaks, sounding_chosen)
    # fit_gaussian_mixture(df_orbit, peak, N_sample=10000, N_quantiles=200, window=200, k=5)
    fit_gaussian_mixture_model_selection(df_orbit, peak, N_sample=10000, N_quantiles=200, window=200)
    # test with randomly chosen peak
    np.random.seed(12)
    orbit = np.random.choice(peaks.orbit.unique())
    sounding_chosen = np.random.choice(peaks.loc[peaks["orbit"] == orbit, "sounding_id"])
    df_orbit, peak = select_peak(df, peaks, sounding_chosen)
    # fit_gaussian_mixture(df_orbit, peak, N_sample=10000, N_quantiles=200, window=200, k=5)
    fit_gaussian_mixture_model_selection(df_orbit, peak, N_sample=10000, N_quantiles=200, window=200)
