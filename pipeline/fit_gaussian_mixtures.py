import pipeline.find_peak as fp
import numpy as np
from scipy.stats import norm
import matplotlib.pyplot as plt
import pandas as pd
from sklearn import mixture

# Parametres
month_chosen = "1608"
sounding_chosen = 2016083111560007
STORE = "https://storage.gra.cloud.ovh.net/v1/AUTH_2aaacef8e88a4ca897bb93b984bd04dd/oco2//datasets/oco-2/"

# Import
df = pd.read_csv(STORE + "soudings/oco2_" + month_chosen + ".csv.xz", compression="xz", sep=";")
peaks = pd.read_csv(STORE + "peaks-detected/result_for_oco2_" + month_chosen + ".csv").drop(columns="Unnamed: 0")
peak = peaks.loc[peaks["sounding_id"] == sounding_chosen, :]
orbit_target = peak['orbit'].astype("int64")
df_orbit = df.loc[df['orbit'] == orbit_target.iloc[0], :]
df_orbit = fp.compute_distance(df_orbit)


def fit_gaussian_mixture(df_orbit, peak, N_sample=10000, N_quantiles=200, window=200):

    km_start = df_orbit.set_index("sounding_id").loc[peak['sounding_id'], 'distance']
    df_slice = df_orbit.loc[(df_orbit["distance"] >= km_start.iloc[0] - window/2) &
                            (df_orbit["distance"] <= km_start.iloc[0] + window/2), :]
    x = df_slice['distance'] - km_start.iloc[0]
    y = df_slice['xco2']
    X = pd.concat([x, y], axis=1)
    X = X.sort_values("distance")
    plt.scatter(x, y, c=y, s=3, label='Observed data')
    # On réduit le bruit en prenant des bins
    X["bins"] = pd.qcut(x, N_quantiles)
    X = X.groupby("bins").agg({"distance": "mean", "xco2": "mean"})
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

    # Run the fit
    gmm = mixture.GaussianMixture(n_components=5, random_state=12)
    gmm.fit(np.reshape(line_distribution, (number_points, 1)))
    gauss_mixt = np.array([p * norm.pdf(n_distance, mu, sd) for mu, sd, p in zip(gmm.means_.flatten(),
                                                                                 np.sqrt(gmm.covariances_.flatten()),
                                                                                 gmm.weights_)])
    gauss_mixt_t = np.sum(gauss_mixt, axis=0)

    # Plot the data
    fig, axis = plt.subplots(1, 1, figsize=(10, 12))
    axis.plot(n_distance, n_co2, label='Observed distance')
    axis.plot(n_distance, gauss_mixt_t, label='Components fit')

    for i in range(len(gauss_mixt)):
        axis.plot(n_distance, gauss_mixt[i], label='Gaussian '+str(i))

    axis.set_xlabel('Distance')
    axis.set_ylabel('co2')
    axis.set_title('Sklearn fit GM fit')

    axis.legend()
    plt.show()
    return None