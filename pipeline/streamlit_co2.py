import streamlit as st
import pandas as pd
import pydeck as pdk
from pydeck import data_utils
import swiftclient
import json
import re
import altair as alt 
import numpy as np

# Read config file
with open("../config.json") as json_data_file:
    config = json.load(json_data_file)

def get_gaussian_param(sounding_id, df_all_peak):
    df_param = df_all_peak.query("sounding_id==@sounding_id")
    if len(df_param)<1:
        """
        ### ERROR : sounding_id not found in dataframe !
        """
        return {'slope' : 1,'intercept' : 1,'amplitude' : 1,'sigma': 1,'delta': 1,'R' : 1}
    param_index = df_param.index[0]
    
    gaussian_param = {
        'slope' : df_param.loc[param_index, 'slope'],
        'intercept' : df_param.loc[param_index, 'intercept'],
        'amplitude' : df_param.loc[param_index, 'amplitude'],
        'sigma': df_param.loc[param_index, 'sigma'],
        'delta': df_param.loc[param_index, 'delta'],
        'R' : df_param.loc[param_index, 'R'],
    }
    return gaussian_param

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
def swift_con(config):
    """
    Function to connect to Open Stack Swift Object Storage
    :param config: JSON configuration
    :return: swiftclient.Connection
    """
    return swiftclient.Connection(user=config['swift_storage']['user'],key=config['swift_storage']['key'], authurl=config['swift_storage']['auth_url'],os_options=config['swift_storage']['options'],tenant_name=config['swift_storage']['tenant_name'], auth_version=config['swift_storage']['auth_version'])

# connect to Open Stack Swift Object Storage
swift_conn = swift_con(config)

# Retrieve file list
files = {}
objects = swift_conn.get_container('oco2')[1]
for data in objects:
    if '/peaks-detected/' in data['name']:
        #print('{0}\t{1}\t{2}'.format(data['name'], data['bytes'], data['last_modified']))
        yearmonth = d = re.findall(r'(\d{4})', data['name'])[-1]
        files.update(
            {yearmonth : config['swift_storage']['base_url']+data['name']}
        )

@st.cache
def load_data(file):
    df = pd.read_csv(file)
    return df


"""
# CO2 peaks found in OCO-2 satellite datas
"""

year = st.sidebar.selectbox("Year", tuple(files.keys()) )

df = load_data(files[year])
view = data_utils.compute_view(df[["longitude", "latitude"]])
view.pitch = 50
view.bearing = 0
view.zoom = 1
view.max_zoom = 10

tooltip = {
   "html": "<b>Delta :</b> {delta}<br/><b>Sounding :</b> {sounding_id}<script>console.log({sounding_id});alert('test');</script>",
   "style": {
        "backgroundColor": "steelblue",
        "color": "white"
   }
}


scatter_layer = pdk.Layer(
    "ScatterplotLayer",
    data=df,
    get_position=["longitude", "latitude"],
    get_radius="delta",
    radius_scale=6000,
    opacity=0.8,
    radius_min_pixels=1,
    radius_max_pixels=100,
    line_width_min_pixels=1,
    #elevation_scale=500,
    #radius=10000,
    #get_fill_color=["co2em*10", "co2em", "1/co2em", 140],
    get_fill_color=[255, 140, 0],
    get_line_color=[0, 0, 0],
    pickable=True,

    #auto_highlight=True,
)

r = pdk.Deck(
    scatter_layer, initial_view_state=view,
    map_style="mapbox://styles/mapbox/light-v9",
    views=("MapView", False),
    tooltip=tooltip
)
st.pydeck_chart(r)
df
sounding_id = st.text_input("label goes here", '2014092005344977')
df_detail = pd.read_json(config['swift_storage']['base_url']+'/datasets/oco-2/peaks-detected-details/peak_data-si_'+sounding_id+'.json')

gaussian_param = get_gaussian_param(sounding_id, df)

df_detail['gaussian_out'] = df_detail.apply(
    lambda dataset: gaussian(dataset['distance'], m=gaussian_param['slope'], b=gaussian_param['intercept'], A=gaussian_param['amplitude'], sig=gaussian_param['sigma'])
    , axis=1)

altair_graph = alt.Chart(df_detail).encode(x='distance')

scatter = altair_graph.mark_circle().encode(
        y=alt.Y('xco2', scale=alt.Scale(zero=False)),
        color='xco2', tooltip=['xco2']
    )
line =  altair_graph.mark_line(color='red').encode(
    y='gaussian_out'
)
st.altair_chart(scatter+line, use_container_width=True)