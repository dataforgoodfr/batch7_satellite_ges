import streamlit as st
import pandas as pd
import pydeck as pdk
from pydeck import data_utils
import swiftclient
import json
import re


# Read config file
with open("../config.json") as json_data_file:
    config = json.load(json_data_file)

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
    if 'CO2_emissions_Edgar' in data['name']:
        #print('{0}\t{1}\t{2}'.format(data['name'], data['bytes'], data['last_modified']))
        year = d = re.findall(r'(\d{4})', data['name'])[-1]
        files.update(
            {year : config['swift_storage']['base_url']+data['name']}
        )

@st.cache
def load_data(file):
    df = pd.read_csv(file)
    df["CO2 classification"].value_counts()
    co2 = df.loc[df["CO2 classification"] == 4, ['latitude', 'longitude', 'CO2 emissions']].copy()
    co2 = co2.rename(columns={"CO2 emissions": "co2em"})
    co2["co2em"] = co2["co2em"]*10**8
    co2["co2em"] = round(co2["co2em"])
    return co2

"""
# EDGAR CO2 levels
This map shows the CO² levels from [EDGAR](https://edgar.jrc.ec.europa.eu/overview.php).
"""

year = st.sidebar.selectbox("Year", tuple(files.keys()) )

df = load_data(files[year])
view = data_utils.compute_view(df[["longitude", "latitude"]])
view.pitch = 50
view.bearing = 0
view.zoom = 1
view.max_zoom = 5

tooltip = {
   "html": "<b>CO2 level x 10*8:</b> {co2em}",
   "style": {
        "backgroundColor": "steelblue",
        "color": "white"
   }
}

column_layer = pdk.Layer(
    "ColumnLayer",
    data=df,
    get_position=["longitude", "latitude"],
    get_elevation="co2em",
    elevation_scale=500,
    radius=10000,
    get_fill_color=["co2em*10", "co2em", "1/co2em", 140],
    pickable=True,
    auto_highlight=True,
)

r = pdk.Deck(
    column_layer, initial_view_state=view,
    map_style="mapbox://styles/mapbox/light-v9",
    views=("MapView", False),
    tooltip=tooltip
)
st.pydeck_chart(r)
