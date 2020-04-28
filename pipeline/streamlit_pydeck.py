import streamlit as st
import pandas as pd
import pydeck as pdk
from pydeck import data_utils

files = {"2017": "dataset/CO2_emissions_Edgar_2017_v3.csv",
         "2018": "dataset/CO2_emissions_Edgar_2018_v3.csv"}


@st.cache
def load_data(file):
    df = pd.read_csv(file)
    df["CO2 classification"].value_counts()
    co2 = df.loc[df["CO2 classification"] == 4, ['latitude', 'longitude', 'CO2 emissions']].copy()
    co2 = co2.rename(columns={"CO2 emissions": "co2em"})
    co2["co2em"] = co2["co2em"]*10**8
    co2["co2em"] = round(co2["co2em"])
    return co2


st.title("EDGAR CO2 levels")
year = st.sidebar.selectbox("Year", ('2017', '2018'))


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
