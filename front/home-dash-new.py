import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objects as go
import dash_dangerously_set_inner_html
import json
import re
from urllib.error import URLError

import pandas as pd
import geopandas as gpd
from shapely.wkt import loads

# Our modules
from oco2peak.datasets import Datasets
from oco2peak import oco2map
from oco2peak import oco2mapfolium
from oco2peak import find_peak

# Read config file
config_file = "./configs/config.json"
with open(config_file) as json_data_file:
    config = json.load(json_data_file)

# Retrieve file list
def get_detected_peak_file_list(datasets):
    files = {}
    urls = datasets.get_files_urls(prefix="/datasets/oco-2/peaks-and-invent/", pattern='peaks_and_invent')
    # urls = datasets.get_files_urls(prefix="/datasets/oco-2/peaks-detected/")
    for url in urls:
        yearmonth = re.findall(r'_(\d{4}).', url)[-1]
        yearmonth_text = yearmonth[2:4] + '/20' +yearmonth[0:2]
        files.update(
            {yearmonth : {
                'url' : url,
                'label' : yearmonth_text
                }
            }
        )
    return files

# Build the mark for the slider 
def get_slider_mark(files):
    yearmonth_marks = {}
    for i, key in enumerate(sorted(files.keys())):
            yearmonth_text = files[key]['label']
            if i == 0 or i == len(files)-1:
                yearmonth_marks.update({i: {'label': yearmonth_text}})
            elif key[2:4] == '01': # Only years
                yearmonth_marks.update({i: {'label': '20' + key[0:2]}})
    return yearmonth_marks

datasets = Datasets(config_file)
files = get_detected_peak_file_list(datasets)


def build_graph(df_oco2, sounding_id):
    # https://storage.gra.cloud.ovh.net/v1/AUTH_2aaacef8e88a4ca897bb93b984bd04dd/oco2//datasets/oco-2/peaks-detected-details/peak_data-si_2016061413390672.json

    if sounding_id is None:
        return html.H1("Please select a point")

    if len(sounding_id)!=16 :
        print(len(sounding_id))
        return html.H1("Wrong sounding_id format !")

    url_peak = datasets.get_url_from_sounding_id(sounding_id) #get_files_urls('peak_data-si_' + sounding_id)[0]
    try:
        df_peak = datasets.get_dataframe(url_peak)
    except URLError as e:
        msg=''
        if hasattr(e, 'reason'):
            msg='We failed to reach a server.Reason: ' + e.reason
        elif hasattr(e, 'code'):
            msg='The server couldn\'t fulfill the request. Error code: '+ e.code
        return html.Div([html.H3('ERROR : souding data not found'),html.P(f"URL : {url_peak}"),html.P(f"Error : {msg}")])

    peak_param = datasets.get_peak_param(sounding_id, df_oco2)

    df_peak['gaussian_y'] = df_peak.distance.apply(
        lambda x: find_peak.gaussian(x=x, m=peak_param['slope'], b=peak_param['intercept'], A=peak_param['amplitude'], sig=peak_param['sigma']))

    sounding_scatter = oco2map.build_sounding_scatter(df_peak, peak_param)

    mapbox_token = config['mapbox_token']
    sounding_map = oco2map.build_sounding_map(df_peak, mapbox_token)

    return html.Div([
        # dash_dangerously_set_inner_html.DangerouslySetInnerHTML(f"<p>The estimated volume of the peak is {peak_param['ktCO2_per_h']:.4f} kilo-ton of CO<SUB>2</SUB> per hour</p>"),
        html.Div([
            html.H3('2D Scatter plot with peak detection'),
            html.P(f"m={peak_param['slope']}, b={peak_param['intercept']}, A={peak_param['amplitude']}, sig={peak_param['sigma']}"),
            dcc.Graph(
                id='xco2-graph',
                figure=sounding_scatter
            )
        ], className="eight columns"),

        html.Div([
            html.H3('3D scatter plot on map'),
            dcc.Graph(
                id='xco_sounding_mapbox',
                figure=sounding_map
            )
        ], className="four columns"),
    ], className="row")



last_key = sorted(files.keys())[-1]
print("Last_key: ", last_key)

######################## DATA IMPORT (In Progress) #################################################
print("- Loading peaks...")
oco2_data = datasets.get_dataframe(files[last_key]['url'])
oco2_data = gpd.GeoDataFrame(oco2_data)
oco2_data['geometry'] = oco2_data['geometry'].apply(loads)
oco2_data.crs = {'init': 'epsg:4326'}
print(oco2_data.shape[0], " peaks loaded.")

print("- Loading cities...")
path_cities = "https://raw.githubusercontent.com/dataforgoodfr/batch7_satellite_ges/master/dataset/cities_v1.csv"
cities = pd.read_csv(path_cities, sep=",", index_col=0)
cities = gpd.GeoDataFrame(cities, geometry=gpd.points_from_xy(cities.longitude, cities.latitude))
cities.crs = {'init': 'epsg:4326'}
print(cities.shape[0], " cities loaded.")

print("- Loading plants...")
path_plants = "https://raw.githubusercontent.com/dataforgoodfr/batch7_satellite_ges/master/dataset/CO2_emissions_centrale.csv"
plants = pd.read_csv(path_plants, sep=",", index_col=0)
plants = gpd.GeoDataFrame(plants, geometry=gpd.points_from_xy(plants.longitude, plants.latitude))
plants.crs = {'init': 'epsg:4326'}
print(plants.shape[0], " plants loaded.")

print("- Loading coal...")
path_plants_coal = "https://raw.githubusercontent.com/dataforgoodfr/batch7_satellite_ges/master/dataset/CO2_emissions_coal_plant.csv"
plants_coal = pd.read_csv(path_plants_coal, sep=",", index_col=0)
plants_coal = plants_coal[plants_coal['Longitude'].notna()]
plants_coal = plants_coal[plants_coal['Latitude'].notna()]
plants_coal = gpd.GeoDataFrame(plants_coal, geometry=gpd.points_from_xy(plants_coal.Longitude, plants_coal.Latitude))
plants_coal.crs = {'init': 'epsg:4326'}
print(plants_coal.shape[0], " plants loaded.")

# oco2_data = datasets.get_dataframe(files[last_key]['url'])
# oco2_data = oco2_data[oco2_data.delta > 1]
print("- Creating map...")
default_folium_map = oco2mapfolium.peaks_capture_map(oco2_data, plants, plants_coal, cities)
print("Map created.")

######################## DATA IMPORT (In Progress)  #################################################



###############################################################################
######################## DASH #################################################
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div(
    [
    html.H1(children='OCO-2 satellite data analysis'),
    html.Div(children=dash_dangerously_set_inner_html.DangerouslySetInnerHTML('''
        The goal of our project is to localize CO<SUB>2</SUB> emissions on Earth based on the the carbon concentration data measured by the OCO-2 Satellite from the NASA.
    ''')),
    html.Div(dash_dangerously_set_inner_html.DangerouslySetInnerHTML('''
    <ul>
        <li>The map shows the places where we detect a peak in CO<SUB>2</SUB> emission based on OCO-2 satellite data.</li>
        <li>We also plot the potential CO<SUB>2</SUB> source from declarative content (EDGAR, IEA, FAO...).</li>
        <li>You can select a month of observation with the slider below.</li>
        <li>You can click on a peak to view a detailed graph of what the satellite really saw and how we find a peak in this data.</li>
    </ul>
    <p>For more info, see <a href="https://github.com/dataforgoodfr/batch7_satellite_ges">our website</a>.</p>
    ''')),
    dcc.Slider(
        id='my-slider',
        min=0,
        max=len(files)-1,
        step=1,
        value=len(files)-1,
        marks=get_slider_mark(files)
    ),
    html.Div(id='slider-output-container'),
    # Big Map
    html.Iframe(id='folium-iframe', srcDoc=default_folium_map.get_root().render(), style={'width': '100%', 'height': '400px'}),
    # Focus on a single peak
    html.Div(id='div-xco2', children=build_graph(None, None)),
    # Input of a peak ID
    html.P("Sounding_id : "),
    dcc.Input(
        id="input_sounding",
        type="text",
        placeholder="2018082510150705",
    ),
    html.Div(children='''
        Made with Dash: A web application framework for Python.
    '''),
])


@app.callback(
    dash.dependencies.Output('div-xco2', 'children'),
    [dash.dependencies.Input('input_sounding', 'value')])
def update_graph(sounding_id):
    global oco2_data
    return build_graph(oco2_data, sounding_id)


@app.callback(
    [dash.dependencies.Output('slider-output-container', 'children'),
    dash.dependencies.Output('folium-iframe', 'srcDoc'),],
    [dash.dependencies.Input('my-slider', 'value')])
def update_output(value):
    global oco2_data
    key = sorted(files.keys())[value]
    url = files[key]['url']

    print("- Re-Loading peaks...")
    oco2_data = datasets.get_dataframe(url)
    oco2_data = gpd.GeoDataFrame(oco2_data)
    oco2_data['geometry'] = oco2_data['geometry'].apply(loads)
    oco2_data.crs = {'init': 'epsg:4326'}
    print(oco2_data.shape[0], " peaks loaded.")

    # oco2_data = datasets.get_dataframe(url)
    # oco2_data = oco2_data[oco2_data.delta > 1]
    return f'Dataset file : {url}', oco2mapfolium.peaks_capture_map(oco2_data, plants, plants_coal, cities).get_root().render()


if __name__ == '__main__':
    app.run_server(debug=True, threaded=True, dev_tools_hot_reload_interval=5000, dev_tools_hot_reload_max_retry=300)