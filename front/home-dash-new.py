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
    detected_peaks_urls = datasets.get_files_urls(prefix="/datasets/oco-2/peaks-and-invent/", pattern='peaks_and_invent')
    for detected_peaks_url in detected_peaks_urls:
        # RegExp to get 4 digits followed by a dot
        yearmonth = re.findall(r'(\d{4})\.', detected_peaks_url)[-1]
        yearmonth_text = yearmonth[2:4] + '/20' +yearmonth[0:2]
        files.update(
            {yearmonth : {
                'url' : detected_peaks_url,
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
        # Get the peak with maximum volume of CO2
        sounding_id = df_oco2.loc[df_oco2.ktCO2_per_h==df_oco2.ktCO2_per_h.max()].iloc[0].sounding_id.astype('int64')
        # return html.H1("Please select a point")
    #if len(str(int(float(sounding_id))))!=16 :
    sounding_id = str(sounding_id)
    #print(sounding_id)
    if len(sounding_id)!=16 :
        return html.H1("Wrong sounding_id format !")
    one_peak_url = datasets.get_url_from_sounding_id(sounding_id) #get_files_detected_peaks_urls('peak_data-si_' + sounding_id)[0]
    try:
        df_peak = datasets.get_dataframe(one_peak_url)
    except URLError as e:
        msg=''
        if hasattr(e, 'reason'):
            msg='We failed to reach a server.Reason: ' + e.reason
        elif hasattr(e, 'code'):
            msg='The server couldn\'t fulfill the request. Error code: '+ e.code
        return html.Div([html.H3('ERROR : souding data not found'),html.P(f"url : {one_peak_url}"),html.P(f"Error : {msg}")])
    peak_param = datasets.get_peak_param(sounding_id, df_oco2)
    if peak_param is None:
        return html.H1("Param of peak not found !")
    df_peak['gaussian_y'] = df_peak.distance.apply(
        lambda x: find_peak.gaussian(x=x, m=peak_param['slope'], b=peak_param['intercept'], A=peak_param['amplitude'], sig=peak_param['sigma']))

    sounding_scatter = oco2map.build_sounding_scatter(df_peak, peak_param)

    mapbox_token = config['mapbox_token']
    sounding_map = oco2map.build_sounding_map(df_peak, mapbox_token, peak_param)

    return html.Div([

        html.Div([
            html.H3('2D Scatter plot with peak detection'),
            html.P(f"m={peak_param['slope']}, b={peak_param['intercept']}, A={peak_param['amplitude']}, sig={peak_param['sigma']}"),
            dcc.Graph(
                id='xco2-graph',
                figure=sounding_scatter
            ),
            dash_dangerously_set_inner_html.DangerouslySetInnerHTML(f"<p>The estimated volume of the peak is {peak_param['ktCO2_per_h']:.4f} kilo-ton of CO<SUB>2</SUB> per hour</p>")
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
detected_peaks_url = files[last_key]['url']
previous_slider_key = last_key


######################## DATA IMPORT (In Progress) #################################################
print("- Loading peaks...")
oco2_data = datasets.get_dataframe(files[last_key]['url'])
oco2_data = gpd.GeoDataFrame(oco2_data)
oco2_data['geometry'] = oco2_data['geometry'].apply(loads)
oco2_data.crs = {'init': 'epsg:4326'}
print(oco2_data.shape[0], " peaks loaded.")

print("- Loading inventory...")
path_invent = "https://raw.githubusercontent.com/dataforgoodfr/batch7_satellite_ges/master/dataset/Output%20inventory%20data/Merge%20of%20peaks/CO2_emissions_peaks_merged_2018.csv"
invent = pd.read_csv(path_invent, sep=",", index_col=0)
invent = gpd.GeoDataFrame(invent, geometry=gpd.points_from_xy(invent.longitude, invent.latitude))
invent.crs = {'init': 'epsg:4326'}
invent = invent[invent['longitude'].notna()]
invent = invent[invent['latitude'].notna()]
invent = invent[invent['CO2/CO2e emissions (in tonnes per year)'].notna()]
print(invent.shape[0], " inventory points loaded.")

# oco2_data = datasets.get_dataframe(files[last_key]['url'])
# oco2_data = oco2_data[oco2_data.delta > 1]
print("- Creating map...")
world_map = oco2mapfolium.peaks_capture_map(oco2_data, invent)
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
    html.Iframe(id='folium-iframe', srcDoc=world_map.get_root().render(), style={'width': '100%', 'height': '400px'}),
    # Focus on a single peak
    html.Div(id='div-xco2', children=build_graph(oco2_data, None)),
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


# @app.callback(
#     dash.dependencies.Output('div-xco2', 'children'),
#     [dash.dependencies.Input('input_sounding', 'value')])
# def update_graph(sounding_id):
#     global oco2_data
#     return build_graph(oco2_data, sounding_id)


@app.callback(
    [dash.dependencies.Output('slider-output-container', 'children'),
    dash.dependencies.Output('folium-iframe', 'srcDoc'),
    dash.dependencies.Output('div-xco2', 'children')],
    [dash.dependencies.Input('my-slider', 'value'),
    dash.dependencies.Input('input_sounding', 'value')])
def update_output(slider_key, sounding_id):
    global oco2_data, world_map, previous_slider_key, detected_peaks_url
    #print('Input:', slider_key, sounding_id)
    if previous_slider_key != slider_key:
        key = sorted(files.keys())[slider_key]
        detected_peaks_url = files[key]['url']
        
        print("- Re-Loading peaks...")
        oco2_data = datasets.get_dataframe(detected_peaks_url)
        oco2_data = gpd.GeoDataFrame(oco2_data)
        oco2_data['geometry'] = oco2_data['geometry'].apply(loads)
        oco2_data.crs = {'init': 'epsg:4326'}
        print(oco2_data.shape[0], " peaks loaded.")

        # oco2_data = datasets.get_dataframe(url)
        # oco2_data = oco2_data[oco2_data.delta > 1]

        world_map = oco2mapfolium.peaks_capture_map(oco2_data, invent).get_root().render()
        previous_slider_key = slider_key
    return f'Dataset file : {detected_peaks_url}', world_map, build_graph(oco2_data, sounding_id)


if __name__ == '__main__':
    app.run_server(debug=True, threaded=True, dev_tools_hot_reload_interval=5000, dev_tools_hot_reload_max_retry=300)