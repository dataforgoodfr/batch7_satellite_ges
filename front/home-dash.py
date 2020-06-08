import flask
import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import dash_dangerously_set_inner_html
import json
import re
import requests
from urllib.error import URLError

import pandas as pd
import geopandas as gpd
from shapely.wkt import loads

# Our modules
from oco2peak.datasets import Datasets
from oco2peak import oco2map
from oco2peak import oco2mapfolium
from oco2peak import find_peak

import locale
locale.setlocale(locale.LC_ALL, '')

# Read config file
config_file = "./configs/config.json"
with open(config_file) as json_data_file:
    config = json.load(json_data_file)
mapbox_token = config['mapbox_token']
datasets = Datasets(config_file)

# Retrieve file list
def get_detected_peak_file_list(datasets):
    files = {}
    detected_peaks_urls = datasets.get_files_urls(prefix="/datasets/oco-2/peaks-and-invent/", pattern='peaks_and_invent')
    for detected_peaks_url in detected_peaks_urls:
        # RegExp to get 4 digits followed by a dot
        date = re.findall(r'(\d{4})\.', detected_peaks_url)[-1]
        year = '20' + date[0:2]
        month = date[2:4]
        yearmonth_text = month + '/' +year
        files.update(
            {date : {
                'url' : detected_peaks_url,
                'label' : yearmonth_text,
                'year' : year,
                'month' : month
                }
            }
        )
    return files

# Build the mark for the slider 
def get_slider_mark(files):
    yearmonth_marks = {}
    for i, key in enumerate(sorted(files.keys())):
            yearmonth_text = files[key]['label']
            #print(yearmonth_text)
            #print(key[2:4])
            if i == 0 or i == len(files)-1:
                yearmonth_marks.update({i: {'label': yearmonth_text}})
            elif key[2:4] == '01': # Only years
                yearmonth_marks.update({i: {'label': '01/20' + key[:2]}})
            elif key[2:4] == '06': # June
                yearmonth_marks.update({i: {'label': '06'}})
            else:
                yearmonth_marks.update({i: {'label': ''}})
    return yearmonth_marks

def build_graph(df_oco2, sounding_id):
    # https://storage.gra.cloud.ovh.net/v1/AUTH_2aaacef8e88a4ca897bb93b984bd04dd/oco2//datasets/oco-2/peaks-detected-details/peak_data-si_2016061413390672.json
    #print("build_graph")
    if sounding_id is None:
        # Get the peak with maximum mass of CO2
        #sounding_id = df_oco2.loc[df_oco2.ktCO2_per_h==df_oco2.ktCO2_per_h.max()].iloc[0].sounding_id.astype('int64')
        return html.H3("Please select a point")
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
    #print(peak_param)
    if peak_param is None:
        print('One sounding ID of the df:',df_oco2.loc[df_oco2.ktCO2_per_h==df_oco2.ktCO2_per_h.max()].iloc[0].sounding_id.astype('int64'), 'Sounding asked:',sounding_id)
        return html.H1("Metadata of the peak not found !")
    df_peak['gaussian_y'] = df_peak.distance.apply(
        lambda x: find_peak.gaussian(x=x, m=peak_param['slope'], b=peak_param['intercept'], A=peak_param['amplitude'], sig=peak_param['sigma']))

    sounding_scatter = oco2map.build_sounding_scatter(df_peak, peak_param)

    
    sounding_map = oco2map.build_sounding_map(df_peak, mapbox_token, peak_param)

    return html.Div([
        html.Div([
            html.H3('Satellite view of the data'),
            dcc.Graph(
                id='xco_sounding_mapbox',
                figure=sounding_map
            )
        ], className="col-lg-4"),
        html.Div([
            html.H3('Satellite data for 100km around the peak'),
            #html.P(f"m={peak_param['slope']}, b={peak_param['intercept']}, A={peak_param['amplitude']}, sig={peak_param['sigma']}"),
            dcc.Graph(
                id='xco2-graph',
                figure=sounding_scatter
            ),
            dash_dangerously_set_inner_html.DangerouslySetInnerHTML(f"<p id='mass'>The estimated mass of the peak is <b>{peak_param['ktCO2_per_h']*1000:n} ton of CO<SUB>2</SUB> per hour</b></p>")
        ], className="col-lg-8")
    ], className="row")

files = get_detected_peak_file_list(datasets)
last_key = sorted(files.keys())[-1]
detected_peaks_url = files[last_key]['url']
previous_slider_key = len(files)-1

#### DATA IMPORT (In Progress)
#print("- Loading peaks...")
url = files[last_key]['url']
oco2_data = datasets.get_peaks(url, delta_threshold=1)


def get_folium_iframe(year, month):
    map_url = 'http://storage.gra.cloud.ovh.net/v1/AUTH_2aaacef8e88a4ca897bb93b984bd04dd/oco2//map/peaks_map/'\
        + 'peaks_capture_map_' + year[2:4] + month +'.html'
    #print(map_url)
    r = requests.get(map_url)
    #print(r.status_code)
    # >>> r.headers['content-type']
    # 'application/json; charset=utf8'
    # >>> r.encoding
    # 'utf-8'
    return html.Iframe(id='folium-iframe', srcDoc=r.text, style={'width': '100%', 'height': '400px'}) # world_map.get_root().render()
world_map_display = get_folium_iframe(files[last_key]['year'], files[last_key]['month'])
print("Map created.")
######################## DATA IMPORT (In Progress)  #################################################



###############################################################################
######################## DASH #################################################
# Light theme
# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
# Dark theme
external_stylesheets=[dbc.themes.DARKLY]

server = flask.Flask(__name__) # define flask app.server
app = dash.Dash(__name__, external_stylesheets=external_stylesheets, server=server) # call flask server
app.title = 'CO2 plume detector'

app.layout = html.Div(
    [
    html.H1(children='OCO-2 satellite data analysis'),
    html.Div(children=dash_dangerously_set_inner_html.DangerouslySetInnerHTML('''
        The goal of our project is to localize CO<SUB>2</SUB> emissions on Earth based on the carbon concentration data measured by the OCO-2 Satellite from the NASA.
    ''')),
    html.Div(dash_dangerously_set_inner_html.DangerouslySetInnerHTML('''
    <ul>
        <li>The map shows in red the places where we detect a peak in CO<SUB>2</SUB> emission based on OCO-2 satellite data.</li>
        <li>We also plot the potential CO<SUB>2</SUB> source from declarative content (EDGAR, IEA, FAO...), in green.</li>
        <li>You can select a month of observations with the slider below.</li>
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
    dcc.Loading(
            id="loading-1",
            type="default",
            children=html.Div(id="loading-output-map"),
            fullscreen=False
        )
    ,
    # Focus on a single peak
    html.Div(id='div-xco2', children=build_graph(oco2_data, None)),
    # Input of a peak ID
    html.Div(className='souding_id_div', children=[
    html.P("ID of OCO-2 sounding : "),
    dcc.Input(
        id="input_sounding",
        type="text",
        placeholder="2018082510150705",
    ),
    html.Div(children='''
        Made with Dash: A web application framework for Python.
    ''')
    ])
], className="oco2app")

@app.callback(
    [dash.dependencies.Output('slider-output-container', 'children'),
    #dash.dependencies.Output('folium-iframe', 'srcDoc'),
    dash.dependencies.Output('loading-output-map', 'children'),
    
    dash.dependencies.Output('div-xco2', 'children')],
    [dash.dependencies.Input('my-slider', 'value'),
    dash.dependencies.Input('input_sounding', 'value')])
def update_output(slider_key, sounding_id):
    #print("update_output")
    global oco2_data, previous_slider_key, detected_peaks_url, world_map_display
    key = sorted(files.keys())[slider_key]
    #print('Input: previous_slider_key=',previous_slider_key,'slider_key=', slider_key, 'sounding_id=', sounding_id)
    if previous_slider_key != slider_key:
        print("- Month change", files[key]['month'], '/', files[key]['year'])
        detected_peaks_url = files[key]['url']
        oco2_data = datasets.get_dataframe(detected_peaks_url)
        oco2_data = oco2_data[oco2_data.delta > 1]
        # oco2_data = gpd.GeoDataFrame(oco2_data)
        # oco2_data['geometry'] = oco2_data['geometry'].apply(loads)
        # oco2_data.crs = {'init': 'epsg:4326'}
        # print(oco2_data.shape[0], " peaks loaded.")
        # world_map = oco2mapfolium.peaks_capture_map(oco2_data, invent)
        previous_slider_key = slider_key
        # world_map_display = html.Iframe(id='folium-iframe', srcDoc=world_map.get_root().render(), style={'width': '100%', 'height': '400px'})
        world_map_display = get_folium_iframe(files[key]['year'], files[key]['month'])
        sounding_id = None
        #print('Map updated')
    else:
        print("No month change, only souding_id :", sounding_id)

    return f"Map for data from : {files[key]['month']}/{files[key]['year']}", world_map_display, build_graph(oco2_data, sounding_id)


if __name__ == '__main__':
    production = False
    if production:
        #app.run_server(debug=True)
        app.run_server(host='0.0.0.0', debug=False, threaded=True, dev_tools_hot_reload_interval=50000, dev_tools_hot_reload_max_retry=3)
    else:
        app.run_server(host='0.0.0.0', debug=True, threaded=True, dev_tools_hot_reload_interval=5000, dev_tools_hot_reload_max_retry=300)