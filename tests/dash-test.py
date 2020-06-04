import dash
import dash_html_components as html
import dash_core_components as dcc
import json
import re
import swiftclient
import plotly.graph_objects as go
import pandas as pd
import dash_leaflet as dl
from bs4 import BeautifulSoup
import dash_dangerously_set_inner_html
import folium
import numpy as np

# Read config file
with open("../config.json") as json_data_file:
    config = json.load(json_data_file)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


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

def load_data(file):
    
    df = pd.read_csv(file)
    df['sounding_id']= df['sounding_id'].astype(str)
    return df

# from datetime import datetime
# def to_date(a):
#     return datetime.strptime(str(a), '%Y%m%d%H%M%S%f')

# data['date'] = data['sounding_id'].apply(to_date)
# data.head()

# connect to Open Stack Swift Object Storage
swift_conn = swift_con(config)

# Retrieve file list
files = {}

objects = swift_conn.get_container('oco2')[1]

for data in objects:
    if '/peaks-detected/' in data['name']:
        #print('{0}\t{1}\t{2}'.format(data['name'], data['bytes'], data['last_modified']))
        yearmonth = re.findall(r'(\d{4})', data['name'])[-1]
        yearmonth_text = yearmonth[2:4] + '/20' +yearmonth[0:2]
        files.update(
            {yearmonth : {
                'url' : config['swift_storage']['base_url']+data['name'],
                'label' : yearmonth_text
                }
            }
        )

yearmonth_marks = {}

for i, key in enumerate(sorted(files.keys())):
        yearmonth_text = files[key]['label']
        if i == 0 or i == len(files)-1:
            yearmonth_marks.update({i: {'label': yearmonth_text}})
        elif key[2:4] == '01': # Only years
            yearmonth_marks.update({i: {'label': '20' + key[0:2]}})


#print('yearmonth_marks', yearmonth_marks)


#import plotly.graph_objects as go # or plotly.express as px
fig = go.Figure() # or any Plotly Express function e.g. px.bar(...)
# fig.add_trace( ... )
# fig.update_layout( ... )
#df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/2014_us_cities.csv')

# key = sorted(files.keys())[-1]
# url = files[key]['url']
# fig = go.Figure()

def build_graph(df_oco2, sounding_id):
    # https://storage.gra.cloud.ovh.net/v1/AUTH_2aaacef8e88a4ca897bb93b984bd04dd/oco2//datasets/oco-2/peaks-detected-details/peak_data-si_2016061413390672.json
    
    if sounding_id is None:
        return html.H1("Please select a point")
    if len(sounding_id)!=16 :
        print(len(sounding_id))
        return html.H1("Wrong sounding_id format !")
    df = pd.read_json(config['swift_storage']['base_url']+'/datasets/oco-2/peaks-detected-details/peak_data-si_'+sounding_id+'.json')
    gaussian_param = get_gaussian_param(sounding_id, df_oco2)
    df['gaussian_y'] = df.distance.apply(
        lambda x: gaussian(x=x, m=gaussian_param['slope'], b=gaussian_param['intercept'], A=gaussian_param['amplitude'], sig=gaussian_param['sigma']))
    
    center_lat = df.latitude.min() + (df.latitude.max() - df.latitude.min())
    center_lon = df.longitude.min() + (df.longitude.max() - df.longitude.min())
    print(df.latitude.max(), df.latitude.min(), df.longitude.max(), df.longitude.min())
    print(center_lat, center_lon)
    xco_sounding_mapbox = go.Figure(go.Scattermapbox(
        lat=df.latitude,
        lon=df.longitude,
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=14
        ),
        text=df.xco2,
    ))
    xco_sounding_mapbox.update_layout(
    hovermode='closest',
    mapbox=dict(
        accesstoken=config['mapbox_token'],
        bearing=0,
        center=go.layout.mapbox.Center(
            lat=center_lat,
            lon=center_lon
        ),
        pitch=0,
        zoom=5
    )
)
# {
#                 'data': [{
#                     'lat': df.latitude, 'lon': df.longitude,
#  'type': 'scattermapbox'
#                 }],
#                 'layout': {
#                     'mapbox': {
#                         'accesstoken': (config['mapbox_token']),
#                         'center' : go.layout.mapbox.Center(lat=center_lat, lon=center_lon),
#                     },
#                     'margin': {
#                         'l': 0, 'r': 0, 'b': 0, 't': 0
#                     },
#                 }
#             }

    return [dcc.Graph(
        id='xco2-graph',
        figure={
            'data': [
                go.Scatter(
                    x=df['distance'],
                    y=df['xco2'],
                    text='xco2',
                    mode='markers',
                    opacity=0.5,
                    marker={
                        'size': 5,
                        'line': {'width': 0.5, 'color': 'white'}
                    },
                    name="xco2"
                ),
                go.Scatter(x=df['distance'], y=df['gaussian_y'], name="Gaussian fit",
                    hoverinfo='name',
                    line_shape='spline')
            ],
            'layout': go.Layout(
                xaxis={'title': 'Distance (km)'},
                yaxis={'title': 'CO² level in ppm'},
                #margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
                legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }
    ),
    dcc.Graph(
            id='xco_sounding_mapbox',
            figure=xco_sounding_mapbox
        ),
    # dcc.Graph(
    #     id='xco_sounding_map',
    #     figure={
    #         'data': [
    #         go.Scattergeo(
    #         locationmode = 'ISO-3',
    #         lon = df['longitude'],
    #         lat = df['latitude'],
    #         text = df['xco2'],
    #         marker = dict(
    #             size = 15,
    #             color = df['xco2'],
    #             line_color='rgb(40,40,40)',
    #             line_width=0.5,
    #             sizemode = 'area'
    #         ))],
    #         'layout': go.layout.Geo(
    #             fitbounds="locations",
    #             projection = go.layout.geo.Projection(
    #                 type = 'azimuthal equal area',
    #                 scale=5
    #             ),
    #             center={'lat': 45.5016889, 'lon': -80.56725599999999},
    #             showcoastlines=True, coastlinecolor="RebeccaPurple",
    #             showland=True, landcolor="LightGreen",
    #             showocean=True, oceancolor="LightBlue",
    #             showlakes=True, lakecolor="Blue",
    #             showrivers=True, rivercolor="Blue",
    #             countrycolor = 'rgb(204, 204, 204)',
    #         ),
    #     }),
    # dcc.Graph(
    #     id='xco_sounding_3d',
    #     figure={
    #         'data': [
    #             go.Surface(z=df['xco2'].values.reshape(len(df)//2,len(df)//2))
    #         ]
    #     })
    ]
     

def build_folium_map(data):
    folium_map = folium.Map([43, 0], zoom_start=4)
    folium.TileLayer("CartoDB dark_matter", name="Dark mode").add_to(folium_map)

    # Adding detected peaks
    peaks_group = folium.FeatureGroup(name="Peaks").add_to(folium_map)
    peaks_group_circle = folium.FeatureGroup(name=" - 50km Circles").add_to(folium_map)
    peaks_group_wind = folium.FeatureGroup(name=" - Wind Vectors").add_to(folium_map)
    
    for _, row in data.iterrows():
        radius = row["amplitude"]/20
        color="#FF3333" # red
        tooltip =  "GPS : ["+str(round(row['latitude'],2))+" ; "+str(round(row['longitude'],2))+"]"
        sounding = str(row['sounding_id'])
        date = str(row['sounding_id'])
        orbit = str(row['orbit'])
        wind = [[row['latitude'],row['longitude']],[row['latitude']+row['windspeed_u'],row['longitude']+row['windspeed_v']]]
        
        popup_html="""<h4>"""+tooltip+"""</h4>"""+date+"""<p>sounding_id: """+sounding+"""</br>orbit: """+orbit+"""</p>"""
        popup_html+='<p><input type="button" value="Show plot"'
        # Injecting JavaScript in popup to fire the Dash Callback
        popup_html+='onclick="\
            let bco_input = parent.document.getElementById(\'input_sounding\'); \
            let lastValue = bco_input.value;'
        popup_html+=f'bco_input.value = \'{sounding}\';'
        popup_html+="let bco_event = new Event('input', { bubbles: true });\
            bco_event.simulated = true;\
            let tracker = bco_input._valueTracker;\
            if (tracker) {\
            tracker.setValue(lastValue);\
            }\
            bco_input.dispatchEvent(bco_event);\
            elt.dispatchEvent(new Event('change'));\
            \"/></p>"
        #onclick="plot_data(\'{url}\', {slope}, {intercept}, {amplitude}, {sigma});"/></p>'

        popup=folium.Popup(popup_html, max_width=450)
        
        peaks_group.add_child(folium.CircleMarker(location=(row["latitude"],
                                    row["longitude"]),
                            radius=radius,
                            color=color,
                            tooltip=tooltip,
                            popup=popup,
                            fill=True))

        peaks_group_circle.add_child(folium.Circle(location=(row["latitude"],
                            row["longitude"]),
                            radius=50000,
                            color='#949494',
                            weight = 1,
                            fill=False))
        
        peaks_group_wind.add_child(folium.PolyLine(wind,
                        color='#B2B2B2',
                        weight = 1))
    return folium_map


last_key = sorted(files.keys())[-1]

oco2_data = load_data(files[last_key]['url'])
default_folium_map = build_folium_map(oco2_data)
# soup = BeautifulSoup(default_folium_map.get_root().render(), 'html.parser')
# for tag in soup.head.find_all('link'):
#     #print(tag)
#     try:
#         #print('Keeping', tag['href'])
#         external_stylesheets.append(tag['href'])
#     except KeyError:
#         continue
#print(external_stylesheets)
# folium_body = ''.join(str(t) for t in soup.body.contents)
# folium_script = ''.join(str(t) for t in soup.body.find_next_siblings("script"))

# folium_map_body = folium_body + folium_script

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div(
    [
    html.H1(children='OCO-2 satellite data analysis'),
    html.Div(children='''
        The goal of our project is to localize CO² emissions on Earth based on the the carbon concentration data measured by the OCO-2 Satellite from the NASA.
    '''),
    html.Div(dash_dangerously_set_inner_html.DangerouslySetInnerHTML('''
    <ul>
        <li>The map shows the places where we detect a peak in CO² emission based on OCO-2 satellite data.</li>
        <li>We also plot the potential CO² source from declarative content (EDGAR, IEA, FAO...).</li>
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
        marks=yearmonth_marks
    ),
    html.Div(id='slider-output-container'),
    #html.Div(dash_dangerously_set_inner_html.DangerouslySetInnerHTML(folium_map_body)),
    #html.Div(id='oco2-folium-map',children=folium_map_body),
    #html.Iframe(id='oco2-folium-map-iframe', srcDoc=folium_map_body, style={'width': '100%', 'height': '800px'}),

    html.Iframe(id='folium-iframe', srcDoc=default_folium_map.get_root().render(), style={'width': '100%', 'height': '400px'}),
    
    html.Div(id='div-xco2', children=build_graph(None, None)),
    #dl.Map(dl.TileLayer(), style={'width': '1000px', 'height': '500px'}),
    #dcc.Graph(id='slider-output-container', figure=fig)
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
    oco2_data = load_data(url)
    return f'Dataset file : {url}', build_folium_map(oco2_data).get_root().render()


if __name__ == '__main__':
    app.run_server(debug=True)