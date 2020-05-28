# OCO-2 CO2 peak detector



## General presentation
> The goal of our project is to localize CO² emissions on Earth based on the the carbon concentration data measured by the OCO-2 Satellite from the NASA. 

We are working with:- Matthieu Porte, from IGN who submit the projet- Marie Heckmann, from the French Ministry of Ecology
- Frederic Chevalier, from IPSL, one of the author of [Observing  carbon  dioxide  emissions over  China’s  cities with  the Orbiting Carbon Observatory-2](https://www.atmos-chem-phys-discuss.net/acp-2020-123/acp-2020-123.pdf)

## What we have as input

**1/ OCO-2 Satellite data**



The OCO-2 Satellite (Orbiting Carbon Observatory) from the NASA orbits around Earth and measures the CO² concentration in the atmosphere.  

Here is a visualisation of the CO² concentration mesured by the OCO-2 satellite in December 2019. 
![CO2_ concentration_OCO2](notebooks/assets/CO2_emissions_Edgar_2018.png)

The satellite uses spectrometers to detect CO² in the atmosphere, as shown in the image bellow.

![OCO2 spectrometers](https://upload.wikimedia.org/wikipedia/commons/thumb/4/44/Artist_rendition_of_the_CO2_column_that_OCO-2_will_see.jpg/321px-Artist_rendition_of_the_CO2_column_that_OCO-2_will_see.jpg)

[source](https://commons.wikimedia.org/wiki/File:Artist_rendition_of_the_CO2_column_that_OCO-2_will_see.jpg)

More info here : <https://oco.jpl.nasa.gov/instrument/>

There are some limitations to the satellite measurement of the CO² concentration:
- The satellite can not see through clouds or fog;
- It does not work the same over ground or water;
- The swath of the satellite is quite narrow (only 10km), as shown in the image bellow; 
- As the satellite orbits the Earth, the coverage is partial.

![OCO2 spectrometers](https://scx1.b-cdn.net/csz/news/800/2020/3-nasasatellit.jpg)
!!

More info on the mission on <https://earth.esa.int/web/eoportal/satellite-missions/o/oco-2>.

The NASA made a global CO² image (see bellow), however this is an extrapolation of the data, and not what the satellite really see.

![NASA Global CO²](https://www.jpl.nasa.gov/images/oco/20090219/sinks-browse.jpg)

**2/ Data on known CO2 emissions**

- The Emissions Database for Global Atmospheric Research (EDGAR) on CO² emissions. For the energy related sectors the activity data is mainly based on the energy balance statistics of IEA (2017), whereas the activity data for the agricultural sectors originates mainly from FAO (2018). The spatial allocation of emissions on the grid is made based on spatial proxy datasets with the location of energy and manufacturing facilities, road networks, shipping routes, human and animal population density and agricultural land use, that vary over time. 
Source : https://edgar.jrc.ec.europa.eu/overview.php?v=50_GHG

![CO2_emissions_Edgar_2018](https://user-images.githubusercontent.com/61688979/79775474-9637d180-8334-11ea-9712-274a11356aea.PNG)

- The World Resource Institute provides a list of power plants producing electricity based on different primary energies. We filtered this list to keep only the fossil primary energies (gas, oil and coal), that release CO² during their combustion.
Source: http://datasets.wri.org/dataset/globalpowerplantdatabase

![power_plant_emissions_2017](notebooks/assets/power_plant_emissions_2017.png)

- Other sources of CO² emissions are under study. 

## What we want to do


First approach: peak detection from O-CO2 & inference from inventory data [in progress]

- Detect peak in O-CO2 data, 2 step methodology
	- Step 1: Identification of local ‘peaks’ through Gaussian fits (curve_fit) ; Taking into account intrinsic complexity of O-CO2 data, notably: High variance across ‘background’ CO² level across the globe, narrowness & incompleteness of plumes observations (due to clouds / fogs / …), ...
	- Step 2: Elimination of irrelevant peaks to keep only ‘true’ anomalies: So far, through a quite drastic & manual methodology, with rules to keep only clear Gaussians ; Objective to improve this part with algo-based anomaly detection 

- Aggregate known sources of CO² from inventory data: Using EDGAR & World Resource Institute

- Find nearest inventory from peak position, using the wind vector.

- Compare peak to known sources emissions and confirm them

Second approach: supervised model to learn to detect peaks from inventory data [not started]
- Use areas where inventory data are complete to let a supervised model learn peaks in OCO2 data

On top: dynamic visualization of data [in progress]
- Display the result on a comprehensive map, crossing satellite & inventory data

## What we have achieved

 - We gather data from EDGAR and World Resource Institute and plotted them on a map.
 - We get raw satellite data from NASA and merge the to monthly dataset with the data we need.
 - We compute a Gaussian curve fit over each orbit and save the results.
 - We plot the results and the know emission on a map.

Here is a sample of a peak witth the gaussian found :
![Gaussian Peak](notebooks/assets/gaussian_peak.png)

And the global map :
![World CO2_peaks](notebooks/assets/map-dark.png)
![CO2_peak over Spain](notebooks/assets/map-dark-orbit.png)


## We need help

- Better peak detection: So far, we are fitting Gaussian curves to detect relevant peaks. 2 issues:
    - We use SciKit Learn curve_fit. Do you know a better algorithme or how to tune parameters of curve_fit ?
    - We are looking at other methodologies to detect anomalies (our 'peaks') in the concentrations  - any idea? 
- Wind modeling to estimate emission from detected concentration - any idea? (inverting the Gaussian plume model)
- Interactive dasboard to share our work on the web (Streamlit ?)

## Git directories structure
* /dataset contains a sample of OCO-2 data and inventory data; _**Important**_ : The whole datas are in a shared Open Stack Storage, not in the Github.
* /notebooks contains the notebooks made by the team;
* /pipeline contains the scripts used to process the data needed.
* /oco2peak containts the modules

**Warning** : The project use NBDev so the doc (including this README !) and the modules ar generated from Notebooks. So you have only to edit the Notebooks.

## Open Stack directories structure

We do not store the original OCO-2 files from NASA.

* /emissions/ contains all the potential source of emissions : factories, power plants, cities...
* /soudings/ contains CSV of the raw features extracted from NASA NC4 files.
* /peaks-detected/ contains all the peak found in the satellite orbit datas.
* /peaks-detected-details/ contains one JSON file of the full data for all detected peak

## Install
### Docker

`
docker-compose build && docker-compose up
`

Front on http://localhost:7901
Jupyter Lab on http://localhost:7988

### Python Package Only
If you are interested to use only our modules for your own project :

`pip install oco2peak`

## How to use

Fill me in please! Don't forget code examples:

### Dataset access

```python
config = '../configs/config.json'
datasets = Datasets(config)
datasets.get_files_urls('result_for_oco2_1808')
```




    ['https://storage.gra.cloud.ovh.net/v1/AUTH_2aaacef8e88a4ca897bb93b984bd04dd/oco2//datasets/oco-2/peaks-detected/result_for_oco2_1808.csv']



### Upload a file

```python
datasets.upload(mask='../*.md', prefix="/Trash/",content_type='text/text')
```

    100%|██████████| 2/2 [00:00<00:00,  6.04it/s]

