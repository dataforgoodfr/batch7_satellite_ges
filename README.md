# batch7_satellite_ges

* /dataset contains a sample of OCO-2 data and inventory data;
* /notebooks centralised notebooks make by the team;
* /pipeline contains scripts used to generate the data needed.

# General presentation

Our goal is to localize CO² emissions on Earth.
We are working with:
- Matthieu Porte, from IGN who submit the projet
- Marie Heckmann, from the French Ministry of Ecology
- Frederic Chevalier, from IPSL, one of the author of <https://www.atmos-chem-phys-discuss.net/acp-2020-123/acp-2020-123.pdf>


# What we have as input

**1/ OCO-2 Satellite data**

The OCO-2 Satellite (Orbiting Carbon Observatory) use spectrometers to detect CO² in the atmosphere.
![OCO2 spectrometers](https://oco.jpl.nasa.gov/media/uploads/2019/05/07/oco_column.jpg)

More info here : <https://oco.jpl.nasa.gov/instrument/>

The data available corresponds to the CO² concentration measured by the satellite during its different orbits. 

Here is a visusalisaiton of the CO² concentration mesured by the OCO-2 satellite in December 2019. 
![CO2_ concentration_OCO2](https://user-images.githubusercontent.com/61688979/79854803-4c012f80-83ca-11ea-921e-49dcbf55440f.PNG)

There are some limitations to the satellite measurement of the CO² concentration.
First, the satellite can not see through clouds or fog.
Second, it does not work the same over ground or water.
Third, the swath of the satellite is quite narrow (only 10km), as shown in the image bellow. 
Finally, as the satellite orbits the Earth, the coverage is partial.

![OCO2 spectrometers](https://scx1.b-cdn.net/csz/news/800/2020/3-nasasatellit.jpg)
!!

More info on the mission on <https://earth.esa.int/web/eoportal/satellite-missions/o/oco-2>.

The NASA made a global CO² image (see bellow), however this is an extrapolation of the data, and not what the satellite really see.

![NASA Global CO²](https://www.jpl.nasa.gov/images/oco/20090219/sinks-browse.jpg)

**2/ Data on known CO2 emissions**

- The Emissions Database for Global Atmospheric Research (EDGAR) on CO2 emissions. For the energy related sectors the activity data is mainly based on the energy balance statistics of IEA (2017), whereas the activity data for the agricultural sectors originates mainly from FAO (2018). The spatial allocation of emissions on the grid is made based on spatial proxy datasets with the location of energy and manufacturing facilities, road networks, shipping routes, human and animal population density and agricultural land use, that vary over time. 
Source : https://edgar.jrc.ec.europa.eu/overview.php?v=50_GHG

![CO2_emissions_Edgar_2018](https://user-images.githubusercontent.com/61688979/79775474-9637d180-8334-11ea-9712-274a11356aea.PNG)

- The World Resource Institute provides a list of power plants producing electricity based on different primary energies. We filtered this list to keep only the fossil primary energies (gas, oil and coal), that release CO2 during their combustion.
Source: http://datasets.wri.org/dataset/globalpowerplantdatabase

![power_plant_emissions_2017](https://user-images.githubusercontent.com/61688979/79775550-b5366380-8334-11ea-9587-8d42b241160e.PNG)

# What we want to do

- Detect peak in data by looking at CO² Plume
- Agregate known sources of CO²
- Compare peak to known sources to correct them
- Machine Learning to find new source
- Display the result on a comprehensive map

# What we have achieved

- XXX Picture of CO² Plume

# What is comming next

- Find nearest inventory from peak position, using the wind vector.

# We need help

- Better peak detection: So far, we are fitting gaussian curves to detect relevant peaks. 2 issues:
    - we use SciKit Learn curve_fit. Do you know a better algorithme or how to tune parameters of curve_fit ?
    - we are looking at other methodologies to detect anomalies (our 'peaks') in the concentrations  - any idea? 
- Wind modeling to estimate emission from detected concentration - any idea? (inverting the gaussian plume model)
- Interactive dasboard to share our work on the web (Streamlit ?)
