# Global Power Plant Database

This project aims to build [an open database of all the power plants in the world](http://www.wri.org/publication/global-power-plant-database). It is the result of a large collaboration involving many partners, coordinated by the [World Resources Institute](https://www.wri.org/) and [Google Earth Outreach](https://www.google.com/earth/outreach/index.html). If you would like to get involved, please [email the team](mailto:powerexplorer@wri.org) or fork the repo and code! To learn more about how to contribute to this repository, read the [`CONTRIBUTING`](https://github.com/wri/global-power-plant-database/blob/master/.github/CONTRIBUTING.md) document.

The latest database release (v1.2.0) is available in CSV format [here](http://datasets.wri.org/dataset/globalpowerplantdatabase) under a [Creative Commons-Attribution 4.0 (CC BY 4.0) license](https://creativecommons.org/licenses/by/4.0/). A bleeding-edge version is in the [`output_database`](https://github.com/wri/global-power-plant-database/blob/master/output_database) directory of this repo.

All Python source code is available under a [MIT license](https://opensource.org/licenses/MIT).

This work is made possible and supported by [Google](https://environment.google/), among other organizations.

## Database description

The Global Power Plant Database is built in several steps.

* The first step involves gathering and processing country-level data. In some cases, these data are read automatically from offical government websites; the code to implement this is in the `build_databases` directory.
* In other cases we gather country-level data manually. These data are saved in `raw_source_files/WRI` and processed with the `build_database_WRI.py` script in the `build_database` directory. 
* The second step is to integrate data from different sources, particularly for geolocation of power plants and annual total electricity generation. Some of these different sources are multi-national databases. For this step, we rely on offline work to match records; the concordance table mapping record IDs across databases is saved in resources/master_plant_concordance.csv.

Throughout the processing, we represent power plants as instances of the `PowerPlant` class, defined in `powerplant_database.py`. The final database is in a flat-file CSV format.

## Key attributes of the database

The database includes the following indicators:

* Plant name
* Fuel type(s)
* Generation capacity
* Country
* Ownership
* Latitude/longitude of plant
* Data source & URL
* Data source year
* Annual generation

We will expand this list in the future as we extend the database.

### Fuel Type Aggregation

We define the "Fuel Type" attribute of our database based on common fuel categories. In order to parse the different fuel types used in our various data sources, we map fuel name synonyms to our fuel categories [here](https://github.com/wri/global-power-plant-database/blob/master/resources/fuel_type_thesaurus). We plan to expand the database in the future to report more disaggregated fuel types.

## Combining Multiple Data Sources

A major challenge for this project is that data come from a variety of sources, including government ministries, utility companies, equipment manufacturers, crowd-sourced databases, financial reports, and more. The reliability of the data varies, and in many cases there are conflicting values for the same attribute of the same power plant from different data sources. To handle this, we match and de-duplicate records and then develop rules for which data sources to report for each indicator. We provide a clear [data lineage](https://en.wikipedia.org/wiki/Data_lineage) for each datum in the database. We plan to ultimately allow users to choose alternative rules for which data sources to draw on.

To the maximum extent possible, we read data automatically from trusted sources, and integrate it into the database. Our current strategy involves these steps:

* Automate data collection from machine-readable national data sources where possible. 
* For countries where machine-readable data are not available, gather and curate power plant data by hand, and then match these power plants to plants in other databases, including GEO and CARMA (see below) to determine their geolocation.
* For a limited number of countries with small total power-generation capacity, use data directly from Global Energy Observatory (GEO). 

A table describing the data source(s) for each country is listed below.

Finally, we are examining ways to automatically incorporate data from the following supra-national data sources:

* [Clean Development Mechanism](https://cdm.unfccc.int/Projects/projsearch.html)
* [ENTSO-E](https://www.entsoe.eu/Pages/default.aspx)
* [E-PRTR](http://prtr.ec.europa.eu/)
* [CARMA](http://carma.org/)
* [Arab Union of Electricity](http://www.auptde.org/Default.aspx?lang=en)
* [IAEA PRIS](https://www.iaea.org/pris/)
* [Industry About](http://www.industryabout.com/energy)
* [Think Geo Energy](http://www.thinkgeoenergy.com/map/)
* [WEC Global Hydropower Database](https://www.worldenergy.org/data/resources/resource/hydropower/)

## ID numbers

We assign a unique ID to each line of data that we read from each source. In some cases, these represent plant-level data, while in other cases they represent unit-level data. In the case of unit-level data, we commonly perform an aggregation step and assign a new, unique plant-level ID to the result. For plants drawn from machine-readable national data sources, the reference ID is formed by a three-letter country code [ISO 3166-1 alpha-3](http://unstats.un.org/unsd/tradekb/Knowledgebase/Country-Code) and a seven-digit number. For plants drawn from other database (including the manually-maintained dataset by WRI), the reference ID is formed by a variable-size prefix code and a seven-digit number.

## Power plant matching

In many cases our data sources do not include power plant geolocation information. To address this, we attempt to match these plants with the GEO and CARMA databases, in order to use that geolocation data. We use an [elastic search matching technique](https://github.com/cbdavis/enipedia-search) developed by Enipedia to perform the matching based on plant name, country, capacity, location, with confirmed matches stored in a concordance file. This matching procedure is complex and the algorithm we employ can sometimes wrongly match two power plants or fail to match two entries for the same power plant. We are investigating using the Duke framework for matching, which allows us to do the matching offline.
 
## Related repos

* [Open Power Systems Data](https://github.com/Open-Power-System-Data/)
* [Public Utility Data Liberation Project](https://github.com/catalyst-cooperative/pudl)
* [Global Energy Observatory](https://github.com/hariharshankar/pygeo)
* [GeoNuclearData](https://github.com/cristianst85/GeoNuclearData)
* [Duke](https://github.com/larsga/Duke)
