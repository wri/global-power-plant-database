# Capstone Project: Analyzing Water Risk Exposure of Global Power Plant Infrastructure

## Project Overview

This repository hosts the work for a Data Science Capstone project focused on analyzing the exposure of global power plant infrastructure to current and future water-related risks. The primary goal is to integrate detailed data on power plants worldwide with comprehensive water risk indicators from WRI's Aqueduct 4.0. This will allow for the identification of potential vulnerabilities and highlight areas of concern at the intersection of energy production and water scarcity.

This repository is a fork of the World Resources Institute's (WRI) [Global Power Plant Database](https://github.com/wri/global-power-plant-database) and utilizes data from WRI's [Aqueduct 4.0 Water Risk Atlas](https://www.wri.org/aqueduct).

## The Problem

A significant portion of global electricity generation, particularly thermal power plants (coal, gas, nuclear) and hydropower, relies heavily on adequate and reliable water resources for processes such as cooling, steam generation, and direct energy production. The intricate relationship between water and energy production is often referred to as the "water-energy nexus."

While this general dependence is known, a comprehensive, granular understanding of the specific water risk exposure faced by individual power plants worldwide—both currently and under future climate and socioeconomic scenarios—is often lacking or fragmented. This project aims to address this gap by identifying and quantifying the varying levels of water risk (such as water stress, depletion, and variability) faced by a global portfolio of power plants. The specific problem is to assess which types of power generation facilities, in which specific geographical basins, are most vulnerable to current and projected future water challenges, thereby providing insights into potential threats to energy security and production.

## Why This Problem Matters

Understanding and quantifying the water risk exposure of power plants is critical for several reasons:

*   **Energy Security:** Water scarcity or variability can lead to reduced operational efficiency of power plants, temporary shutdowns, or even long-term unviability, directly impacting energy supply and grid reliability.
*   **Economic Impacts:** Disruption in power generation due to water issues can have significant economic consequences, affecting industrial output, commercial activities, and incurring costs associated with mitigation or alternative energy sourcing.
*   **Sustainable Development:** This analysis aligns with Sustainable Development Goals (SDGs) concerning clean water and sanitation (SDG 6), affordable and clean energy (SDG 7), and climate action (SDG 13).
*   **Investment and Policy:** A clearer understanding of these risks can inform investment decisions for new power infrastructure, guide policy-making for water resource management in the energy sector, and support the development of climate change adaptation strategies for existing assets.
*   **Environmental Co-benefits:** Promoting water-efficient energy systems can also reduce the strain on local ecosystems and water resources shared with other users (agriculture, domestic supply).

## Key Research Questions

*   What is the current level of water stress and other water risks faced by different types of power plants globally and in specific regions?
*   How might these water risks change for existing power infrastructure under various future climate and socioeconomic scenarios (e.g., 2030, 2050, 2080)?
*   Are there particular types of power plants or geographical regions that are disproportionately vulnerable to water scarcity?
*   What are the potential implications of these water risks for energy security and sustainable development?

## Datasets Used

1.  **WRI Global Power Plant Database (GPPD):**
    *   **Description:** A comprehensive, open-source database of power plants around the world, geolocated and containing information on capacity, primary fuel type, commissioning year, ownership, and other attributes.
    *   **Source:** Forked from [wri/global-power-plant-database](https://github.com/wri/global-power-plant-database).
    *   **Key Variables:** `gppd_idnr` (unique ID), `name`, `country`, `capacity_mw`, `primary_fuel`, `commissioning_year`, `latitude`, `longitude`, `estimated_generation_gwh_2017`.

2.  **WRI Aqueduct 4.0 Water Risk Atlas:**
    *   **Description:** A suite of tools and datasets from WRI that map and analyze current and future water risks globally, primarily at the river basin level.
    *   **Source:** [WRI Aqueduct Platform](https://www.wri.org/aqueduct).
    *   **Key Datasets & Variables:**
        *   **Baseline Monthly Data:** Provides monthly baseline (1979-2019 trend) indicators for `pfaf_id` basins (HydroBASIN Level 6 catchments). Key indicators include `bws` (Baseline Water Stress), `bwd` (Baseline Water Depletion), `iav` (Interannual Variability), each with raw values, scores, categories, and labels.
        *   **Future Annual Data:** Provides future annual projections for `pfaf_id` basins for time horizons "30" (2015-2045), "50" (2035-2065), "80" (2065-2095) under `bau` (Business as Usual), `opt` (Optimistic), and `pes` (Pessimistic) scenarios. Key indicators include `ba` (Available blue water), `ww` (Gross water demand), `ws` (Water stress), `wd` (Water depletion), `iv` (Interannual variability), `sv` (Seasonal variability).
        *   **Baseline Annual Data:** Contains comprehensive baseline annual indicators, including groundwater table decline, flood risk, drought risk, water quality, regulatory risks, and "Grouped Water Risk" scores for different industries, including "Electric Power" (`elp`).

3.  **Linking Strategy:**
    *   Power plant locations (latitude/longitude from GPPD) are geospatially linked to the relevant water basin identifiers (`pfaf_id` from Aqueduct 4.0) using HydroBASIN Level 6 shapefiles. This assigns water risk indicators to each power plant.

## Methodology & Tools

The project involves the following general steps:

1.  **Data Acquisition and Preprocessing:**
    *   Cleaning and preparing the Global Power Plant Database.
    *   Processing and understanding the Aqueduct 4.0 water risk data (baseline and future projections).
    *   Geospatially linking power plant locations to water basin identifiers.
    *   Merging the power plant data with the water risk indicators.
2.  **Exploratory Data Analysis (EDA):**
    *   Analyzing distributions, identifying patterns, and visualizing relationships between power plant characteristics and water risk indicators.
3.  **Water Risk Assessment:**
    *   Developing metrics to quantify current and future water risk exposure for individual plants and aggregated groups (by fuel type, country, basin).
4.  **Reporting and Visualization:**
    *   Summarizing findings and creating visualizations to communicate the results.

Key tools and libraries used include Python, Pandas, GeoPandas, Jupyter Notebooks, and various data visualization libraries.

## Project Structure

*   `build_databases/`: Contains Python scripts for processing raw data and building the core datasets.
    *   `build_global_power_plant_database.py`: Script to process the WRI GPPD.
    *   `build_aqueduct_database.py`: Script to process and combine Aqueduct 4.0 datasets.
    *   `join_power_plants_to_basins.py`: Script to perform the geospatial join between power plants and water basins.
*   `output_database/`: Contains the final processed datasets.
    *   `global_power_plant_database.csv`: The main enriched power plant database with water risk indicators.
    *   `global_power_plant_database_country_summary.csv`: A summary of water risk exposure aggregated by country.
*   `raw_source_files/`: Stores the original downloaded datasets (e.g., from WRI). (Note: Large data files may be gitignored and require manual download).
*   `Exploratory_Data_Analysis.ipynb`: Jupyter Notebook for initial data exploration, cleaning, and visualization.
*   `Data_Curation_Deliverable.ipynb`: Jupyter Notebook detailing the data sourcing, cleaning, and linking processes.

## Getting Started / Setup

### Prerequisites

*   Python (version 3.8 or higher recommended)
*   Access to a terminal or command prompt.
*   Git for cloning the repository.

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```
2.  **Create and activate a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
3.  **Install dependencies from `requirements.txt`:**
    ```bash
    pip install -r requirements.txt
    ```
    (Note: Some geospatial libraries might have additional system dependencies. Refer to their respective documentation if issues arise.)

## How to Run

1.  **Download Raw Data:** Ensure the necessary raw data files from WRI (Global Power Plant Database, Aqueduct 4.0 datasets, HydroBASIN shapefiles) are downloaded and placed in the `raw_source_files/` directory as expected by the build scripts. (Refer to scripts in `build_databases/` for specific file names and expected locations).
2.  **Build Databases:** Run the Python scripts in the `build_databases/` directory in sequence:
    ```bash
    python build_databases/build_global_power_plant_database.py
    python build_databases/build_aqueduct_database.py
    python build_databases/join_power_plants_to_basins.py
    # (Order might vary, check script dependencies if any)
    ```
    This will generate the processed files in the `output_database/` directory.
3.  **Explore Data and Analysis:**
    *   Open and run cells in `Exploratory_Data_Analysis.ipynb` to understand the data.
    *   Review `Data_Curation_Deliverable.ipynb` for a detailed account of the data processing steps.

## Project Outputs

*   **`output_database/global_power_plant_database.csv`**: The primary output dataset. This CSV file contains each power plant from the WRI GPPD, augmented with relevant current and future water risk indicators from WRI Aqueduct 4.0, based on its location.
*   **`output_database/global_power_plant_database_country_summary.csv`**: A CSV file providing an aggregated summary of power plant capacities exposed to different levels of water risk, grouped by country and primary fuel type.

## Contributing

Contributions to this project are welcome. Please refer to the [CONTRIBUTING.md](.github/CONTRIBUTING.md) file for guidelines on how to contribute. (Note: As this is a capstone project fork, contributions may be context-specific).

## License

This project is a fork of the WRI Global Power Plant Database, which is licensed under the Creative Commons Attribution 4.0 International License (CC BY 4.0). The original data from WRI Aqueduct 4.0 is also publicly available under its own terms (typically open access, refer to WRI for specific details).

The work done in this fork (scripts, analysis, documentation) is made available under the [MIT License](./LICENSE), unless otherwise stated. Please ensure compliance with the original data source licenses when using or distributing any outputs.
