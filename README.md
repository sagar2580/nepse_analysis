# Real Time Data Analysis of Nepse
## Overview
### This project implements a complete real time data processing pipeline using kafka and pyspark with following steps:
- Scraper/Producer: Collects raw data from merolagani.com
- Consumer: Processes and validates the raw data
- Transformer: Transforms the data (using a Jupyter notebook)
- Visualization: Generates insights from the transformed data

### Pipeline Stages
1. Scraper/Producer
    - Collects data from specified sources
    - Outputs raw data in JSON format
    
2. Consumer
   - Validates and prepares raw data
   - Handles basic data cleaning
   - Outputs to intermediate storage i.e. postgres database

3. Transformer (Notebook)
   - Performs complex transformations
   - Uses Jupyter notebook for flexible, iterative development
   - Saves processed data to /output/

4. Visualization
   - Generates visualizations
   - Outputs visualizations to /data/outputs/
   - Can optionally send email reports

### Customization
To adapt this pipeline for your specific needs:
    Scraper: Modify the scraper_producer() function in data_pipeline.py
    Transformation: Edit the notebooks/transformation.ipynb file
    Visualization: Update the visualization code 
