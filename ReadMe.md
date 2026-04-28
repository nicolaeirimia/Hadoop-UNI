# Yelp Data Analysis with Hadoop & Spark

This project processes, analyzes, and models the Yelp Open Dataset using Apache Spark and Hadoop (HDFS). It demonstrates end-to-end big data processing, from raw JSON ingestion to machine learning model evaluation.

## 📂 Project Structure

To maintain a clean and modular codebase, the project is split into multiple files:

yelp_spark_project/
│
├── README.md                   # Project documentation and 
├── data/
│   ├── raw/                    # Place raw Yelp JSON files here
│   └── processed/              # HDFS/Local output for Parquet, ORC files
│
└── src/
    ├── 01_data_ingestion.py    # PySpark script: Loads JSON, saves to HDFS in multiple formats
    ├── 02_EDA_and_Queries.ipynb # Jupyter Notebook: Statistics, distributions, and visualizations + 3 spark queries


help for local running:
    in terminal run the following commands
        python -m venv venv
        .\venv\Scripts\activate
        pip install pyspark jupyterlab matplotlib seaborn pandas

        !! if pyspark is not compatible with java check version 3.4.1


raw data files can be downloaded from here: https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset