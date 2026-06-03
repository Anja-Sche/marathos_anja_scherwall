# Marathos - ultra marathon 

## About
Marathos hosts ultra marathons all over the world and they are starting their digital journey.
In Databricks I have built a pipeline using medallion schema, bronze, silver and gold. This is to preserve the data in the different layers for the possibility to go back.

## Project structure
```text
marathos_anja_scherwall
├── dimensional_modeling
├── docs
├── explorations
├── transformations
│ ├── bronze
│ ├── gold
│ └── silver
└── utils
```   


## Bronze layer
The bronze layer contains the raw data. 

An initial EDA (Exploratory Data Analysis) was made to see what the dataset contained.

## Silver layer
The silver layer contains cleaned data. 

Silver EDA has been used to explore the data more and to develop the functions for cleaning the data. 
## Gold layer
The gold layer contains structured dimensional models and marts (tailored dataset)

Gold EDA Was used to test SQL querys (requests) for creating the marts.


## Other information
[Sources to data and validation limits](docs/sources.md)

[Reflections](docs/reflections.md)