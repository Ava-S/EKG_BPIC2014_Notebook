# OCED Schema for generic analysis

This repository contains 3 analysis notebooks for 2 datasets.
- `process_discovery.ipynb` to discover a process
- `task_analysis.ipynb` to perform task analysis
- `performance_analysis.ipynb` to perform performance analysis

The user can decide to either do the analysis on BPIC14 or BPIC17.

## Data
We provide data and scripts for BPI Challenge 2014 and BPI Challenge 2017; store the original data in CSV format in the directory `bpic<xx>/data`.
The datasets are available from:

            Esser, Stefan, & Fahland, Dirk. (2020). Event Data and Queries
            for Multi-Dimensional Event Data in the Neo4j Graph Database
            (Version 1.0) [Data set]. Zenodo. 
            http://doi.org/10.5281/zenodo.3865222

In case you want to work with BPIC14, you first have to run `bpic14_prepare.py` once after you have stored the data.

---------------------
## Installation

The notebooks will execute queries against a database, for this you have to ensure you have a running database.
Please follow the steps below.

### Neo4j
The code assumes that Neo4j is installed.

Install [Neo4j](https://neo4j.com/download/):

- Use the [Neo4j Desktop](https://neo4j.com/download-center/#desktop)  (recommended), or
- [Neo4j Community Server](https://neo4j.com/download-center/#community)

### PromG
PromG should be installed as a Python package using pip
`pip install promg==2.4.6`.

The source code for PromG can be found [PromG Core Github repository](https://github.com/PromG-dev/promg-core).

---------------------
## Get started

### <a name="create_db"></a> Create a new graph database

- Create a New Graph Data In Neo4j Desktop
   1. Select `+Add` (Top right corner)
   2. Choose Local DBMS or Remote Connection
   3. Follow the prompted steps 
  - the default password we assume is bpic2014 for BPIC14 and bpic2017 for BPIC17
  - version should be v5.24.0

> [!IMPORTANT]  
> The code only works Neo4j databases from v5.24.0 (tested with v5.26.0)

- Install APOC (see https://neo4j.com/labs/apoc/)
  - Install `Neo4j APOC Core library`: 
    1. Select the database in Neo4j desktop 
    2. On the right, click on the `plugins` tab > Open the `APOC` section > Click the `install` button
    3. Wait until a green check mark shows up next to `APOC` - that means it's good to go!
  - Install `Neo4j APOC Extended library`
    1. Download the [appropriate release](https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases) (same version numbers as your Neo4j version)
       1. Look for the release that matches the version number of your Neo4j Database.
       2. Download the file `apoc-[your neo4j version]-extended.jar`
    2. Locate the `plugins` folder of your database:  
       Select the Neo4j Server in Neo4j Desktop > Click the three dots > Select `Open Folder` > Select `Plugins`
    4. Put `apoc-[your neo4j version]-extended.jar` into the `plugins` folder of your database
    5. Restart the server (database)
  - Configure extra settings using the configuration file `$NEO4J_HOME/conf/apoc.conf`
    1. Locate the `conf` folder of your database  
       Select the Neo4j Server in Neo4j Desktop > Click the three dots > Select `Open Folder` > Select `Conf`
    2. Create the file `apoc.conf`
    3. Add the following line to `apoc.conf`: `apoc.import.file.enabled=true`.
- Ensure to allocate enough memory to your database, advised: `dbms.memory.heap.max_size=10G`
  1. Select the Neo4j Server in Neo4j Desktop > Click the three dots > Select `Settings`
  2. Locate `dbms.memory.heap.max_size=512m`
  3. Change `512m` to `10G` or `20G`
 
### Set configuration file
- Configuration; `bpic<xx>/config.yaml`
  - Set the URI in `config.yaml` to the URI of your server. Default value is `bolt://localhost:7687`.
  - Set the password in `config.yaml` to the password of your server. 
    - Default value for BPIC14 is `bpic2014`.
    - Default value for BPIC17 is `bpic2017`.

### Install PromG Library for Script
The library can be installed in Python using pip: `pip install promg==2.3.1`.
The source code for PromG can be found [PromG Core Github repository](https://github.com/PromG-dev/promg-core).

---------------------

## How to use
1. Set the configuration in `config.yaml`. 
   - For database settings, see [Create a new graph database](#create_db).
   - Set `use_sample` to True/False
2. start the Neo4j server
3. run `import_data.ipynb` first to import the data (this takes 5 to 20 minutes)
4. We recommend to first discover the process using `process_discovery.ipynb`
5. Then you can choose to either to a task analysis using `task_analysis.ipynb` or performance analysis using `performance_analysis.ipynb`

------------------------

## Provided Scripts
### Notebooks
- `import_data.ipynb` to import the data
- `process_discovery.ipynb` to discover a process
- `task_analysis.ipynb` to perform task analysis
- `performance_analysis.ipynb` to perform performance analysis
- `bpic14/bpic14_prepare.py` to prepare the BPIC14 datasets before import

### Semantic header and dataset description in JSON files 
- **bpic<xx>/json_files/BPIC<XX>.json** - json file that contains the semantic header for BPIC<XX>
- **bpic<xx>/json_files/BPIC<XX>_DS.json** - json file that contains a description for the different datasets of BPIC<XX>



