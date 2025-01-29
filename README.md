# Accessibility Analysis with PostgreSQL/PostGIS

## Overview
This project was developed during my internship at Region Dalarna as part of my Higher Vocational Education (YH) program. It performs an accessibility analysis to determine the shortest distance from any given set of points to another set of points using a road network. The analysis is flexible and can be applied to different datasets by specifying the relevant tables.

The project includes:
- Importing and processing geospatial data
- Creating a road network with additional nodes for accurate routing
- Performing shortest-path calculations using pgRouting
- Generating results for accessibility analysis

## Requirements
### Software:
- R (>= 4.0.0)
- PostgreSQL (>= 12)
- PostGIS extension
- pgRouting extension

### R Packages:
Ensure the following R packages are installed:
```r
if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse, sf, httr, keyring, RPostgres, glue, dplyr)
```

## Installation & Setup
1. Clone this repository:
   ```bash
   git clone https://github.com/mikael-leonidsson/accessibility_analysis.git
   cd accessibility_analysis
   ```

2. Set up PostgreSQL database:
   ```sql
   CREATE DATABASE accessibility_data;
   CREATE EXTENSION postgis;
   CREATE EXTENSION pgrouting;
   ```

3. Update database connection settings in `avstandsanalys.R`:
   ```r
   db_host = "your_database_host"
   db_port = 5432
   db_name = "accessibility_data"
   keyring_service_name = "your_keyring_service"
   ```
   Ensure credentials are stored securely using the `keyring` package.

## Usage
This project provides a wrapper function that allows users to run the accessibility analysis on any set of point and network data. The function `avstandsanalys_tva_punkttabeller_ett_natverk()` takes the following parameters:

```r
avstandsanalys_tva_punkttabeller_ett_natverk(
    w_con = "default",     # Database connection (default or user-defined)
    schema_fran,           # Schema of the source table
    tabell_fran,           # Source table (starting points)
    id_fran,               # ID column in the source table
    geometri_fran,         # Geometry column in the source table
    schema_till,           # Schema of the destination table
    tabell_till,           # Destination table (end points)
    id_till,               # ID column in the destination table
    geometri_till,         # Geometry column in the destination table
    schema_graf,           # Schema of the network table
    tabell_graf,           # Network table
    id_graf,               # ID column in the network table
    geometri_graf,         # Geometry column in the network table
    tolerans_avstand = 3   # Distance tolerance for node creation
)
```

### Example Usage:
To calculate the shortest path between addresses and bus stops using a specific road network, run:
```r
avstandsanalys_tva_punkttabeller_ett_natverk(
    schema_fran = "public",
    tabell_fran = "addresses",
    id_fran = "address_id",
    geometri_fran = "geom",
    schema_till = "public",
    tabell_till = "bus_stops",
    id_till = "stop_id",
    geometri_till = "geom",
    schema_graf = "network",
    tabell_graf = "road_segments",
    id_graf = "segment_id",
    geometri_graf = "geom"
)
```

## Database Structure
### Schemas:
- **network**: Contains the processed road network
- **accessibility**: Stores computed accessibility results

### Key Tables:
- `network.road_segments` - Stores road network segments
- `network.address_nodes` - Stores address nodes with connections to the network
- `network.stop_nodes` - Stores bus stop nodes with connections to the network
- `accessibility.results` - Stores computed shortest-path distances

## Error Handling
The script uses `tryCatch` for robust error handling:
- Errors trigger database rollback to prevent corruption.
- Issues are logged for debugging.
- Logs can be stored in a dedicated file for monitoring.

## Future Improvements
- Automate API data fetching for transit information.
- Implement a configuration file for database and API settings.
- Optimize routing algorithms for improved performance.
- Translate all code and comments to English for better accessibility.

For questions or contributions, feel free to open an issue or submit a pull request!

---
Developed by Mikael Leonidsson
Avesta, April 2024

