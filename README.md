# data_release_builder

## Requirements:
- YAML file definition from a data release. 
- Connection to the current Research Database 
- Participant IDs list to exclude 

## Features
- This script exports in csv format,  a requested set of questionnaires from a data release request. 
- Filters participant IDs which dropped out at Baseline. 
- Exports a summary of unique participant identifiers along with its unit, condition, randomise value as an updated version from existing REDCap list as CSV file.
- Creates a copy of exported CSV file without headers. 
