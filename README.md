# IMMERSE Data Release builder

Pipeline to extract, filter and pseudonymise research datasets.

## Pre-requiremements: 
Data definition according to reseach proposal including intested files (phase II) from each IMMERSE data source, interested variables, and assessment window (Baseline, T1, T2, T3).

## Requirements:
- Connection to current **Research Database**, 
- Participant IDs list to exclude (essential filter)

## Features
- This script exports in csv format,  a requested set of questionnaires in a tabular form from a requested data entrance. 
- Filters participant IDs which dropped out at Baseline. 
- Exports a summary of unique participant identifiers along with its unit, condition, randomise value as an updated version from existing REDCap list as CSV file.
- Creates a copy of exported CSV file without headers. 

Use template `example_request_xx.yaml`,  and From `main.py`, enable from STEP2 to STEP7:
```
# Step 2: Reads requirements from YAML.
requirements_dict, assessment_windows = read_yaml_file(filepath_requirements_id_xx)
```

**Optional step:**
Define variables in a info.txt according template and from  `main.py`, enable:
```
# Step 1: Generates YAML file from Info.txt.
info_to_yaml(filepath_requirements_id_30)
```

## Contact
Nayeli A. Silva (DIZ Erlangen)
