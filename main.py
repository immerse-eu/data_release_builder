import os
import re
import yaml
import sqlite3
import pandas as pd
from filtering import assessment_window_filtering, filtering_interesting_ids, filtering_excluded_ids
from utils import load_config_file, write_config_file, detect_separator


db_filepath = load_config_file('DB', 'current_db')
baseline_ids_directory = load_config_file('filters', 'baseline_ids_directory')

# --- Record release: Record_00 (test)
filepath_requirements_id_00 = load_config_file('data_requirements', 'input_release_num_00')
filepath_release_id_00 = load_config_file('data_release', 'output_release_num_00')
additional_ids_filter_directory_id_00 = load_config_file('filters', 'additional_id_filter_num_22')


def connect_db():
    return sqlite3.connect(db_filepath)


def read_yaml_file(filename):
    item_map = {}
    with open(filename, 'r') as f:
        config = yaml.safe_load(f)

    for entry in config.get('files', []):
        item = entry.get('item')
        names = entry.get('table', [])
        added_vars = entry.get('variables', [])
        if item:
            item_map[item] = {
                'table_name': names,
                'variables': added_vars
            }

    assessment_window = config.get('assessment_window', [])
    return item_map, assessment_window


def get_columns_from_table(table_name):
    conn = connect_db()
    query = f"PRAGMA table_info('{table_name}')"
    info = pd.read_sql_query(query, conn)
    return info['name'].tolist()


def prepare_tables_to_export(file_map):
    tables_to_export = []
    variables_to_export = []

    conn = connect_db()
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    all_tables = pd.read_sql_query(query, conn)['name'].tolist()

    for item_number, item_data in file_map.items():

        table_names = item_data.get('table_name', [])
        if isinstance(table_names, str):
            table_names = [table_names]
        elif isinstance(table_names, list):
            table_names = [name for name in table_names if isinstance(name, str)]

        variables_to_add = item_data.get('variables', None)
        if isinstance(variables_to_add, str):
            variables_to_add = [variables_to_add]
        elif isinstance(variables_to_add, list):
            variables_to_add = [v for v in variables_to_add if isinstance(v, str)]

        for table in table_names:
            if table not in all_tables:
                print(f"Warning: Table '{table}' not found in DB. Skipping.")
                continue
            if not variables_to_add:
                tables_to_export.append({
                    'item': item_number,
                    'table': table,
                    'columns': None
                })
            else:
                cols = get_columns_from_table(table)
                base_cols = cols[:10]
                # base_cols = [cols[i] for i in range(7) if i != 1]  # TODO: Use only for DMMH
                filter_cols = [c for c in variables_to_add if c in cols and c not in base_cols]
                selected_cols = base_cols + filter_cols
                tables_to_export.append({
                    'item': item_number,
                    'table': table,
                    'columns': selected_cols
                })

    return tables_to_export


def export_sqlite_tables_to_csv(file_map, output_dir):
    conn = connect_db()
    tables_to_export = prepare_tables_to_export(file_map)

    os.makedirs(output_dir, exist_ok=True)

    for entry in tables_to_export:
        item = entry['item']
        table = entry['table']
        columns = entry['columns']
        print('Exporting..')
        print("table", table)
        print("columns: ", columns)

        if columns is None:
            query = f'SELECT * FROM "{table}"'
        else:
            cols_sql = ', '.join([f'"{col}"' for col in columns])
            query = f'SELECT {cols_sql} FROM "{table}"'

        df = pd.read_sql_query(query, conn)
        out_path_headers = os.path.join(output_dir, f"{item}_{table}.csv")

        df.to_csv(out_path_headers, index=False, sep=";")

        print(f"Exported {table} ({'all columns' if columns is None else f'{len(columns)} columns'})")

    conn.close()


def create_participants_summary_from_df(output_path):
    print("\nPreparing participants summary...")
    unique_values = {}
    value_columns = ["unit", "condition", "randomize"]
    # value_columns = ["participant_number", "VisitCode", "SiteCode"]  # --> only for MovisensESM

    for file in os.listdir(output_path):
        if file.endswith(".csv") and file.startswith("ITEM"):
            filepath = os.path.join(output_path, file)
            separator = detect_separator(filepath)
            current_df = pd.read_csv(filepath, sep=separator, quotechar='"')

            for _, row in current_df.iterrows():
                identifier = row.iloc[0]
                if pd.notna(identifier) and identifier not in unique_values:
                    values = [row.get(col) for col in value_columns]
                    unique_values[identifier] = values

    unique_participants_df = pd.DataFrame.from_dict(unique_values, orient="index", columns=value_columns)
    unique_participants_df.index.name = "participant_identifier"
    unique_participants_df.reset_index(inplace=True)
    output_filename = f'participants_conditions_summary.csv'
    output_file = os.path.join(os.path.dirname(output_path), output_filename)
    unique_participants_df.to_csv(output_file, sep=';', index=False)
    print(f"Exported {len(unique_participants_df)} unique IDs in:\n{output_file}\n")


def remove_header_from_csv(input_csv_path):
    for file in os.listdir(input_csv_path):
        if file.startswith("ITEM") and file.endswith(".csv"):
            filepath = os.path.join(input_csv_path, file)
            print(f"Removing header from {file}")

            df = pd.read_csv(filepath, sep=";")
            new_filepath = os.path.join(input_csv_path, f"{file.replace(".csv", "_")}no_headers.csv")
            df.to_csv(new_filepath, sep=";", index=False, header=False)


def info_to_yaml(info_txt_file_path):
    info_path = os.path.join(info_txt_file_path, "info.txt")
    file_data = None

    try:
        with open(info_path, 'r') as file:
            file_data = file.read()
    except FileNotFoundError:
        print("The info.txt was not found in {}.".format(info_txt_file_path))

    if file_data is None:
        raise FileNotFoundError(f"No data loaded from {info_path}")

    pattern = r"REQUEST: Record ID (\d+)\n\n(.*?)\Z"
    interested_var1 = r"INTERESTED_VARIABLES:\s*\[([^\]]*)\]|"
    interested_var2 = r"^\s*INTERESTED_VARIABLES:\s*\n((?:[ \t]+.+\n?)*)"
    interested_vars = rf"{interested_var1}|{interested_var2}"
    assessment_vars = r"-\s*Data Phase II assessment window:\s*([^\n]+)"

    match = re.search(pattern, file_data, re.DOTALL)
    if match:
        record_id = int(match.group(1))  # Convert to integer
        items_text = match.group(2)

        data_dict = {"files": []}

        # Detect assessment window to filter data
        match_assessment = re.search(assessment_vars, items_text, re.DOTALL)
        if match_assessment:
            assessment_raw_values = match_assessment.group(1)
            assessment_values = [v.strip() for v in assessment_raw_values.split(",") if v.strip()]
            if assessment_values:
                data_dict["assessment_window"] = assessment_values

        item_blocks = [block for block in items_text.split('ITEM ') if block.strip()]

        for item in item_blocks:
            item_number_match = re.search(r"(\d+):", item)
            if item_number_match:
                item_number = int(item_number_match.group(1))
            else:
                continue  # Skip this item if no item number is found

            csv_filenames_match = re.findall(r"([^\s]+\.csv)", item)
            csv_filenames = [filename.replace('.csv', '') for filename in csv_filenames_match]

            interested_variables_matches = re.findall(interested_vars, item, flags=re.MULTILINE)
            interested_variables = []
            for match1, match2 in interested_variables_matches:
                captured = match1 or match2
                if not captured:
                    continue
                if ',' in captured:
                    interested_variables.extend([v.strip() for v in captured.split(',') if v.strip()])
                else:
                    interested_variables.extend([v.strip() for v in captured.splitlines() if v.strip()])

            item_data = {
                "item": item_number,
                "table": csv_filenames or []
            }

            if interested_variables:
                item_data["variables"] = interested_variables

            data_dict["files"].append(item_data)

        # bring into yaml-format & save
        file_path = os.path.join(info_txt_file_path, f'request_id_{record_id}.yaml')
        with open(file_path, 'w') as file:
            yaml.dump(data_dict, file, default_flow_style=False, sort_keys=False)
        saved_path = write_config_file(os.path.dirname(file_path), os.path.basename(file_path))

        if file_path:
            print(f"Info.txt written to request_id_{record_id}.yaml created at: {saved_path}")


def main():

    # Step 1: Generates YAML file from Info.txt
    info_to_yaml(filepath_requirements_id_00)

    # Step 2: Reads requirements from YAML.
    requirements_dict, assessment_windows = read_yaml_file(filepath_requirements_id_00)

    # Step 3: Exports CSV files from Research DB tables.
    export_sqlite_tables_to_csv(file_map=requirements_dict, output_dir=filepath_release_id_00)

    # Step 4: Filtering per assessment window (Screening, Baseline, 2-month, 6-month, and 12-month).
    assessment_window_filtering(assessment_list=assessment_windows, source_path=filepath_release_id_00)
    #
    # Step 5: Excludes participants whose dropped out from Baseline.
    filtering_excluded_ids(baseline_ids_path=baseline_ids_directory, source_path=filepath_release_id_00)

    # Step 6: Additional participant IDs filtering (depends on each data release).
    if os.path.isfile(additional_ids_filter_directory_id_00) and additional_ids_filter_directory_id_00.endswith('.xlsx'):
        filtering_interesting_ids(baseline_ids_path=additional_ids_filter_directory_id_00,
                                  source_path=filepath_release_id_00)

    # Step 7: Creates a summary of participants (n=379).
    create_participants_summary_from_df(filepath_release_id_00)

    # Step 8: Pseudo

    # Step 9: Exports a copy of CSV files without headers.
    remove_header_from_csv(filepath_release_id_00)


if __name__ == '__main__':
    main()
