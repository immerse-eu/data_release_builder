import os
import yaml
import sqlite3
import pandas as pd

#TODO: Check why it only produces files with Variables. Fix bug.

def load_config_file(directory, file):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
        return config[directory][file]

db_filepath = load_config_file('DB', 'current_db')

def connect_db():
    return sqlite3.connect(db_filepath)

def read_file(filename):
    item_map = {}
    with open(filename, 'r') as f:
        config = yaml.safe_load(f)
    for entry in config.get('files', []):
        item = entry.get('item')
        names = entry.get('name', [])
        added_vars = entry.get('variables', [])
        item_map[item] = {
            'table_name': names,
            'variables': added_vars
        }
    return item_map

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

    for item_data in file_map.values():
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
                tables_to_export.append({'table': table, 'columns': None})
            else:
                cols = get_columns_from_table(table)
                base_cols = cols[:10]
                filter_cols = [c for c in variables_to_add if c in cols and c not in base_cols]
                selected_cols = base_cols + filter_cols
                tables_to_export.append({'table': table, 'columns': selected_cols})

        return tables_to_export


def export_sqlite_tables_to_csv(db_path, file_map, output_dir):

    conn = connect_db()
    tables_to_export = prepare_tables_to_export(file_map)
    print("prepared_tables:")

    os.makedirs(output_dir, exist_ok=True)

    for entry in tables_to_export:

        table = entry['table']
        columns = entry['columns']

        if columns is None:
            query = f'SELECT * FROM "{table}"'
        else:
            cols_sql = ', '.join([f'"{col}"' for col in columns])
            query = f'SELECT {cols_sql} FROM "{table}"'

        df = pd.read_sql_query(query, conn)
        out_path_headers = os.path.join(output_dir, f"ITEM_1_{table}.csv")
        out_path_without_headers = os.path.join(output_dir, f"ITEM_1_{table}_no_headers.csv")

        df.to_csv(out_path_headers, index=False)
        df.to_csv(out_path_without_headers, index=False, header=False)

        print(f"Exported {table} ({'all columns' if columns is None else f'{len(columns)} columns'})")

    conn.close()


def main():
    filepath_requirements_id_10 = load_config_file('data_requirements', 'id_10')
    filepath_release_id_10 = load_config_file('data_release', 'id_10')

    table_dict = read_file(filepath_requirements_id_10)
    export_sqlite_tables_to_csv(
        db_path = db_filepath,
        file_map = table_dict,
        output_dir = filepath_release_id_10
    )

if __name__ == '__main__':
    main()

